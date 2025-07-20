from typing import Tuple
from typing import Generator, TypeVar, Callable, Any, Dict, Generic
import os
import random
import sys
import tempfile
import time
import subprocess
import re
from dataclasses import dataclass

from pathlib import Path
from uv import find_uv_bin

from concurrent.futures import Future
from threading import Thread, Event

import rpyc
from rpyc import BgServingThread
from rpyc.utils.zerodeploy import DeployedServer

from plumbum import SshMachine
from plumbum.commands import CommandNotFound, ProcessExecutionError
from plumbum.commands.base import BoundCommand
from plumbum.machines.paramiko_machine import ParamikoMachine

old_print = print

##################################################################

SERVER_SCRIPT = r"""\
import logging

# logging.basicConfig(
#     level=logging.INFO,  
#     filename="app.log",
#     filemode="a"
# )
# logger = logging.getLogger(__name__)
    
if __name__ == "__main__":
    import sys
    import os
    import atexit
    import shutil
    import time
    
    here = os.path.dirname(__file__)
    os.chdir(here)

    def rmdir():
        shutil.rmtree(here, ignore_errors = True)
    atexit.register(rmdir)

    try:
        for dirpath, _, filenames in os.walk(here):
            for fn in filenames:
                if fn == "__pycache__" or (fn.endswith(".pyc") and os.path.exists(fn[:-1])):
                    os.remove(os.path.join(dirpath, fn))
    except Exception:
        pass

    sys.path.insert(0, here)
    from $SERVER_MODULE$ import $SERVER_CLASS$ as ServerCls
    from $SERVICE_MODULE$ import $SERVICE_CLASS$ as ServiceCls

    logger = None

    $EXTRA_SETUP$
    
    t = ServerCls(ServiceCls, hostname = "localhost", port = 0, reuse_addr = True, logger = logger)
    # ServiceCls.server = t
    thd = t._start_in_thread()

    sys.stdout.write(f"{t.port}\n")
    sys.stdout.flush()
    try:
        sys.stdin.read()
    finally:
        thd.join(2)
"""


@dataclass
class tupleprocess:
    number: int


@dataclass
class localprocess(tupleprocess):
    number: int


@dataclass
class networkprocess(tupleprocess):
    number: int
    connection: SshMachine | ParamikoMachine | tuple[str, int]


@dataclass
class proxyprocess(tupleprocess):
    number: int
    host: str | None
    port: int | None
    client: list["localprocess | networkprocess | proxyprocess"]
    ssh_login: Tuple
    ssh_remote: Dict[str, Tuple]


################################################################

T = TypeVar("T")
U = TypeVar("U")


class Recursive_RPC:
    """
    This class is a scheduler and load balancer
    support local process and ssh
    """

    def __init__(
        self,
        client: list[localprocess | networkprocess | proxyprocess],
        conn: Dict[str, Tuple],
    ):
        self.client: list[localprocess | networkprocess | proxyprocess] = client
        self.connection: list[None | Runner] = [None for i in range(len(client))]
        self.weight: list[int] = [0 for i in range(len(self.connection))]
        for index, val in enumerate(client):
            match val:
                case localprocess(i):
                    runner = ProcessRunner(i)
                case networkprocess(i, j):
                    runner = NetworkRunner(i, j)
                case proxyprocess(i, j, k, l, m, n):
                    runner = ProxyRunner(i, j, k, l, m, n)  # type: ignore
                case _:
                    raise TypeError
            self.weight[index] = runner.process_num
            self.connection[index] = runner
        if not any(self.connection):
            raise RuntimeError("No runner")
        # print(len(self.connection))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for i, c in enumerate(self.connection):
            if c:
                c.close()
        self.connection.clear()

    def schedule(self) -> "Runner":
        # start_time = time.time()
        # memo = {}
        # def get_status(i):
        #     if memo.get(i):
        #         return memo.get(i)
        #     is_active, p_num, n_done_count, latency = i.status()
        #     memo[i] = p_num - n_done_count, latency
        #     return p_num - n_done_count, latency

        # print(f"->: {time.time() - start_time:f} seconds")

        # return CustomList(self.connection).filter(
        #             lambda x: x is not None
        #         ).sort(
        #             key=lambda x: get_status(x)[0], reverse=True
        #         ).take(1)[0]
        # rand = random.randint(0, len(self.connection) - 1)
        # return self.connection[rand]
        rand = random.choices(self.connection, weights=self.weight, k=1)[0]
        if not rand:
            raise RuntimeError("No runner")
        return rand
        # return self.connection[0]
        # return self.connection[1]

    def apply(self, func: Callable[[Any], T], /, *args, **kwargs) -> T:
        # Apply then wait
        runner: Runner = self.schedule()
        if not runner:
            raise RuntimeError("No runner")
        result: RPC_Future = runner.run(func, *args, **kwargs)
        return result.get()

    def apply_async(self, func, /, *args, **kwargs) -> "RPC_Future":
        # Apply then give future object
        runner: Runner = self.schedule()
        if not runner:
            raise RuntimeError("No runner")
        return runner.run(func, *args, **kwargs)

    def map_ordered(self, iters, func, /, *args, **kwargs):
        future_list: list["RPC_Future"] = []
        for i in iters:
            runner: Runner = self.schedule()
            if not runner:
                raise RuntimeError("No runner")
            r = runner.run(func, *[i, *args], **kwargs)
            future_list.append(r)
        return [f.get() for f in future_list]

    def map_ordered_async(self, iters, func, /, *args, **kwargs) -> list["RPC_Future"]:
        return self.map_async(iters, func, *args, **kwargs)

    def map(self, iters, func, /, *args, **kwargs):
        future_list: list["RPC_Future"] = []
        for i in iters:
            runner: Runner = self.schedule()
            if not runner:
                raise RuntimeError("No runner")
            r = runner.run(func, *[i, *args], **kwargs)
            future_list.append(r)
        return [f for f in RPC_Future.as_completed(future_list)]

    def map_async(self, iters, func, /, *args, **kwargs) -> list["RPC_Future"]:
        future_list: list["RPC_Future"] = []
        for i in iters:
            runner: Runner = self.schedule()
            if not runner:
                raise RuntimeError("No runner")
            r = runner.run(func, *[i, *args], **kwargs)
            future_list.append(r)
        return future_list

    @staticmethod
    def as_completed(cls_act: list["RPC_Future"]) -> Generator[Any, None, None]:
        cls = cls_act.copy()
        while any(cls):
            for i, v in enumerate(cls):
                if v and v.status():
                    yield v.get()
                    cls.remove(v)


class RPC_Future(Generic[T]):
    """
    This class is a placeholder of future value
    """

    def __init__(self, result: Future, scheduler: "Runner"):
        self.result = result
        self.scheduler = scheduler

    def check_scheduler(self):
        return self.scheduler.status()

    def get(self):
        while not self.result.done():
            time.sleep(0.1)
        return self.result.result()

    def status(self):
        # self.scheduler.conn.ping()
        return self.result.done()

    @staticmethod
    def as_completed(cls_act: list["RPC_Future[T]"]) -> Generator[T, None, None]:
        cls = cls_act.copy()
        while any(cls):
            for i, v in enumerate(cls):
                if v and v.status():
                    yield v.get()
                    cls.remove(v)


################################################################


class Runner:
    """
    This class is concrete runner
    """

    def __init__(self):
        pass

    def run(self, func, /, *args, **kwargs): 
        raise NotImplementedError

    def close(self): ...

    def status(self):
        raise NotImplementedError


class ProcessRunner(Runner):
    def __init__(self, num: int, printer=print):
        print("ProcessRunner", num)
        printer("ProcessRunner", num)

        self.pool: "Pool" = Pool(num)
        self.process_num = self.pool.max_workers
        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        result = self.pool.apply_async(func, args, kwargs)
        self.process_handle.append(RPC_Future(result, self))
        return self.process_handle[-1]

    def close(self):
        self.pool.close()

    def status(self) -> tuple[bool, int, int, int]:
        for i, v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)

        is_pool_active: bool = not not self.pool
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)


class NetworkRunner(Runner):
    def __init__(self, num: int, con: SshMachine | ParamikoMachine | tuple[str, int]):
        self.process_num = num

        self.server = None
        match con:
            case SshMachine() | ParamikoMachine():
                self.machine = con

                # Get uv binary path
                self.uv_bin = os.fsdecode(find_uv_bin())
                print(self.uv_bin)
                if not self._check_uv_available():
                    raise RuntimeError("uv not installed")

                # Generate requirements and script header
                header = "#!/usr/bin/env uv run --script\n"
                curdir = os.getcwd()

                print(curdir)
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)
                    # Generate requirements.txt using uv
                    os.chdir(temp_dir)
                    self._generate_requirements()

                    # Create util.py and add dependencies
                    Path(temp_dir / "util.py").touch()
                    self._add_script_dependencies()
                    os.chdir(curdir)

                    # Read and process the generated script
                    with open(temp_dir / "util.py", "r") as f:
                        lines = f.read()
                        # Fix recursiverpc path to use current environment
                        # TODO: this should be replaced
                        newlines = self._fix_recursiverpc_path(lines)
                        header += newlines
                        # header += lines

                server_script_user = header + "\n" + SERVER_SCRIPT[1:]
                extra_setup = ""
                print(server_script_user)

                major = sys.version_info[0]
                minor = sys.version_info[1]

                # Use uv run with script mode
                self.uv_bin = os.path.relpath(os.fsdecode(find_uv_bin()), os.getcwd())
                # TODO: fix this, shell name is leaking
                executable = [
                    "/usr/bin/env",
                    "uv",
                    "run",
                    "-q",
                    "--python",
                    f"{major}.{minor}",
                    "--script",
                ]
                # executable = ["uv.exe", "run", "-q", "--python", f"{major}.{minor}", "--script"]
                self.server = DeployedCrossPlatformServer(
                    self.machine,
                    server_script=server_script_user,
                    extra_setup=extra_setup,
                    server_class="rpyc.utils.server.OneShotServer",
                    python_executable=executable,
                )

                self.conn = self.server.classic_connect()
            case (hostname, port):
                self.conn = rpyc.classic.connect(hostname, port=port)
            case _:
                raise RuntimeError("Invalid connection")

        self.bg_event_loop = BgServingThread(self.conn)
        Pool = self.conn.modules.recursiverpc.Pool
        self.conn.modules.sys.stdout = sys.stdout
        self.conn.modules.sys.stderr = sys.stderr
        self.conn.namespace["pool"] = Pool(self.process_num)
        self.pool = self.conn.namespace["pool"]
        # self.conn.root.close()
        # self.conn.root.close()

        self.process_handle: list[RPC_Future] = []

    def _check_uv_available(self) -> bool:
        result = subprocess.run(
            [self.uv_bin, "--version"], capture_output=True, text=True
        )
        return result.returncode == 0

    def _generate_requirements(self):
        result = subprocess.run(
            [self.uv_bin, "pip", "freeze"], capture_output=True, text=True, check=True
        )
        with open("requirements.txt", "w") as f:
            f.write(result.stdout)

    def _add_script_dependencies(self):
        """Add dependencies to script using uv"""
        print(
            subprocess.run(
                [
                    self.uv_bin,
                    "add",
                    "-q",
                    "-r",
                    "requirements.txt",
                    "--script",
                    "util.py",
                ],
                # check=True, capture_output=True
                capture_output=True,
            )
        )

    def _fix_recursiverpc_path(self, content: str) -> str:
        """Fix recursiverpc path to use current environment's installation"""
        recursiverpc_path = self._get_recursiverpc_path()
        if not recursiverpc_path:
            return content

        pattern = r"recursiverpc = \{[^}]*\}"
        escaped_path = recursiverpc_path.replace(
            "\\", "/"
        )  # TODO: this is windows specific line
        replacement = f'recursiverpc = {{ path = "{escaped_path}", editable = true }}'

        # Replace the pattern
        return re.sub(pattern, replacement, content)

    def _get_recursiverpc_path(self) -> str:
        """Get the path to recursiverpc project root using __file__"""
        try:
            import recursiverpc

            # Get the module file path
            module_file = Path(recursiverpc.__file__)
            current = module_file.parent  # src/recursiverpc

            # Go up until we find pyproject.toml
            while current.parent != current:
                if (current / "pyproject.toml").exists():
                    return str(current)
                current = current.parent

            # If no pyproject.toml found, assume it's one level up from src
            if module_file.parent.parent.name == "src":
                project_root = module_file.parent.parent.parent
                if (project_root / "pyproject.toml").exists():
                    return str(project_root)

            # Fallback: return the parent of the module directory
            return str(module_file.parent.parent)
        except ImportError:
            print("Warning: Could not import recursiverpc to determine path")
            return ""

    def _get_uv_executable(self, major: int, minor: int):
        """Get uv executable command for remote execution"""
        return [self.uv_bin, "run", "-q", "--python", f"{major}.{minor}", "--script"]

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        if self.conn is None:
            raise RuntimeError("No connection")

        # self.conn.ping()
        self.conn.teleport(func)
        self.conn.namespace["args"] = args
        self.conn.namespace["kwargs"] = kwargs
        self.conn.namespace["result"] = self.pool.apply_async(
            func, self.conn.namespace["args"], self.conn.namespace["kwargs"]
        )
        result = self.conn.namespace["result"]
        self.process_handle.append(RPC_Future(result, self))
        # print(result.done())
        return self.process_handle[-1]

    def close(self):
        print("cleaning")
        try:
            if self.conn is not None:
                self.pool.close()
        except Exception as e:
            print("abc", e)
        try:
            if self.conn is not None:
                self.bg_event_loop.stop()
                # print("-1 Cleaned up")
                # self.conn.root.close()
                # print(self.conn.root.server)
                # self.conn.root.server.close()
                # print("0 Cleaned up")
                # self.bg_event_loop.stop()
                self.conn.close()
                print("1 Cleaned up")
            self.conn = None
        except Exception as e:
            print("asdf", e)
        try:
            # time.sleep(10)
            if self.server is not None:
                self.server.close()
            self.server = None
        except Exception as e:
            print("b", e)
        try:
            if self.machine is not None:
                self.machine.close()
            self.machine = None
        except Exception as e:
            print("c", e)
        print("allesclar")

    def __del__(self):
        self.close()

    def status(self) -> tuple[bool, int, int, int]:
        for i, v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)

        is_pool_active: bool = not not self.conn
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)


class ProxyRunner(Runner):
    def __init__(
        self,
        num: int,
        host: str,
        port: int,
        clientlist: list[localprocess | networkprocess | proxyprocess],
        ssh_login: Tuple | None = None,
        ssh_remote: Dict[str, Tuple] = {},
    ):
        self.host = host
        self.port = port
        self.num = num
        self.ssh_login = ssh_login

        self.stop = None
        if host is not None:
            user, password = (
                self.ssh_login if self.ssh_login is not None else ("root", "")
            )
            self.stop = activate_ssh(self.host, user, self.port, password, num)

            self.conn = rpyc.classic.connect(host, port=num)
            self.conn.execute("import recursive_rpc as rp")
            self.conn.execute("import dill")
            self.conn.execute("dill.settings['recurse'] = True")
            self.conn.execute("runner_list = []")

        self.process_num: int = 0
        self.weight: list[int] = []
        self.connection: list[Runner] = []
        self.process_handle: list[RPC_Future] = []

        self.drop_list = []

        for index, val in enumerate(clientlist):
            match val:
                case localprocess(i):
                    print("Here3")
                    if host is not None:
                        self.conn.execute(
                            "runner_list.append(rp.ProcessRunner(" + str(i) + "))"
                        )
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        runner = ProcessRunner(i)
                case networkprocess(i, j, k, l):
                    if host is not None:
                        if callable(l):
                            self.drop_list.append(l)
                            func_name = "None"
                            print("Here2")
                        else:
                            # create ssh connection
                            # stop = activate_ssh(HOSTNAME,
                            #     USER,
                            #     PORT,
                            #     PASSWORD,
                            #     remote_port)
                            print("Here")
                            HOSTNAME, USER, PORT, PASSWORD, remote_port = ssh_remote[l]
                            # check if user has double backslashes
                            # if "\\" in USER:
                            #     USER = USER.replace("\\", "\\\\")
                            # print("stop = rp.activate_ssh(\"" + str(HOSTNAME) + \
                            #                   "\", \"" + str(USER) + \
                            #                   "\", " + str(PORT) + \
                            #                   ", \"" + str(PASSWORD) +\
                            #                   "\"," + str(remote_port) + ")")
                            if "\\\\" in USER:
                                USER = USER.replace("\\\\", "\\")
                            self.conn.namespace["HOSTNAME"] = HOSTNAME
                            self.conn.namespace["USER"] = USER
                            self.conn.namespace["PORT"] = PORT
                            self.conn.namespace["PASSWORD"] = PASSWORD
                            self.conn.namespace["remote_port"] = remote_port
                            print(
                                f"HOSTNAME: {HOSTNAME}, USER: {USER}, PORT: {PORT}, PASSWORD: {PASSWORD}, remote_port: {remote_port}"
                            )
                            print(self.conn.namespace["HOSTNAME"])
                            print(self.conn.namespace["USER"])
                            print(self.conn.namespace["PORT"])
                            print(self.conn.namespace["PASSWORD"])
                            print(self.conn.namespace["remote_port"])
                            print("Here2")
                            # self.conn.execute("stop = rp.activate_ssh(HOSTNAME, USER, PORT, PASSWORD, remote_port)")
                            # with rpyc.classic.redirected_stdio(self.conn):
                            #     from textwrap import dedent
                            #     command = dedent("""
                            #                     print("hello")
                            #                     try:
                            #                         stop = rp.activate_ssh(HOSTNAME, USER, PORT, PASSWORD, remote_port)
                            #                         print(stop)
                            #                     except Exception as e:
                            #                         print(e)
                            #                     """)
                            #     self.conn.execute(command)
                            self.conn.execute(
                                "stop = rp.activate_ssh(HOSTNAME, USER, PORT, PASSWORD, remote_port)"
                            )
                            # self.conn.execute("stop = rp.activate_ssh(\"" + str(HOSTNAME) + \
                            #                   "\", \"" + str(USER) + \
                            #                   "\", " + str(PORT) + \
                            #                   ", \"" + str(PASSWORD) +\
                            #                   "\", " + str(remote_port) + ")")
                            print("Here 7")
                            func_name = "stop"
                        self.conn.execute(
                            "runner_list.append(rp.NetworkRunner("
                            + str(i)
                            + ", "
                            + '"'
                            + str(j)
                            + '"'
                            + ", "
                            + str(k)
                            + ", "
                            + func_name
                            + "))"
                        )
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        if callable(l):
                            runner = NetworkRunner(i, j, k, l)
                        else:
                            runner = NetworkRunner(i, j, k, None)
                case proxyprocess(i, j, k, l, m, n):
                    if host is not None:
                        self.conn.execute(
                            "runner_list.append(rp.ProxyRunner("
                            + str(i)
                            + ", "
                            + str(j)
                            + ", "
                            + str(k)
                            + ", "
                            + str(l)
                            + ", "
                            + str(m)
                            + ", "
                            + str(n)
                            + "))"
                        )
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        raise NotImplementedError("This causes an infinite loop")
                case _:
                    raise TypeError
            self.process_num += runner.process_num
            self.weight.append(runner.process_num)
            self.connection.append(runner)
        print("done init")

    def schedule(self):
        rand = random.choices(self.connection, weights=self.weight, k=1)[0]
        return rand

    def run(self, func, /, *args, **kwargs):
        runner: Runner = self.schedule()
        if not runner:
            raise RuntimeError("No runner")

        # teleport function
        if self.host is not None:
            self.conn.teleport(func)
            self.conn.namespace["args"] = args
            self.conn.namespace["kwargs"] = kwargs

            # self.conn.namespace["printdata"] = print
            # print("=======================================")
            # import sys
            # self.conn.modules.sys.stdout = sys.stdout
            # self.conn.execute("printdata(args)")
            # self.conn.execute("print(kwargs)")
            # num = [5]
            # def addnum():
            #     num[0] += 2
            # print(num)
            # self.conn.namespace["addnum"] = addnum
            # self.conn.execute("addnum()")
            # print(num)
            # print("=======================================")

            run_lst: list = self.conn.namespace["runner_list"]
            runner_index = run_lst.index(runner)
            self.conn.execute(
                f"result = runner_list[{runner_index}].run(worker_func, *args, **kwargs)"
            )

            result = self.conn.namespace["result"]
            # print(self.conn.eval("type(result)"))
            # print(self.conn.eval("dir(result)"))
            # self.process_handle.append(RPC_Future(result, self))
            self.process_handle.append(result)
            future_instance = self.process_handle[-1]
        else:
            future_instance = runner.run(func, *args, **kwargs)

        return future_instance

    def close(self):
        for i in self.connection:
            i.close()
        self.connection.clear()
        for i in self.drop_list:
            i()
        if self.stop:
            self.stop()

    def status(self) -> tuple[bool, int, int, int]:
        for i, v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)

        is_pool_active: bool = not not self.connection
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)


#################################################################


class Pool:
    def __init__(self, max_workers: int, offset: int = 0):
        import multiprocessing

        self.max_workers = (
            max_workers if max_workers > 0 else multiprocessing.cpu_count()
        )
        self.scheduler = []

        # print("Connecting")
        for _ in range(max_workers):
            conn = rpyc.classic.connect_multiprocess()
            conn.modules.sys.stdout = sys.stdout
            conn.modules.sys.stderr = sys.stderr
            self.scheduler.append(conn)

        self.scheduler_index = 0
        self.scheduler_status: list[bool] = [False] * self.max_workers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _schedule(self):
        index = self.scheduler_index
        index_found = False
        for i in range(len(self.scheduler_status)):
            if not self.scheduler_status[(index + i) % len(self.scheduler_status)]:
                index = (index + i) % len(self.scheduler_status)
                self.scheduler_status[index] = True
                index_found = True
                break
            else:
                index = (index + i) % len(self.scheduler_status)
        if not index_found:
            index = (index + 1) % len(self.scheduler_status)
            print("ALL BUSY")
        self.scheduler_index = index
        return self.scheduler[index]

    def apply_async(self, func, args, kwargs):
        runner = self._schedule()
        function = runner.teleport(func)

        result_future = Future()

        def function_async(*args, **kwargs):
            nonlocal result_future
            result = function(*args, **kwargs)
            result_future.set_running_or_notify_cancel()
            result_future.set_result(result)
            self.scheduler_status[self.scheduler_index] = False

        thread = Thread(target=function_async, args=args, kwargs=kwargs)
        thread.start()

        return result_future

    def close(self):
        for i in self.scheduler:
            i.close()

    def __del__(self):
        self.close()


#################################################################


class DeployedCrossPlatformServer(DeployedServer):
    def __init__(
        self,
        remote_machine: SshMachine | ParamikoMachine,
        server_class="rpyc.utils.server.ThreadedServer",
        service_class="rpyc.core.service.SlaveService",
        server_script=SERVER_SCRIPT,
        extra_setup="",
        python_executable=None,
    ) -> None:
        self.proc = None
        self.tun = None
        self.remote_machine = remote_machine
        self._tmpdir_ctx = None

        # print(python_executable)
        if isinstance(python_executable, (list, tuple)):
            cmd = remote_machine[python_executable[0]]
            for part in python_executable[1:]:
                cmd = cmd[part]
        elif isinstance(python_executable, BoundCommand):
            cmd = python_executable
        elif python_executable:
            cmd = remote_machine[python_executable]
        else:
            cmd = self._find_python_command(remote_machine)

        self._tmpdir_ctx = CrossPlatformTempDir(remote_machine)
        tmp = self._tmpdir_ctx.__enter__()

        server_modname, server_clsname = server_class.rsplit(".", 1)
        service_modname, service_clsname = service_class.rsplit(".", 1)

        for source, target in (
            ("$SERVER_MODULE$", server_modname),
            ("$SERVER_CLASS$", server_clsname),
            ("$SERVICE_MODULE$", service_modname),
            ("$SERVICE_CLASS$", service_clsname),
            ("$EXTRA_SETUP$", extra_setup),
        ):
            server_script = server_script.replace(source, target)

        # Write server script
        script_path = self._write_server_script(remote_machine, tmp, server_script)
        print("\n\n\n-------------")
        print(script_path, cmd.bound_command)

        self.proc = cmd.popen(script_path, new_session=True)
        self._handle_server_startup()

        # if hasattr(remote_machine, "connect_sock"):
        if isinstance(remote_machine, ParamikoMachine):
            self.local_port = None
        else:
            import socket
            from contextlib import closing
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            with closing(s):
                s.bind(("localhost", 0))
                self.local_port = s.getsockname()[1]
            self.tun = remote_machine.tunnel(self.local_port, self.remote_port)

    def _find_python_command(self, remote_machine):
        """Find appropriate Python command across platforms"""
        major = sys.version_info[0]
        minor = sys.version_info[1]

        python_candidates = [
            f"python{major}.{minor}",
            f"python{major}",
            "python3",
            "python",
        ]

        for candidate in python_candidates:
            try:
                return remote_machine[candidate]
            except CommandNotFound:
                continue

        # Fallback to plumbum's python detection
        return remote_machine.python

    def _write_server_script(self, remote_machine, tmp_dir, server_script):
        """Write server script in a cross-platform way"""
        s = remote_machine.session()
        try:
            print(tmp_dir)
            s.run(f"cd '{tmp_dir}'")

            # Write script line by line to avoid shell escaping issues
            s.run("rm -f server.py")  # Remove if exists
            for line in server_script.split("\n"):
                # Escape single quotes in the line
                escaped_line = line.replace("'", "'\"'\"'")
                s.run(f"echo '{escaped_line}' >> server.py")

            # Get absolute path
            result = s.run("pwd")
            current_dir = result[1].strip()
            script_path = f"{current_dir}/server.py"
            print(s.run(f"cat '{script_path}'"))
            # print(s.run(f"env | grep path"))
            # print(s.run(f"uv"))

            return script_path
        finally:
            del s

    def _handle_server_startup(self):
        """Handle server startup and output monitoring"""
        line = ""
        save_forward = Event()

        try:
            if self.proc is None:
                raise Exception("stdout is None")
            line = self.proc.stdout.readline()
            self.remote_port = int(line.strip())
            print("remote port", self.remote_port)
            save_forward.set()

            def output_monitor():
                save_forward.wait()
                while True:
                    try:
                        if self.proc is None:
                            raise Exception("stdout is None")
                        line = self.proc.stdout.read(1)
                        if line != b"":
                            print(">", line, "<", sep="", end="")
                    except Exception as e:
                        print(e)
                        break

            thread = Thread(target=output_monitor, daemon=True)
            thread.start()

        except Exception:
            if self.proc:
                try:
                    self.proc.terminate()
                except Exception:
                    pass

            stdout, stderr = self.proc.communicate() if self.proc else (b"", b"")
            from rpyc.lib.compat import BYTES_LITERAL

            BYTES_LITERAL: Callable
            raise ProcessExecutionError(
                self.proc.argv if self.proc else [],
                self.proc.returncode if self.proc else -1,
                BYTES_LITERAL(line) + stdout,
                stderr,
            )


class CrossPlatformTempDir:
    """Cross-platform temporary directory context manager"""

    def __init__(self, remote_machine):
        self.remote_machine = remote_machine
        self.path = None
        self.session = None

    def __enter__(self):
        self.session = self.remote_machine.session()

        # Try Unix/Linux/macOS method first
        result = self.session.run("mktemp -d")

        if result[0] == 0:
            self.path = result[1].strip()
            return self.path

        # Try Python method (more portable)
        result = self.session.run(
            "python3 -c 'import tempfile; print(tempfile.mkdtemp())'"
        )

        if result[0] == 0:
            self.path = result[1].strip()
            return self.path

        # Fallback method
        import uuid

        temp_name = f"tmp_{uuid.uuid4().hex[:8]}"

        # Try different temp locations
        temp_bases = ["/tmp", "~/tmp", "."]
        for base in temp_bases:
            result = self.session.run(f"mkdir -p {base}/{temp_name}")
            if result[0] == 0:
                pwd_result = self.session.run(f"cd {base}/{temp_name} && pwd")
                if pwd_result[0] == 0:
                    self.path = pwd_result[1].strip()
                    return self.path

        raise RuntimeError("Could not create temporary directory")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not (self.path and self.session):
            return

        # Clean up temporary directory
        max_retries = 3
        for attempt in range(max_retries):
            result = self.session.run(f"rm -rf '{self.path}'")
            if result[0] == 0:
                break

            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry
            else:
                print(f"Warning: Could not clean up temp directory {self.path}")

        del self.session
