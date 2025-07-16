from typing import Optional, List, Tuple
from typing import Generator, TypeVar, Callable, Any, Dict, Generic
import os
import random
import platform
import sys
import tempfile
import time
from dataclasses import dataclass

from concurrent.futures import Future
from threading import Thread, Event

import rpyc
from rpyc import BgServingThread
from rpyc.utils.zerodeploy import DeployedServer, SERVER_SCRIPT

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
    thd = t._start_in_thread()

    sys.stdout.write(f"{t.port}\n")
    sys.stdout.flush()
    try:
        read_data = sys.stdin.read()
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
    ssh_remote: Dict[str,Tuple]

################################################################

T = TypeVar("T")
U = TypeVar("U")

class Recursive_RPC:
    """
    This class is a scheduler and load balancer
    support local process and ssh
    """
    def __init__(self, client: list[localprocess | networkprocess | proxyprocess], conn: Dict[str, Tuple]):
        self.client: list[localprocess | networkprocess | proxyprocess] = client
        self.connection: list[None|Runner] = [None for i in range(len(client))]
        self.weight: list[int] = [0 for i in range(len(self.connection))]
        for index, val in enumerate(client):
            match val:
                case localprocess(i):
                    runner = ProcessRunner(i)
                case networkprocess(i, j):
                    runner = NetworkRunner(i, j)
                case proxyprocess(i, j, k, l, m, n):
                    runner = ProxyRunner(i, j, k, l, m, n) # type: ignore
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
    
    def map(self, iters, func,  /, *args, **kwargs):
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

    def run(self, func: Callable[[T], U], /, *args, **kwargs) -> RPC_Future[U]:
        ...

    def close(self): ...

    def status(self) -> Tuple: ...

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
        for i,v in enumerate(self.process_handle):
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
                try:
                    env_cmd = self.machine["/c/Program\\ Files/Git/usr/bin/env"]
                    uv_cmd = env_cmd("uv --version".split(" "))
                except:
                    raise RuntimeError("uv not installed")
                
                # read requirements txt with uv
                header = "#!/usr/bin/env uv run --script\n"
                curdir = os.getcwd()
                with tempfile.TemporaryDirectory() as temp_dir:
                    os.chdir(temp_dir)
                    # os.system(f"uv pip freeze")
                    os.system(f"uv pip freeze > requirements.txt 2> log.txt")
                    os.system(f"touch util.py")
                    # os.system(f"uv add -q --active -r requirements.txt --script util.py")
                    os.system(f"uv add -q --active -r requirements.txt --script util.py")
                    with open("util.py", "r") as f:
                        lines = f.read()
                        newlines = lines.replace("recursiverpc = { path = \"../../../../Documents/Dev/experiment/"
                                      "async_python_practice/lib/recursiverpc\" }", 
                                      "recursiverpc = { path = \"C:/Users/User/Documents/Dev/experiment/"
                                      "async_python_practice/lib/recursiverpc\", editable = true }")
                        header += newlines
                    os.chdir(curdir)
                server_script_user = header + "\n" + SERVER_SCRIPT
                extra_setup = ""
                
                major = sys.version_info[0]
                minor = sys.version_info[1]
                
                executable = env_cmd["uv", "run", "-q", "--python", f"{major}.{minor}", "--script"]
                self.server = DeployedWindowsServer(self.machine,
                                             server_script=server_script_user,
                                             extra_setup=extra_setup,
                                             server_class="rpyc.utils.server.OneShotServer",
                                             python_executable=executable)
                
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
        
        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        if self.conn is None:
            raise RuntimeError("No connection")
        
        # self.conn.ping()
        self.conn.teleport(func)
        self.conn.namespace["args"] = args
        self.conn.namespace["kwargs"] = kwargs
        self.conn.namespace["result"] = self.pool.apply_async(func, 
                                                              self.conn.namespace["args"], 
                                                              self.conn.namespace["kwargs"])
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
                self.conn.close()
                print("1 Cleaned up")
            self.conn = None
        except Exception as e:
            print("asdf", e)
        try:
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
        for i,v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)
        
        is_pool_active: bool = not not self.conn
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)

class ProxyRunner(Runner):
    def __init__(self, num: int, host: str, port: int, 
                 clientlist: list[localprocess | networkprocess | proxyprocess], 
                 ssh_login: Tuple | None = None,
                 ssh_remote: Dict[str,Tuple] = {}):
        self.host = host
        self.port = port
        self.num = num
        self.ssh_login = ssh_login
        
        self.stop = None
        if host is not None:
            user, password = self.ssh_login if self.ssh_login is not None else ("root", "")
            self.stop = activate_ssh(self.host, 
                                user,
                                self.port, 
                                password, 
                                num)
            
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
                        self.conn.execute("runner_list.append(rp.ProcessRunner(" + str(i) + "))")
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
                            print(f"HOSTNAME: {HOSTNAME}, USER: {USER}, PORT: {PORT}, PASSWORD: {PASSWORD}, remote_port: {remote_port}")
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
                            self.conn.execute("stop = rp.activate_ssh(HOSTNAME, USER, PORT, PASSWORD, remote_port)")
                            # self.conn.execute("stop = rp.activate_ssh(\"" + str(HOSTNAME) + \
                            #                   "\", \"" + str(USER) + \
                            #                   "\", " + str(PORT) + \
                            #                   ", \"" + str(PASSWORD) +\
                            #                   "\", " + str(remote_port) + ")")
                            print("Here 7")
                            func_name = f"stop"
                        self.conn.execute("runner_list.append(rp.NetworkRunner("\
                            + str(i)+ ", " + "\"" + str(j) + "\""  \
                            + ", " + str(k) + ", " + func_name + "))")
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        if callable(l):
                            runner = NetworkRunner(i, j, k, l)
                        else:
                            runner = NetworkRunner(i, j, k, None)
                case proxyprocess(i, j, k, l, m, n):
                    if host is not None:
                        self.conn.execute("runner_list.append(rp.ProxyRunner(" + str(i) \
                            + ", " + str(j) \
                            + ", " + str(k) + ", " + str(l) + ", " + str(m) \
                            + ", " + str(n) + "))")
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
            self.conn.execute(f"result = runner_list[{runner_index}].run(worker_func, *args, **kwargs)")
            
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
        for i,v in enumerate(self.process_handle):
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
        self.max_workers = max_workers if max_workers > 0 else multiprocessing.cpu_count()
        self.scheduler = []
        
        print("Connecting")
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
            # print("ALL BUSY")
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

class DeployedWindowsServer(DeployedServer):
    def __init__(self: "DeployedWindowsServer",
                remote_machine: SshMachine | ParamikoMachine,
                server_class="rpyc.utils.server.ThreadedServer",
                service_class="rpyc.core.service.SlaveService",
                server_script=SERVER_SCRIPT,
                extra_setup="",
                python_executable=None) -> None:
        self.proc = None
        self.tun = None
        self.remote_machine = remote_machine
        self._tmpdir_ctx = None
        
        if isinstance(python_executable, BoundCommand):
            cmd = python_executable
        elif python_executable:
            cmd = remote_machine[python_executable]
        else:
            major = sys.version_info[0]
            minor = sys.version_info[1]
            cmd = None
            for opt in [f"python{major}.{minor}", f"python{major}"]:
                try:
                    cmd = remote_machine[opt]
                except CommandNotFound:
                    pass
                else:
                    break
            if not cmd:
                cmd = remote_machine.python

        class tempdir:
            def __init__(self, remote_machine):
                self.remote_machine = remote_machine
                env_cmd = remote_machine["/c/Program\\ Files/Git/usr/bin/env"]
                # self.origdir = env_cmd["pwd"]().strip()
                # tempdir = env_cmd["python3", "-c"]("import tempfile; "
                #               "td = tempfile.TemporaryDirectory(); "
                #               "print(td.name)").strip()
                # print(tempdir, "11111111111")
                tempdir = env_cmd["mktemp"]("-d").strip()
                self.path = tempdir
                
                print(tempdir)
                self.s = remote_machine.session()
            
            def __enter__(self):
                # self.remote_machine.cwd.chdir(self.path)
                return self.path
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                # self.remote_machine.cwd.chdir(self.origdir)
                # env_cmd = self.remote_machine["/c/Program\\ Files/Git/usr/bin/env"]
                # print(self.origdir)
                # print(self.path.strip().split("/")[-1])
                path_folder = self.path.strip().split("/")[-1]
                self.s.run(f"cd AppData/Local/Temp/{path_folder}")
                self.s.run(f"cd ..")
                # print(self.s.run(f"pwd")[1].strip())
                # print(self.path, path_folder)
                # print(self.s.run(f"pwd"))
                # print(self.s.run(f"ls {path_folder}"))
                failed = False
                try:
                    self.s.run(f"rm -rf {path_folder}")
                    # print(self.s.run(f"rm -rf {path_folder}"))
                except Exception as e:
                    print("1", e)
                    failed = True
                if failed: 
                    try:
                        time.sleep(2)
                        self.s.run(f"rm -rf {path_folder}")
                        # print(self.s.run(f"rm -rf {path_folder}"))
                    except Exception as e:
                        print("2", e)
                        self.s.run(f"rm -rf {path_folder}")
                # try:
                #     print(self.s.run(f"ls {path_folder}"))
                # except Exception as e:
                #     print("2", e)
                # print(self.path, path_folder)
                # print(self.s.run(f"pwd"))
                del self.s

        self._tmpdir_ctx = tempdir(remote_machine)
        tmp = self._tmpdir_ctx.__enter__()
        # print(tmp)
        
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

        s = remote_machine.session()
        s.run(f"cd {tmp}")
        for line in server_script.split("\n"):
            s.run(f"echo '{line}' >> server.py")
        script = str(s.run(f"pwd")[1].strip()) + "/server.py"
        del s
        
        self.proc = cmd.popen(script, new_session=True)

        line = ""
        save_forward = Event()
        try:
            line = self.proc.stdout.readline()
            self.remote_port = int(line.strip())
            print("remote port", self.remote_port)
            save_forward.set()
            
            def thread_printer():
                print("thread printer")
                save_forward.wait()
                while True:
                    try:
                        line = self.proc.stdout.read(1)
                        # print(">", line.decode("utf-8"), line, "<", sep="", end="")
                        if line != b"":
                            print(">", line, "<", sep="", end="")
                        # line = self.proc.stderr.read(1)
                        # print("+", line, "+", sep="", end="")
                    except Exception as e:
                        # print("a", e)
                        break

            thread = Thread(target=thread_printer)
            thread.start()
            # del thread
        except Exception:
            try:
                self.proc.terminate()
            except Exception:
                pass
            # stdout, stderr = self.proc.communicate("exit\n".encode("utf-8"))
            stdout, stderr = self.proc.communicate()
            from rpyc.lib.compat import BYTES_LITERAL
            raise ProcessExecutionError(self.proc.argv, self.proc.returncode, BYTES_LITERAL(line) + stdout, stderr)

        if hasattr(remote_machine, "connect_sock"):
            # Paramiko: use connect_sock() instead of tunnels
            self.local_port = None
        else:
            self.local_port = rpyc.utils.factory._get_free_port()
            self.tun = remote_machine.tunnel(self.local_port, self.remote_port)
