from typing import Optional, List, Tuple
from typing import Generator, TypeVar, Callable, Any, Dict
import os
import random
import platform
import sys
import tempfile
import time
from dataclasses import dataclass

from concurrent.futures import Future, ProcessPoolExecutor
from threading import Thread

import inspect
import rpyc
from rpyc.utils.zerodeploy import DeployedServer
from plumbum import SshMachine, TF
from plumbum.machines.paramiko_machine import ParamikoMachine

old_print = print
# import paramiko
# from scp import SCPClient

# print = lambda *args, **kwargs: None
# import multiprocess as multiprocessing
# from multiprocess import Pool

##################################################################

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

class CustomList:
    def __init__(self, items):
        self.items = list(items)
    
    def sort(self, key=None, reverse=False):
        """Sorts the list in place and returns self for chaining."""
        self.items.sort(key=key, reverse=reverse)
        return CustomList(self.items)
    
    def filter(self, function):
        """Filters the list based on the given function and returns self for chaining."""
        self.items = list(filter(function, self.items))
        return CustomList(self.items)
    
    def map(self, function):
        """Applies the given function to each item in the list and returns self for chaining."""
        self.items = list(map(function, self.items))
        return CustomList(self.items)

    def inspect(self):
        """Print each element to screen and return as is"""
        for i in self.items:
            print(i)
        return self
    
    def tee(self, out, function=None):
        """Applies the given function to each item in the list and returns self for chaining."""
        out.clear()
        if function is None:
            function = lambda x: x
        for i in self.items:
            out.append(function(i))
        return CustomList(self.items)

    def teemap(self, out, function=None):
        """Applies the given function to each item in the list and returns self for chaining."""
        import copy
        out.clear()
        if function is None:
            function = lambda x: x
        temp = []
        for i in self.items:
            i = function(i)
            temp.append(i)
            out.append(copy.copy(i))
        return CustomList(temp)
 
    def take(self, n):
        """Takes the first n elements of the list and returns self for chaining."""
        if n > len(self.items):
            raise IndexError("List index out of range")
        self.items = self.items[:n]
        return CustomList(self.items)

    def skip(self, n):
        """Skips the first n elements of the list and returns self for chaining."""
        if n > len(self.items):
            raise IndexError("List index out of range")
        self.items = self.items[n:]
        return CustomList(self.items)

    def reduce(self, function, initial=None):
        """Reduces the list using the given function."""
        if initial is None:
            accumulator = self.items[0]
        for i in range(len(self.items) - 1):
            accumulator = function(accumulator, self.items[i + 1])
        return accumulator

    def reverse(self):
        """Reverses the list and returns self for chaining."""
        self.items = self.items[::-1]
        return CustomList(self.items)

    def get(self, index):
        """Returns the item at the given index."""
        return self.items[index]
    
    def to_list(self):
        """Returns the underlying list."""
        return self.items

    def __repr__(self):
        return f"CustomList({self.items})"

    def __getitem__(self, index):
        return self.items[index]

    def __setitem__(self, index, value):
        raise NotImplementedError

    def __len__(self):
        return len(self.items)

def pipe(x, *fns):
    """
    pipe(x, f1, f2, (f3, (2), {})) == f3(f2(f1(x)), 2, **{})
    """
    for i in fns:
        match i:
            case (func, args, kwargs) if (callable(func) 
                    and isinstance(args, tuple) 
                    and isinstance(kwargs, dict)):
                x = func(x, *args, **kwargs)
            case (func, args) if (callable(func)
                                    and isinstance(args, tuple)):
                x = func(x, *args)
            case (func, kwargs) if (callable(func)
                                    and isinstance(kwargs, dict)):
                x = func(x, **kwargs)
            case func if callable(func):
                x = func(x)
            case _:
                raise TypeError
    return x 

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
                    runner = ProxyRunner(i, j, k, l, m, n)
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
        result = runner.run(func, *args, **kwargs)
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

class RPC_Future:
    """
    This class is a placeholder of future value
    """
    def __init__(self, result, scheduler: "Runner"):
        self.result = result
        self.scheduler = scheduler

    def check_scheduler(self):
        return self.scheduler.status()

    def get(self):
        return self.result.result()

    def status(self):
        return self.result.ready()

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
        pass

    def close(self):
        pass

    def status(self):
        pass

class ProcessRunner(Runner):
    def __init__(self, num: int):
        self.pool: "Pool" = Pool(num)
        self.process_num = self.pool.max_workers
        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        # print("============", func.__name__)
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
                    print(env_cmd("uv --version".split(" ")))
                    uv_cmd = self.machine['.local/bin/uv']
                except:
                    raise RuntimeError("uv not installed")
                
                from rpyc.utils.zerodeploy import SERVER_SCRIPT
                
                # read requirements txt with uv
                header = "#!/usr/bin/env uv run --script\n"
                curdir = os.getcwd()
                with tempfile.TemporaryDirectory() as temp_dir:
                    print(temp_dir)
                    os.chdir(temp_dir)
                    os.system(f"uv pip freeze > requirements.txt")
                    os.system(f"touch util.py")
                    print(os.popen(f"cat requirements.txt").read())
                    os.system(f"uv add --active -r requirements.txt --script util.py")
                    with open("util.py", "r") as f:
                        lines = f.read()
                        newlines = lines.replace("recursive-rpc = { path = \"../../../../Documents/Dev/experiment/"
                                      "async_python_practice/lib/recursive_rpc\" }", 
                                      "recursive-rpc = { path = \"C:/Users/User/Documents/Dev/experiment/"
                                      "async_python_practice/lib/recursive_rpc\" }")
                        header += newlines
                    print(header)
                    os.chdir(curdir)
                    # time.sleep(50)
                # add deps to script with uv
                # add rpyc to script
                # add rpyc with uv
                # send script to remote
                
                server_script_user = header + "\n" + SERVER_SCRIPT[1:]
                extra_setup = ""
                
                major = sys.version_info[0]
                minor = sys.version_info[1]
                
                print(server_script_user)
                os._exit(0)
                
                # executable = [f"python{major}.{minor}"]
                executable = f"uv run --python {major}.{minor} --script"
                self.server = DeployedServer(self.machine,
                                             extra_setup=extra_setup,
                                             python_executable=executable)
                
                # check uv
                # start with uv
                
                self.conn = self.server.classic_connect()
                # print("connected")
                # if self.machine['uv --version']() & TF(1):
                #     raise RuntimeError("uv not installed")
                # 
                # def start_rpyc_server():
                #     f_name = f".rpyc_temp{int(10000 * random.random())}.py"
                #     while self.machine[f'cat {f_name}']() & TF(1):
                #         print("File exists")
                #         f_name = f".rpyc_temp{int(10000 * random.random())}.py"
                #     script = inspect.getsource(cl)
                #     self.machine[f"echo '{script}' > {f_name}"]()
                #     
                #     major, minor, patch = platform.python_version_tuple()
                #     self.machine[f"uv run --python {major}.{minor}.{patch} --script {f_name} -p 18812 -m oneshot"]()
                # 
                # self.thread = Thread(target=start_rpyc_server)
                # self.thread.start()
                # self.conn = rpyc.classic.connect("localhost", port=18812)
            case (hostname, port):
                self.conn = rpyc.classic.connect(hostname, port=port)
            case _:
                raise RuntimeError("Invalid connection")
        
        # check dependency
        # run pool on remote instance
        self.conn.execute("from recursive_rpc import *")
        self.conn.execute(f"pool = ProcessRunner({self.process_num})")
        
        # self.conn.execute("from multiprocess import Pool as Pl")
        # self.conn.execute(f"pool = Pl({self.process_num})")

        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        # teleport function
        self.conn.teleport(func)
        self.conn.namespace["args"] = args
        self.conn.namespace["kwargs"] = kwargs
    
        result = self.conn.eval(f"pool.apply_async({func.__name__}, args, kwargs)")
        self.process_handle.append(RPC_Future(result, self))
        return self.process_handle[-1]

    def close(self):
        self.conn.execute("pool.close()")
        self.conn.close()
        if self.server is not None:
            self.server.close()
        if self.machine is not None:
            self.machine.close()

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

    def status(self) -> tuple[bool, int, int]:
        for i,v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)
        
        is_pool_active: bool = not not self.connection
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)

#################################################################

# def start_rpyc_server(port: int = 18812, f_name: str = "rpyc_classic.py"):
#     run_uv(["run", f_name, "-p", str(port), "-m", "oneshot"])

class Pool:
    def __init__(self, max_workers: int, offset: int = 0):
        import multiprocessing
        self.max_workers = max_workers if max_workers > 0 else multiprocessing.cpu_count()
        self.scheduler = []
        
        for _ in range(max_workers):
            conn = rpyc.classic.connect_multiprocess()
            self.scheduler.append(conn)

        # assign to scheduler
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
        self.scheduler_index = index
        return self.scheduler[index]
    
    def apply_async(self, func, args, kwargs):
        runner = self._schedule()
        function = runner.teleport(func)
        
        result_future = Future()
        def function_async(*args, **kwargs):
            nonlocal result_future
            result = function(*args, **kwargs)
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
# 
# from uv import find_uv_bin
# 
# def _detect_virtualenv() -> str:
#     """
#     Find the virtual environment path for the current Python executable.
#     """
# 
#     # If it's already set, then just use it
#     value = os.getenv("VIRTUAL_ENV")
#     if value:
#         return value
# 
#     # Otherwise, check if we're in a venv
#     venv_marker = os.path.join(sys.prefix, "pyvenv.cfg")
# 
#     if os.path.exists(venv_marker):
#         return sys.prefix
# 
#     return ""
# 
# def run_uv(args) -> None:
#     uv = os.fsdecode(find_uv_bin())
# 
#     env = os.environ.copy()
#     venv = _detect_virtualenv()
#     if venv:
#         env.setdefault("VIRTUAL_ENV", venv)
# 
#     # Let `uv` know that it was spawned by this Python interpreter
#     env["UV_INTERNAL__PARENT_INTERPRETER"] = sys.executable
# 
#     if sys.platform == "win32":
#         import subprocess
# 
#         completed_process = subprocess.run([uv, *args], env=env)
#         sys.exit(completed_process.returncode)
#     else:
#         os.execvpe(uv, [uv, *args], env=env)

#################################################################

class RemoteUVRunner_old:
    """
    A class to manage SSH connections for running Python scripts with 'uv'.

    This class handles connecting to a remote host, copying a script,
    and executing it using 'uv run', which manages dependencies defined
    within the script's '/// script' block.

    It can be used as a context manager to ensure the SSH connection
    is properly closed.
    """

    def __init__(
        self,
        host: int | str,
        user: str,
        port: int = 22,
        password: Optional[str] = None,
        remote_temp_dir: str = "~/tmp",
        ssh_key_path: Optional[str] = None,
    ):
        """
        Initializes the RemoteUVRunner.

        Args:
            host (str): The remote server hostname or IP address.
            user (str): The username for the SSH connection.
            remote_temp_dir (str): The temporary directory on the remote host.
            ssh_key_path (Optional[str]): Path to the private SSH key.
        """
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.remote_temp_dir = remote_temp_dir
        self.ssh_key_path = ssh_key_path
        self.ssh_client: Optional[paramiko.SSHClient] = None

    def connect(self):
        """Establishes the SSH connection."""
        if self.ssh_client and self.ssh_client.get_transport().is_active():
            print("Already connected.")
            return

        try:
            print(f"Connecting to {self.user}@{self.host}...")
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.host,
                username=self.user,
                port=self.port,
                password=self.password,
                key_filename=self.ssh_key_path,
            )
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.ssh_client = None
            raise

    def disconnect(self):
        """Closes the SSH connection."""
        if self.ssh_client:
            print("Closing SSH connection.")
            self.ssh_client.close()
            self.ssh_client = None

    def run_script(
        self, local_script_path: str, script_args: Optional[List[str]] = None
    ) -> Tuple[Any, Any, Any]:
        """
        Copies a script to the remote host and executes it with 'uv run'.

        Args:
            local_script_path (str): The local path to the Python script.
            script_args (Optional[List[str]]): A list of command-line
                                               arguments for the script.

        Returns:
            Tuple[str, str]: A tuple containing the stdout and stderr from the
                             remote command execution.
        """
        if not self.ssh_client:
            raise ConnectionError("Not connected. Call connect() first or use as a context manager.")

        if not os.path.exists(local_script_path):
            raise FileNotFoundError(f"Local script not found at: {local_script_path}")

        script_filename = os.path.basename(local_script_path)
        remote_script_path = f"{self.remote_temp_dir}/{script_filename}"

        # 1. Copy the file using SCP
        with SCPClient(self.ssh_client.get_transport()) as scp:
            print(f"Copying {local_script_path} to {self.host}:{remote_script_path}...")
            # scp.put(local_script_path, remote_script_path)
            # check if file exists
            sftp = self.ssh_client.open_sftp()
            try:
                print(sftp.stat(remote_script_path))
            # if self.client(remote_script_path):
            except (IOError, FileNotFoundError):
                print(f"File {remote_script_path} does not exist")
                scp.put(local_script_path, remote_script_path)

        # 2. Construct and execute the command
        args_str = " ".join(script_args) if script_args else ""
        major, minor, patch = platform.python_version_tuple()
        command = f"cd {self.remote_temp_dir} && uv run --python {major}.{minor}.{patch} --script {script_filename} {args_str}"

        print(f"Executing remote command: {command}")
        stdin, stdout, stderr = self.ssh_client.exec_command(command)

        # It's important to read the streams before the connection closes
        # stdout_str = stdout.read().decode("utf-8").strip()
        # stderr_str = stderr.read().decode("utf-8").strip()

        # return stdin, stdout_str, stderr_str
        return stdin, stdout, stderr

    def __enter__(self):
        """Context manager entry point: connects to the host."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point: disconnects from the host."""
        self.disconnect()

def activate_ssh_old(
            host: str, 
            user: str, 
            port: int, 
            password: Optional[str] = None,
            remote_port: int = 18812
        ) -> Callable[[], None]:
    """
    Start a remote SSH process and return a stop function.
    
    Returns:
        A callable that stops the remote process
    """
    
    # logfile = "activate_ssh.log"
    # logfile_handler = open(logfile, "w")
    
    print(f"Activating SSH connection to {host}:{port}...")
    # print(f"Activating SSH connection to {host}:{port}...", file=logfile_handler)
    # logfile_handler.flush()
    
    # Configuration
    SCRIPT_TO_RUN = "rpyc_classic.py"
    SCRIPT_ARGS = ["-m", "oneshot", "-p", str(remote_port)]
    
    # Shared state
    runner = None
    thread_running = Event()
    stop_requested = Event()
    
    def ssh_runner():
        nonlocal runner
        try:
            # Create and connect to remote host
            with RemoteUVRunner(
                host=host, 
                user=user, 
                port=port, 
                password=password, 
                remote_temp_dir="."
            ) as runner:
                print(f"Connected to {host}:{port}")
                # print(f"Connected to {host}:{port}", file=logfile_handler)
                # logfile_handler.flush()
                
                # Start the remote script
                stdin, stdout, stderr = runner.run_script(
                    local_script_path=SCRIPT_TO_RUN,
                    script_args=SCRIPT_ARGS
                )
                
                stdin.channel.setblocking(False)
                thread_running.set()  # Signal that we're ready
                
                print("Remote script started successfully")
                # print("Remote script started successfully", file=logfile_handler)
                # logfile_handler.flush()
                
                # Keep the connection alive until stop is requested
                stop_requested.wait()
                
                print("Stopping remote process...")
                # print("Stopping remote process...", file=logfile_handler)
                # logfile_handler.flush()
                
                # Send interrupt signal to remote process
                print("closing stdin")
                if stdin:
                    try:
                        stdin.channel.send('\x03')  # Send Ctrl+C
                        # stdin.channel.flush()
                    except Exception as e:
                        print(e)
                
                # Process any remaining output
                print("closing stdout")
                if stdout:
                    try:
                        remaining_output = stdout.read()
                        if remaining_output:
                            print(f"[STDOUT]: {remaining_output}")
                            # print(f"[STDOUT]: {remaining_output}", file=logfile_handler)
                            # logfile_handler.flush()
                    except Exception as e:
                        print("err out",e)
                
                print("closing stderr")
                if stderr:
                    try:
                        remaining_errors = stderr.read()
                        if remaining_errors:
                            print(f"[STDERR]: {remaining_errors}")
                            # print(f"[STDERR]: {remaining_errors}", file=logfile_handler)
                            # logfile_handler.flush()
                    except Exception as e:
                        print("err", e)
                
                try:
                    stdin.channel.close()
                except Exception as e:
                    print(e)
                finally:
                    print(stdin.channel.closed)
                    
        except FileNotFoundError as e:
            print(f"Script not found: {e}")
        except Exception as e:
            print(f"SSH connection error: {e}")
        finally:
            thread_running.clear()
            print("SSH connection closed")
    
    # Start the SSH connection in background thread
    ssh_thread = Thread(target=ssh_runner)
    ssh_thread.start()
    
    # Wait for connection to be established
    if not thread_running.wait(timeout=30):
        print("Timeout waiting for SSH connection")
        stop_requested.set()
        return lambda: None
    
    print("SSH connection established")
    # print("SSH connection established", file=logfile_handler)
    # logfile_handler.flush()
    class Stopper:
        def  __call__(self, *args: Any, **kwds: Any) -> Any:
            """Stop the remote process and close SSH connection"""
            if not stop_requested.is_set():
                print("Requesting stop...")
                stop_requested.set()
                ssh_thread.join(timeout=60)
                print("Remote process stopped")
        
        def __repr__(self) -> str:
            return "None"
        
        def __str__(self) -> str:
            return "None"

    stop = Stopper()
    
    return stop
