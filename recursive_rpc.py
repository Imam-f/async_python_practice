from typing import Optional, List, Tuple
from typing import Generator, TypeVar, Callable, Any
import time
import random
import os
import concurrent.futures as ft
# import multiprocessing
from dataclasses import dataclass

import paramiko
from scp import SCPClient

import multiprocess as multiprocessing
from multiprocess import Pool


import rpyc

##################################################################

@dataclass
class tupleprocess:
    number: int

@dataclass
class localprocess(tupleprocess):
    number: int

@dataclass
class networkprocess(tupleprocess):
    host: int | str
    port: int
    number: int

@dataclass
class proxyprocess(tupleprocess):
    number: int
    host: int | str
    port: int
    client: list["localprocess | networkprocess | proxyprocess"]

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
    def __init__(self, client: list[localprocess | networkprocess | proxyprocess]):
        self.client: list[localprocess | networkprocess | proxyprocess] = client
        self.connection: list[None|Runner] = [None for i in range(len(client))]
        self.weight: list[int] = [0 for i in range(len(self.connection))]
        for index, val in enumerate(client):
            match val:
                case localprocess(i):
                    runner = ProcessRunner(i)
                case networkprocess(i, j, k):
                    runner = NetworkRunner(i, j, k)
                case proxyprocess(i, j, k, l):
                    runner = ProxyRunner(i, j, k, l)
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
                self.connection[i] = None
        if any(self.connection):
            print(self.connection)
            print(self.connection[0])
            raise RuntimeError("Not all clients are closed")

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
                    cls[i] = None
    
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
        return self.result.get()

    def status(self):
        return self.result.ready()

    @staticmethod
    def as_completed(cls_act: list["RPC_Future[T]"]) -> Generator[T, None, None]:
        cls = cls_act.copy()
        while any(cls):
            for i, v in enumerate(cls):
                if v and v.status():
                    yield v.get()
                    cls[i] = None

################################################################

# TODO: make ssh runner
# TODO: make setup project script

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
        self.process_num = num if num >= 0 else multiprocessing.cpu_count()
        self.pool = Pool(self.process_num)
        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        result = self.pool.apply_async(func, args, kwargs)
        self.process_handle.append(RPC_Future(result, self))
        return self.process_handle[-1]

    def close(self):
        self.pool.close()
        self.pool = None

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
    def __init__(self, host: int | str, port: int, num: int, ssh_port: int = 22):
        # start server
        self.host = host
        self.port = port
        self.process_num = num if num >= 0 else multiprocessing.cpu_count()
        # self.process_num = 1
        try:
            self.conn = rpyc.classic.connect(host, port=port)
        except:
            self.ssh_con = activate_ssh(host, ssh_port, port)
            self.conn = rpyc.classic.connect(host, port=port)
        self.conn.execute("import time")
        self.conn.execute("import inspect")
        self.conn.execute("import random")
        self.conn.execute("import dill")
        self.conn.execute("from multiprocess import Pool as Pl")
        self.conn.execute("dill.settings['recurse'] = True")
        self.conn.execute(f"async_pool = Pl({self.process_num})")

        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        # teleport function
        self.conn.teleport(func)
        self.conn.namespace["args"] = args
        self.conn.namespace["kwargs"] = kwargs
        
        self.conn.execute("result = async_pool.apply_async(worker_func, args, kwargs)")
        
        result = self.conn.namespace["result"]
        self.process_handle.append(RPC_Future(result, self))
        return self.process_handle[-1]

    def close(self):
        self.conn.close()
        self.conn = None
        if getattr(self, "ssh_con", None):
            self.ssh_con.close()

    def status(self) -> tuple[bool, int, int]:
        for i,v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)
        
        is_pool_active: bool = not not self.conn
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)

class ProxyRunner(Runner):
    def __init__(self, num: int, host: int | str, port: int, clientlist: list[localprocess | networkprocess | proxyprocess]):
        self.host = host
        self.port = port
        self.num = num
        
        if host is not None:
            # raise NotImplementedError
            self.conn = rpyc.classic.connect(host, port=port)
            self.conn.execute("import recursive_rpc as rp")
            self.conn.execute("runner_list = []")
        
        self.process_num: int = 0
        self.weight: list[int] = []
        self.connection: list[Runner] = []
        self.process_handle: list[RPC_Future] = []
        
        for index, val in enumerate(clientlist):
            match val:
                case localprocess(i):
                    if host is not None:
                        self.conn.execute("runner_list.append(rp.ProcessRunner(" + str(num) + "))")
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        runner = ProcessRunner(i)
                case networkprocess(i, j, k):
                    if host is not None:
                        self.conn.execute("runner_list.append(rp.NetworkRunner(" + str(i) + ", " + str(j) \
                            + ", " + str(k) + "))")
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        runner = NetworkRunner(i, j, k)
                    runner = NetworkRunner(i, j, k)
                case proxyprocess(i, j, k, l):
                    if host is not None:
                        self.conn.execute("runner_list.append(rp.ProxyRunner(" + str(i) + ", " + str(j) \
                            + ", " + str(k) + ", " + str(l) + "))")
                        runner = self.conn.namespace["runner_list"][-1]
                    else:
                        runner = ProxyRunner(i, j, k, l)
                case _:
                    raise TypeError
            self.process_num += runner.process_num
            self.weight.append(runner.process_num)
            self.connection.append(runner)
            
    def schedule(self):
        rand = random.choices(self.connection, weights=self.weight, k=1)[0]
        return rand

    def run(self, func, /, *args, **kwargs):
        runner: Runner = self.schedule()
        if not runner:
            raise RuntimeError("No runner")
        return runner.run(func, *args, **kwargs)

    def close(self):
        for i in self.connection:
            i.close()
        self.connection.clear()
        self.connection = None

    def status(self) -> tuple[bool, int, int]:
        for i,v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)
        
        is_pool_active: bool = not not self.connection
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)

class RemoteUVRunner:
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
        remote_temp_dir: str = "/tmp",
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
    ) -> Tuple[str, str]:
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
            scp.put(local_script_path, remote_script_path)

        # 2. Construct and execute the command
        args_str = " ".join(script_args) if script_args else ""
        command = f"cd {self.remote_temp_dir} && uv run {script_filename} {args_str}"

        print(f"Executing remote command: {command}")
        _, stdout, stderr = self.ssh_client.exec_command(command)

        # It's important to read the streams before the connection closes
        stdout_str = stdout.read().decode("utf-8").strip()
        stderr_str = stderr.read().decode("utf-8").strip()

        return stdout_str, stderr_str

    def __enter__(self):
        """Context manager entry point: connects to the host."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point: disconnects from the host."""
        self.disconnect()

def activate_ssh(host: int | str, user: str, port: int, password: Optional[str] = None):
    # === CONFIGURATION ===
    REMOTE_HOST = "localhost" if host is None else host
    REMOTE_USER = "IMF-PC\\User" if user is None else user
    REMOTE_PORT = 2222 if port is None else port
    REMOTE_PASSWORD = "***REMOVED***" if password is None else password
    # Optional: if your key is not in the default location (~/.ssh/id_rsa)
    # SSH_KEY_PATH = "/path/to/your/private/key"

    # The local Python script to execute remotely
    SCRIPT_TO_RUN = "rpyc_classic.py"

    # Arguments to pass to the remote script
    SCRIPT_ARGS = [""]
    # === END CONFIGURATION ===

    """
    Demonstrates using the RemoteUVRunner class to execute a script remotely.
    """
    try:
        # Use the class as a context manager for automatic connection handling
        with RemoteUVRunner(host=REMOTE_HOST, 
                            user=REMOTE_USER, 
                            port=REMOTE_PORT, 
                            password=REMOTE_PASSWORD) as runner:
            # Call the run_script method
            stdout, stderr = runner.run_script(
                local_script_path=SCRIPT_TO_RUN,
                script_args=SCRIPT_ARGS
            )

            # Process the results
            print("\n--- Remote Execution Complete ---")
            if stdout:
                print("\n[STDOUT]:")
                print(stdout)
            if stderr:
                print("\n[STDERR]:")
                print(stderr)

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


#################################################################

activate_ssh("localhost", "IMF-PC\\User", 2222, "***REMOVED***")

def worker_func(number):
    # time.sleep(random.random() * 2)  # Simulate a time-consuming task
    sum_num = 0
    for i in range(30000000):
        sum_num += i
    # print(number, sum)
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    with Recursive_RPC(client=[
                proxyprocess(-1, "localhost", 18812, [
                    localprocess(-1),
                    networkprocess("localhost", 18812, -1)
                ]),
                # localprocess(-1),
                # networkprocess("localhost", 18812, -1)
            ]) as pool:
        
        # Create a list of numbers to process
        numbers = list(range(10))
        
        # List to store the AsyncResult objects
        async_results = []
        
        start_time = time.time()
        for number in numbers:
            print("Results 1:", pool.apply(worker_func, number))
        print(f"execution time: {time.time() - start_time:.2f} seconds")

        # Apply the worker function to each number asynchronously
        start_time = time.time()
        print()
        for number in numbers:
            async_result = pool.apply_async(worker_func, number)
            async_results.append(async_result)

        # Retrieve the results
        for async_result in RPC_Future.as_completed(async_results):
            print("Results 2:", async_result)
        print(f"execution time: {time.time() - start_time:.2f} seconds")
        
        start_time = time.time()
        print()
        for i in pool.map_ordered(numbers, worker_func):
            print("Results 3:", i)
        print(f"execution time: {time.time() - start_time:.2f} seconds")

        start_time = time.time()
        print()
        for i in pool.map_ordered_async(numbers, worker_func):
            print("Results 4:", i.get())
        print(f"execution time: {time.time() - start_time:.2f} seconds")

        start_time = time.time()
        print()
        for i in pool.map(numbers, worker_func):
            print("Results 5:", i)
        print(f"execution time: {time.time() - start_time:.2f} seconds")

        start_time = time.time()
        print()
        async_results = pool.map_async(numbers, worker_func)
        for i in RPC_Future.as_completed(async_results):
            print("Results 6:", i)
        print(f"execution time: {time.time() - start_time:.2f} seconds")

################################################################

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

