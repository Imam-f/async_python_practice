from typing import Generator, TypeVar, Callable
import time
import random
import concurrent.futures as ft
# import multiprocessing
from dataclasses import dataclass

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
    host: int
    port: int
    number: int

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
    def __init__(self, client: list[tupleprocess]):
        self.client: list[tupleprocess] = client
        self.connection: list[None|Runner] = [None for i in range(len(client))]
        self.weight: list[int] = [0 for i in range(len(self.connection))]
        for index, val in enumerate(client):
            match val:
                case localprocess(i):
                    runner = ProcessRunner(i)
                case networkprocess(i, j, k):
                    runner = NetworkRunner(i, j, k)
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
        memo = {}
        def get_status(i):
            if memo.get(i):
                return memo.get(i)
            is_active, p_num, n_done_count, latency = i.status()
            memo[i] = p_num - n_done_count, latency
            return p_num - n_done_count, latency

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

    def apply(self, func: Callable[[any], T], /, *args, **kwargs) -> T:
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
    
    def map_ordered(self, iters: list[U], func: Callable[[U, any], T], /, *args, **kwargs) -> T:
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
    def as_completed(cls_act: list["RPC_Future[T]"]) -> Generator[T, None, None]:
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
        # self.pool = ft.ProcessPoolExecutor(
        #         max_workers=self.process_num
        #     )
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
    def __init__(self, host: int, port: int, num: int):
        # start server
        self.host = host
        self.port = port
        self.process_num = num if num >= 0 else multiprocessing.cpu_count()
        # self.process_num = 1
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
        self.pool = None

    def status(self) -> tuple[bool, int, int]:
        for i,v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)
        
        is_pool_active: bool = not not self.pool
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity, latency
        return (is_pool_active, process_num, not_done_count, 0)

class NetworkService():
    """
    I want to send this thing through network to run on other side
    """
    pass

class ProxyRunner(Runner):
    pass

#################################################################

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
                localprocess(-1),
                networkprocess("localhost", 18812, -1)
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

