from typing import Generator, TypeVar, Callable
import time
import random
import concurrent.futures as ft
import multiprocessing
from dataclasses import dataclass

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

################################################################

"""
TODO:
    # [done] intilaize each client
    # [done] make process runner
    # [done] make future
    # [done] make apply
    # [done] make map function
    # make proper scheduler
    # make network runner
"""

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
        for i, v in enumerate(client):
            match v:
                case localprocess(i):
                    runner = ProcessRunner(i)
                case networkprocess(i, j, k):
                    runner = NetworkRunner(i, j, k)
                case _:
                    raise TypeError
            self.connection[i] = runner
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

    def apply(self, func, /, *args, **kwargs) -> T:
        # Apply then wait
        runner: Runner = self.connection[0]
        if not runner:
            raise RuntimeError("No runner")
        result = runner.run(func, *args, **kwargs)
        return result.get()

    def apply_async(self, func, /, *args, **kwargs) -> "RPC_Future":
        # Apply then give future object
        runner: Runner = self.connection[0]
        if not runner:
            raise RuntimeError("No runner")
        return runner.run(func, *args, **kwargs)
    
    def map_ordered(self, iters: list[U], func: Callable[[U, any], T], /, *args, **kwargs) -> T:
        runner: Runner = self.connection[0]
        if not runner:
            raise RuntimeError("No runner")
        future_list: list["RPC_Future"] = []
        for i in iters:
            r = runner.run(func, *[i, *args], **kwargs)
            future_list.append(r)
        return [f.get() for f in future_list]

    def map_ordered_async(self, iters, func, /, *args, **kwargs) -> list["RPC_Future"]:
        return self.map_async(iters, func, *args, **kwargs)
    
    def map(self, iters, func,  /, *args, **kwargs):
        runner: Runner = self.connection[0]
        if not runner:
            raise RuntimeError("No runner")
        future_list: list["RPC_Future"] = []
        for i in iters:
            r = runner.run(func, *[i, *args], **kwargs)
            future_list.append(r)
        return [f for f in RPC_Future.as_completed(future_list)]
    
    def map_async(self, iters, func, /, *args, **kwargs) -> list["RPC_Future"]:
        runner: Runner = self.connection[0]
        if not runner:
            raise RuntimeError("No runner")
        future_list: list["RPC_Future"] = []
        for i in iters:
            r = runner.run(func, *[i, *args], **kwargs)
            future_list.append(r)
        return future_list
    
class RPC_Future:
    """
    This class is a placeholder of future value
    """
    def __init__(self, result: ft.Future, scheduler: "Runner"):
        self.result = result
        self.scheduler = scheduler

    def check_scheduler(self):
        return self.scheduler.status()

    def get(self):
        return self.result.result()

    def status(self):
        return self.result.done()

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
        self.pool = ft.ProcessPoolExecutor(
                max_workers=self.process_num
            )
        self.process_handle: list[RPC_Future] = []

    def run(self, func, /, *args, **kwargs) -> RPC_Future:
        result = self.pool.submit(func, *args, **kwargs)
        self.process_handle.append(RPC_Future(result, self))
        return self.process_handle[-1]

    def close(self):
        self.pool.shutdown()
        self.pool = None

    def status(self) -> tuple[bool, int, int]:
        for i,v in enumerate(self.process_handle):
            if self.process_handle[i].status():
                self.process_handle.remove(v)
        
        is_pool_active: bool = not not self.pool
        process_num: int = self.process_num
        not_done_count: int = len(self.process_handle)

        # Connnection, max capacity, used capacity
        return (is_pool_active, process_num, not_done_count)

class NetworkRunner(Runner):
    def __init__(self, host: int, port: int, num: int, runner: list[Runner]):
        pass

class GPURunner(Runner):
    pass

class FPGARunner(Runner):
    pass

#################################################################

def worker(number):
    time.sleep(random.random() * 2)  # Simulate a time-consuming task
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    with Recursive_RPC(client=[
                localprocess(-1),
                # networkprocess("localhost", 5050, -1)
            ]) as pool:
        
        # Create a list of numbers to process
        numbers = list(range(10))
        
        # List to store the AsyncResult objects
        async_results = []
        
        for number in numbers:
            print("Results 1:", pool.apply(worker, number))

        # Apply the worker function to each number asynchronously
        print()
        for number in numbers:
            async_result = pool.apply_async(worker, number)
            async_results.append(async_result)
        
        # Retrieve the results
        for async_result in RPC_Future.as_completed(async_results):
            print("Results 2:", async_result)
        
        print()
        for i in pool.map_ordered(numbers, worker):
            print("Results 3:", i)

        print()
        for i in pool.map_ordered_async(numbers, worker):
            print("Results 4:", i.get())

        print()
        for i in pool.map(numbers, worker):
            print("Results 5:", i)
        
        print()
        async_results = pool.map_async(numbers, worker)
        for i in RPC_Future.as_completed(async_results):
            print("Results 6:", i)
        
################################################################

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
