from typing import Generator 
import time
import random
import concurrent.futures as ft
import multiprocessing
from dataclasses import dataclass

def worker(number):
    number, = number
    """A simple worker function that simulates a time-consuming task."""
    time.sleep(random.random() * 2)  # Simulate a time-consuming task
    return number * number

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
    # make map function
    # make proper scheduler
    # make network runner
"""

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for c in self.connection:
            if not c:
                c.close()

    def apply(self, func, args):
        # Apply then wait
        runner: Runner = self.connection[0]
        if not runner:
            raise RuntimeError("No runner")
        result = runner.run(func, args)
        return result.get()

    def apply_async(self, func, /, *args, **kwargs) -> "RPC_Future":
        # Apply then give future object
        return self.connection[0].run(func, *args, **kwargs)
    
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
        return self.check_scheduler() and self.result.done()

    @staticmethod
    def as_completed(cls: list["RPC_Future"]) -> Generator["RPC_Future", None, None]:
        while any(cls):
            for i, v in enumerate(cls):
                if v and v.status():
                    yield v.get()
                    cls[i] = None

class Runner:
    """
    This class is concrete runner
    """
    def __init__(self):
        pass

    def run():
        pass

class ProcessRunner(Runner):
    def __init__(self, num: int):
        super().__init__()
        process_num = num if num >= 0 else multiprocessing.cpu_count()
        self.pool = ft.ProcessPoolExecutor(
                max_workers=process_num
            )

    def run(self, func, /, *args, **kwargs):
        result = self.pool.submit(func, *args, **kwargs)
        return RPC_Future(result, self)

    def close(self):
        self.pool.shutdown()
        self.pool = None

    def status(self):
        return not not self.pool

class NetworkRunner(Runner):
    pass

class GPURunner(ProcessRunner):
    pass

class RemoteGPURunner(NetworkRunner):
    pass

#################################################################

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
        
        # Apply the worker function to each number asynchronously
        for number in numbers:
            async_result = pool.apply_async(worker, (number,))
            async_results.append(async_result)
        
        # Retrieve the results
        for async_result in RPC_Future.as_completed(async_results):
            print("Results:", async_result)
        

################################################################

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
