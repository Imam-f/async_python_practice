from recursiverpc import Recursive_RPC, localprocess, networkprocess, proxyprocess, RPC_Future
import time
import os
from dotenv import load_dotenv
load_dotenv()
import traceback

from typing import Callable
from queue import Queue
import paramiko
from plumbum.machines.paramiko_machine import ParamikoMachine

# import faulthandler
# faulthandler.enable()
# faulthandler.dump_traceback_later(timeout=10)
            
#################################################################

def worker_func(number):
    sum_num = 0
    for i in range(30000000):
    # for i in range(300000):
    # for i in range(3000000):
        sum_num += i
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    
    HOSTNAME = os.getenv("HOSTNAME")
    if HOSTNAME is None:
        HOSTNAME = "localhost"
    USER = os.getenv("USER")
    if USER is None:
        USER = "root"
    PORT = os.getenv("PORT")
    if PORT is None:
        PORT = 22
    else:
        PORT = int(PORT)
    PASSWORD = os.getenv("PASSWORD")
    if PASSWORD is None:
        PASSWORD = None
    
    # HOSTNAME_FORWARD = HOSTNAME
    # PORT_FORWARD = PORT
    # ssh_login = (USER, PASSWORD)
    
    # print("connecting")
    # print(HOSTNAME, USER, PORT, PASSWORD)
    sshmachine = ParamikoMachine(host=HOSTNAME,
                                 user=USER, 
                                 port=PORT, 
                                 password=PASSWORD,
                                 missing_host_policy=paramiko.AutoAddPolicy())
    
    if True:
        with Recursive_RPC(client=[
                    # proxyprocess(remote_port[1], HOSTNAME_FORWARD, PORT_FORWARD, [
                    #     localprocess(4),
                    #     networkprocess(2, HOSTNAME, remote_port[2], "tag1")
                    # ], ssh_login, {"tag1": (HOSTNAME, USER, PORT, PASSWORD, remote_port[2])}),
                    localprocess(4),
                    networkprocess(4, sshmachine),
                    # networkprocess(2, HOSTNAME, remote_port[0], stop),
                    # networkprocess(2, HOSTNAME, remote_port[3], stop2)
                ], conn={}) as pool:
            print("Connected")
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
    
    sshmachine = ParamikoMachine(host=HOSTNAME,
                                 user=USER, 
                                 port=PORT, 
                                 password=PASSWORD,
                                 keep_alive=True,
                                 connect_timeout=5,
                                 missing_host_policy=paramiko.AutoAddPolicy())
    if True:
        with Recursive_RPC(client=[
                    # proxyprocess(remote_port[1], HOSTNAME_FORWARD, PORT_FORWARD, [
                    #     localprocess(4),
                    #     networkprocess(2, HOSTNAME, remote_port[2], "tag1")
                    # ], ssh_login, {"tag1": (HOSTNAME, USER, PORT, PASSWORD, remote_port[2])}),
                    localprocess(2),
                    networkprocess(2, sshmachine),
                    # networkprocess(2, HOSTNAME, remote_port[3], stop2)
                ], conn={}) as pool:
            print("Hello", os.getpid())
            queue = Queue()
            queue_put = lambda x: queue.put(x)
            queue_get = lambda: queue.get()
            queue_pool = lambda: not queue.empty()
            # queue_put = queue
            # queue_get = queue
            
            def value_producer(queue, print):
                if callable(print):
                    print = print
                else:
                    print = __builtins__["print"]
                import time
                import os
                print("Hello there", os.getpid())
                for i in range(10):
                    print("Hello", i)
                    if callable(queue):
                        queue(i)
                    else:
                        queue.put(i)
                    time.sleep(0.4)
                if callable(queue):
                    queue(None)
                else:
                    queue.put(None)

            def value_consumer(queue, print):
                queue, pooler = queue
                if callable(print):
                    print = print
                else:
                    print = __builtins__["print"]
                import time
                import os
                print("Inigo Montoya", os.getpid())
                item: int | None = 0
                while True:
                    if callable(queue):
                        # if pooler():
                        if True:
                            item = queue() # type: ignore
                        else:
                            print("EMPTY")
                    else:
                        item = queue.get()
                    if item is None:
                        print("Done")
                        return 5
                    print(item + 7)
                    time.sleep(0.2)

            value1 = pool.apply_async(value_producer, queue_put, print)
            value2 = pool.apply_async(value_consumer, (queue_get, queue_pool), print)
            
            for async_result in RPC_Future.as_completed([value1, value2]):
                print(async_result)

################################################################

if __name__ == "__main__":
    start_time = time.time()
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
