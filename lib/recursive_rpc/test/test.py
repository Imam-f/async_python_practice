from recursive_rpc import *
import time
import os
from dotenv import load_dotenv
load_dotenv()
load_dotenv()
import traceback

from queue import Queue
import paramiko
from plumbum.machines.paramiko_machine import ParamikoMachine
from plumbum import PuttyMachine
from plumbum.machines.ssh_machine import SshMachine

#################################################################

def worker_func(number):
    sum_num = 0
    for i in range(30000000):
    # for i in range(300000):
        sum_num += i
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    
    # HOSTNAME: str = os.getenv("HOSTNAME") if os.getenv("HOSTNAME") else "localhost"
    # USER: str = os.getenv("USER") if os.getenv("USER") else "root"
    # PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 22
    # PASSWORD = os.getenv("PASSWORD") if os.getenv("PASSWORD") else None
    
    HOSTNAME: str = 'localhost'
    USER: str = "IMF-PC\\User"
    PORT = 2222
    PASSWORD = "***REMOVED***"
    
    remote_port = [18812, 18813, 18814, 18815]
    # stop = activate_ssh(HOSTNAME,
    #                     USER,
    #                     PORT,
    #                     PASSWORD,
    #                     remote_port[0])
    # stop()
    # stop2 = activate_ssh(HOSTNAME,
    #                     USER,
    #                     PORT,
    #                     PASSWORD,
    #                     remote_port[3])
    # stop2()

    # HOSTNAME_FORWARD = HOSTNAME
    # PORT_FORWARD = PORT
    # ssh_login = (USER, PASSWORD)
    
    # print("connecting")
    # print(HOSTNAME, USER, PORT, PASSWORD)
    # with ParamikoMachine(host=HOSTNAME,
    #                              user=USER, 
    #                              port=PORT, 
    #                              password=PASSWORD,
    #                     missing_host_policy=paramiko.AutoAddPolicy()) as sshmachine:
    #     print("asdfasd")
    # 
    # sshmachine = ParamikoMachine(host=HOSTNAME, 
    #                              user=USER, 
    #                              port=PORT, 
    #                              password=PASSWORD,
    #                              missing_host_policy=paramiko.AutoAddPolicy(), 
    #                              connect_timeout=20)
    # 
    # print("connected")
    
    if False:
        with Recursive_RPC(client=[
                    # proxyprocess(remote_port[1], HOSTNAME_FORWARD, PORT_FORWARD, [
                    #     localprocess(4),
                    #     networkprocess(2, HOSTNAME, remote_port[2], "tag1")
                    # ], ssh_login, {"tag1": (HOSTNAME, USER, PORT, PASSWORD, remote_port[2])}),
                    localprocess(8),
                    # networkprocess(2, HOSTNAME, remote_port[0], stop),
                    # networkprocess(2, HOSTNAME, remote_port[3], stop2)
                ], conn={}) as pool:
            
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
    if True:
        with Recursive_RPC(client=[
                    # proxyprocess(remote_port[1], HOSTNAME_FORWARD, PORT_FORWARD, [
                    #     localprocess(4),
                    #     networkprocess(2, HOSTNAME, remote_port[2], "tag1")
                    # ], ssh_login, {"tag1": (HOSTNAME, USER, PORT, PASSWORD, remote_port[2])}),
                    localprocess(2),
                    # networkprocess(2, sshmachine),
                    # networkprocess(2, HOSTNAME, remote_port[3], stop2)
                ], conn={}) as pool:
            
            queue = Queue()
            # queue_put = lambda x: queue.put(x)
            # queue_get = lambda: queue.get()
            queue_put = queue
            queue_get = queue
            value1 = pool.apply_async(value_producer, queue_put, print)
            value2 = pool.apply_async(value_consumer, queue_get, print)
            print(value1.get())
            print(value2.get())

def value_producer(queue, print):
    import time
    import os
    print("Hello", os.getpid())
    for i in range(10):
        print("Hello", i)
        if callable(queue):
            queue(i)
        else:
            queue.put(i)
        time.sleep(0.2)
        # print(i)
    if callable(queue):
        queue(None)
    else:
        queue.put(None)

def value_consumer(queue, print):
    import os
    print("Inigo Montoya", os.getpid())
    item = 0
    while True:
        if callable(queue):
            item = queue()
        else:
            item = queue.get()
        if item is None:
            return 5
        print(item + 7)

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
