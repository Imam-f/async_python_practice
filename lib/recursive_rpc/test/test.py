from recursive_rpc import *
import time
import os
from dotenv import load_dotenv
load_dotenv()
import traceback
#################################################################

def worker_func(number):
    # time.sleep(random.random() * 2)  # Simulate a time-consuming task
    sum_num = 0
    # for i in range(30000000):
    for i in range(300000):
        sum_num += i
    # print(number, sum)
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    
    HOSTNAME: str = os.getenv("HOSTNAME") if os.getenv("HOSTNAME") else "localhost"
    USER: str = os.getenv("USER") if os.getenv("USER") else "root"
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 22
    PASSWORD = os.getenv("PASSWORD") if os.getenv("PASSWORD") else None
    
    remote_port = [18812, 18813, 18814]
    # stop = activate_ssh(HOSTNAME, 
    #                     USER, 
    #                     PORT, 
    #                     PASSWORD, 
    #                     remote_port[0])

    stop2 = activate_ssh(HOSTNAME, 
                        USER, 
                        PORT, 
                        PASSWORD, 
                        remote_port[1])
    
    HOSTNAME_FORWARD = HOSTNAME
    PORT_FORWARD = PORT
    ssh_login = (USER, PASSWORD)
    
    with Recursive_RPC(client=[
                proxyprocess(remote_port[2], HOSTNAME_FORWARD, PORT_FORWARD, [
                    localprocess(4),
                    networkprocess(2, HOSTNAME, remote_port[1], stop2)
                ], ssh_login),
                # localprocess(2),
                # networkprocess(4, HOSTNAME, remote_port[0], stop)
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
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
