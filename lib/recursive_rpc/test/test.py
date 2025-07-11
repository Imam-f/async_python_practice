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
    for i in range(30000000):
    # for i in range(300000):
        sum_num += i
    # print(number, sum)
    return number * number

# $env:Path = "C:\Users\User\.local\bin;$env:Path"
def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    with Recursive_RPC(client=[
                # proxyprocess(-1, "localhost", 18812, [
                #     localprocess(-1),
                #     networkprocess( -1, "localhost", 18813)
                # ]),
                # localprocess(-1),
                networkprocess(4, "localhost", 18812)
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
    stop = activate_ssh(os.getenv("HOSTNAME"), os.getenv("USER"), os.getenv("PORT"), os.getenv("PASSWORD"))
    start_time = time.time()
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.format_exc())
    finally:
        stop()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
