import multiprocessing
import time

def worker(number):
    """A simple worker function that simulates a time-consuming task."""
    time.sleep(1)  # Simulate a time-consuming task
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        
        # Create a list of numbers to process
        numbers = list(range(10))
        
        # List to store the AsyncResult objects
        async_results = []
        
        # Apply the worker function to each number asynchronously
        for number in numbers:
            async_result = pool.apply_async(worker, (number,))
            async_results.append(async_result)
        
        # Retrieve the results
        results = [async_result.get() for async_result in async_results]
        
        print("Results:", results)

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

