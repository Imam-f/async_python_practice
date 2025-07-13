import concurrent.futures
import time
import os

def square(x, name="Me", number=0):
    print("Presleep", name, os.getpid())
    # time.sleep(2)  # Sleep for 2 seconds
    sum_num = 0
    for i in range(30000000):
    # for i in range(300000):
        sum_num += i
    return number * number

    print("Postsleep", number)
    return x ** 2

if __name__ == "__main__":
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
    # with concurrent.futures.ThreadPoolExecutor() as executor:
        print("Main", os.getpid())
        future = executor.submit(square, 2, number=1)
        future2 = executor.submit(square, 4, number=2, name="Me2")

        # Wait for the future to complete
        result = future.result()
        result2 = future2.result()

        print(result)  # Output: 4
        print(result2)  # Output: 4
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
