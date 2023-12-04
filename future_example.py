import concurrent.futures
import time

def square(x, name="Me", number=0):
    print("Presleep", name)
    time.sleep(2)  # Sleep for 2 seconds
    print("Postsleep", number)
    return x ** 2

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
    # with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(square, 2, number=1)
        future2 = executor.submit(square, 4, number=2, name="Me2")

        # Wait for the future to complete
        result = future.result()
        result2 = future2.result()

        print(result)  # Output: 4
        print(result2)  # Output: 4
