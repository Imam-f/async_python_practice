import multiprocessing
import time

import os
import inspect

def func(*args, **kwargs):
    pass

# Sequential pipeline implementation
def sequential_pipeline(data, functions):
    for func in functions:
        data = func(data)
    return data

# Feeder process
def feeder(input_data, queue):
    for item in input_data:
        while queue.full():  # Wait for space in the queue
            time.sleep(0.01)
        queue.put(item)
    #     print("put", item)
    # print("Done feeding")
    queue.put(None)  # Sentinel value

def stage_worker(input_queue, output_queue, func_str, name):
    try:
        exec(func_str, globals())
    except:
        func = func_str
    while True:
        item = input_queue.get(timeout=1)
        while output_queue.full():  # Wait for space in the queue
            time.sleep(0.01)
        if item is None:
            output_queue.put(None)
            # print("Done")
            break
        for result in func([item]):
            output_queue.put(result)

# Output collector
def output_collector(output_queue):
    # print("Start collecting")
    while True:
        item = output_queue.get(timeout=1)
        if item is None:
            # print("Done collecting")
            break
        yield item


# Parallel pipeline implementation
def parallel_pipeline(data, functions):
    num_stages = len(functions)
    processes = []
    queues = [multiprocessing.Queue(maxsize=1000) for _ in range(num_stages + 1)]

    feeder_process = multiprocessing.Process(target=feeder, args=(data, queues[0]), name="feeder")
    feeder_process.start()
    processes.append(feeder_process)

    # Stage processes
    for i, func in enumerate(functions):
        # source = inspect.getsource(func)
        # source = source.split("\n")
        # source[0] = source[0].replace(source[0].split(" ")[1].split("(")[0], "func")
        # source = "\n".join(source)
        # print(source)
        # p = multiprocessing.Process(target=stage_worker, args=(queues[i], queues[i+1], source, func.__name__), name=f"stage_{i}")
        p = multiprocessing.Process(target=stage_worker, args=(queues[i], queues[i+1], func, func.__name__), name=f"stage_{i}")
        p.start()
        processes.append(p)

    output_gen = output_collector(queues[-1])
    output_gen = list(output_gen)

    # Wait for processes to finish
    # for p in processes:
    #     p.join()

    return output_gen

# Example functions
def double(nums):
    for num in nums:
        time.sleep(0.0001)
        num = num * 2
        yield num

def increment(nums):
    for num in nums:
        time.sleep(0.0001)
        num = num + 1
        yield num

def to_string(nums):
    for num in nums:
        time.sleep(0.0001)
        num = str(num)
        yield num

if __name__ == '__main__':
    # Large dataset
    data_size = 100_000  # Adjust the size as needed
    # data_size = 10  # Adjust the size as needed
    # data_size = 200  # Adjust the size as needed
    data = range(data_size)

    # List of functions to apply
    functions = [double, increment, to_string]

    # Measure time for sequential pipeline
    start_time = time.perf_counter()
    sequential_result = sequential_pipeline(data, functions)
    sequential_output = list(sequential_result)
    sequential_time = time.perf_counter() - start_time

    # Print the results
    print(f"Sequential pipeline time: {sequential_time:.4f} seconds")

    # Measure time for parallel pipeline
    start_time = time.perf_counter()
    parallel_result = parallel_pipeline(data, functions)
    parallel_output = list(parallel_result)
    parallel_time = time.perf_counter() - start_time

    # Verify that both outputs are the same
    assert sequential_output == parallel_output, "Outputs do not match!"

    print(f"Parallel pipeline time: {parallel_time:.4f} seconds")

"""
With delay:
Sequential pipeline time: 54.1161 seconds
Parallel pipeline time: 27.5257 seconds

Without delay:
Sequential pipeline time: 0.0167 seconds
Parallel pipeline time: 7.0269 seconds

## There are 7 seconds of communciation overhead when using queue
"""