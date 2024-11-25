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
def feeder(input_data, send_end):
    for item in input_data:
        send_end.send(item)
    send_end.send(None)  # Sentinel value
    send_end.close()

def stage_worker(recv_end, send_end, func):
    while True:
        item = recv_end.recv()
        if item is None:
            send_end.send(None)
            break
        for result in func([item]):
            send_end.send(result)
    recv_end.close()
    send_end.close()

# Output collector in main process
def output_collector(recv_end):
    while True:
        item = recv_end.recv()
        if item is None:
            break
        yield item
    recv_end.close()

# Parallel pipeline implementation using Pipe
def parallel_pipeline(data, functions):
    num_stages = len(functions)
    processes = []
    pipes = []

    # Create unidirectional pipes between stages
    for _ in range(num_stages + 1):
        recv_end, send_end = multiprocessing.Pipe(duplex=False)
        pipes.append((recv_end, send_end))

    feeder_process = multiprocessing.Process(target=feeder, args=(data, pipes[0][1]))
    feeder_process.start()
    processes.append(feeder_process)
    # Close the send end in the parent process
    pipes[0][1].close()

    # Stage processes
    for i, func in enumerate(functions):
        p = multiprocessing.Process(target=stage_worker, args=(pipes[i][0], pipes[i+1][1], func))
        p.start()
        processes.append(p)
        # Close the recv and send ends in the parent process
        pipes[i][0].close()
        pipes[i+1][1].close()

    # The last pipe's recv end is connected to the last stage
    output_recv_end = pipes[-1][0]
    output_gen = output_collector(output_recv_end)

    # Close the send end of the last pipe in the parent process
    pipes[-1][1].close()

    # Wait for processes to finish
    # for p in processes:
    #     p.join()

    return output_gen

# Example functions
def double(nums):
    for num in nums:
        time.sleep(0.0001)
        yield num * 2

def increment(nums):
    for num in nums:
        time.sleep(0.0001)
        yield num + 1

def to_string(nums):
    for num in nums:
        time.sleep(0.0001)
        yield str(num)

if __name__ == '__main__':
    # Large dataset
    data_size = 10_000  # Adjust the size as needed
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
Without delay: 100000 element
Sequential pipeline time: 0.0246 seconds
Parallel pipeline time: 2.1585 seconds

With delay: 1000 element
Sequential pipeline time: 4.2753 seconds
Parallel pipeline time: 1.5818 seconds
"""