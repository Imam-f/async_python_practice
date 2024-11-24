import multiprocessing
import functools

# This one doesnt work:
# Reason 1. local nested function cannot be pickled
# Reason 2. decorator cannot be pickled
# Reason 3. join might be unnecessary

def feeder(input_data, queue):
    for item in input_data:
        queue.put(item)
    queue.put(None)  # Sentinel value to indicate the end

def stage_worker(input_queue, output_queue, func):
    while True:
        item = input_queue.get()
        if item is None:
            output_queue.put(None)  # Pass the sentinel to the next stage
            break
        for result in func([item]):
            output_queue.put(result)

def output_collector(output_queue):
    while True:
        item = output_queue.get()
        if item is None:
            break
        yield item

class Pipeline:
    def __init__(self, stages=None):
        self.stages = stages if stages else []

    def __or__(self, other):
        if isinstance(other, CallablePipe):
            # Add the next function to the pipeline
            return Pipeline(self.stages + [other.func])
        else:
            raise ValueError("Can only chain with pipeable functions")

    def __ror__(self, other):
        # 'other' is the initial data (generator/iterator)
        return self.run(other)

    def run(self, data):
        num_stages = len(self.stages)
        processes = []
        queues = [multiprocessing.Queue() for _ in range(num_stages + 1)]
        
        # Start feeder process to put initial data into the first queue
        feeder_process = multiprocessing.Process(target=feeder, args=(data, queues[0]))
        feeder_process.start()
        processes.append(feeder_process)

        # Start each stage in a separate process
        for i, func in enumerate(self.stages):
            p = multiprocessing.Process(target=stage_worker, args=(queues[i], queues[i+1], func))
            p.start()
            processes.append(p)

        # Collect output from the last queue
        output_gen = output_collector(queues[-1])

        # Optionally, join the processes to ensure they have finished
        # for p in processes:
        #     p.join()

        return output_gen

def pipeable(func):
    # Decorator to make functions pipeable
    return CallablePipe(func)

class CallablePipe:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        # Make the instance callable
        return self.func(*args, **kwargs)

    def __or__(self, other):
        if isinstance(other, CallablePipe):
            # Combine this function with the next in the pipeline
            return Pipeline([self.func, other.func])
        elif isinstance(other, Pipeline):
            # Add this function to the existing pipeline
            return Pipeline([self.func] + other.stages)
        else:
            raise ValueError("Can only chain with pipeable functions or pipelines")

    def __ror__(self, other):
        # 'other' is the initial data; start the pipeline
        return Pipeline([self.func]).run(other)

# Example pipeable functions
@pipeable
def double(nums):
    for num in nums:
        yield num * 2

@pipeable
def increment(nums):
    for num in nums:
        yield num + 1

@pipeable
def to_string(nums):
    for num in nums:
        yield str(num)

# Example usage
if __name__ == '__main__':
    # Chaining functions with the '|' operator
    result = range(5) | double | increment | to_string

    # Consuming the result
    print(list(result))
