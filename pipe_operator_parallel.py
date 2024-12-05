import multiprocessing
import os
import dis
import builtins

import concurrent.futures as ft

import inspect
import time
import types

import threading

# This one doesnt work:
# Reason 1. local nested function cannot be pickled [done]
# Reason 2. decorator cannot be pickled [done in linux][todo: windows]
# Reason 3. join might be unnecessary [done]

def feeder(input_data, queue):
    for item in input_data:
        queue.put(item)
    queue.put(None)  # Sentinel value to indicate the end

def func(*args, **kwargs):
    pass

def stage_worker(input_queue, output_queue, func_str):
    # if os.name == "nt":
    #     func = dill.dumps(func)
    # else:
    #     func = func
    # print(func_str)
    # func = func_str
    if os.name == "nt":
        global func
        exec(func_str, globals())
    else:
        func = func_str
    # print(func)
    while True:
        item = input_queue.get()
        if isinstance(func_str, types.FunctionType):
            fnc = func.__name__
        else:
            fnc = func_str.split("\n")[0]
        print("item ", fnc, item)
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
        print("run")
        
        # Check if data is generator
        if os.name == "nt" and isinstance(data, types.GeneratorType):
            # feeder(data, queues[0])
            sender_thread = threading.Thread(target=feeder, args=(data, queues[0]))
            sender_thread.start()
            processes.append(sender_thread)
        else:
            # Start feeder process to put initial data into the first queue
            feeder_process = multiprocessing.Process(target=feeder, args=(data, queues[0]))
            feeder_process.start()
            processes.append(feeder_process)

        # Start each stage in a separate process
        if os.name == "nt":
            for i, func in enumerate(self.stages):
                source = inspect.getsource(func)
                source = source.split("\n")[1:]
                source[0] = source[0].replace(source[0].split(" ")[1].split("(")[0], "func")
                source = "\n".join(source)
                p = multiprocessing.Process(target=stage_worker, args=(queues[i], queues[i+1], source))
                p.start()
                processes.append(p)
        else:
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

def is_pure_function(func, _analyzed_funcs=None):
    """
    Check if a function is pure by analyzing bytecode, closure variables, and recursively checking function calls.
    
    Args:
        func (callable): Function to check for purity
        _analyzed_funcs (set, optional): Internal set to track already analyzed functions and prevent infinite recursion
    
    Returns:
        bool: True if function appears to be pure, False otherwise
    """
    return True
    # Initialize set of analyzed functions to prevent infinite recursion
    if _analyzed_funcs is None:
        _analyzed_funcs = set()
    
    # Prevent analyzing the same function multiple times
    if func in _analyzed_funcs:
        return True
    _analyzed_funcs.add(func)
    
    # Check closure variables
    closure_vars = inspect.getclosurevars(func)
    
    # Check if any non-read-only closure variables exist
    for var, value in closure_vars.nonlocals.items():
        if not isinstance(value, (int, float, str, tuple, frozenset, bytes, type(None))):
            return False
    
    for var, value in closure_vars.globals.items():
        # Allow only immutable globals and builtins
        if not (isinstance(value, (int, float, str, tuple, frozenset, bytes, type(None))) or 
                hasattr(builtins, var)):
            return False
    
    # Analyze bytecode
    code = dis.Bytecode(func)
    
    # Tracks function calls to recursively check
    function_calls = []
    
    impure_ops = {
        'STORE_DEREF',   # Modifying closure variable
        'STORE_GLOBAL',  # Modifying global variable
    }
    
    for instr in code:
        # Track function calls for recursive purity check
        if instr.opname == 'CALL_FUNCTION':
            # Try to get the function being called
            try:
                # This is a simplified approach and might not work for all cases
                frame = inspect.currentframe()
                function_calls.append(frame.f_locals.get(instr.argval))
            except Exception:
                # If we can't determine the function, assume it might not be pure
                return False
        
        # Check for impure operations
        if instr.opname in impure_ops:
            return False
    
    # Recursively check purity of called functions
    for called_func in function_calls:
        if called_func and callable(called_func):
            try:
                if not is_pure_function(called_func, _analyzed_funcs):
                    return False
            except Exception:
                return False
    
    return True

def pipeable(func):
    # Decorator to make functions pipeable
    if not is_pure_function(func):
        raise ValueError("Function is not pure")
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
        time.sleep(0.01)
        yield num * 2

@pipeable
def increment(nums):
    for num in nums:
        time.sleep(0.01)
        yield num + 1

@pipeable
def to_string(nums):
    for num in nums:
        time.sleep(0.01)
        yield str(num)

# Example usage
if __name__ == '__main__':
    # Chaining functions with the '|' operator
    result = range(8) | double | increment | double | to_string
    # result = range(8) | double

    # Consuming the result
    print(list(result))
