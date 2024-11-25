import multiprocessing
import os
import inspect
import dis
import builtins

def func(*args, **kwargs):
    pass

def is_pure_function(func, _analyzed_funcs=None):
    """
    Check if a function is pure by analyzing bytecode, closure variables, and recursively checking function calls.
    
    Args:
        func (callable): Function to check for purity
        _analyzed_funcs (set, optional): Internal set to track already analyzed functions and prevent infinite recursion
    
    Returns:
        bool: True if function appears to be pure, False otherwise
    """
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
        # Allows chaining: 'self | other'
        if isinstance(other, CallablePipe):
            return Pipeline([self.func, other.func])
        elif isinstance(other, Pipeline):
            return Pipeline([self.func] + other.stages, data=other.data)
        else:
            raise ValueError("Can only chain with pipeable functions or pipelines")

    def __ror__(self, other):
        # Allows 'other | self', where 'other' is the initial data
        return Pipeline([self.func], data=other)

def feeder(input_data, queue):
    for item in input_data:
        queue.put(item)
    queue.put(None)  # Sentinel value to indicate the end

def stage_worker(input_queue, output_queue, func_str):
    exec(func_str, globals())
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
    def __init__(self, stages=None, data=None):
        self.stages = stages if stages else []
        self.data = data
        self.processes = []
        self.queues = []

    def __or__(self, other):
        if isinstance(other, CallablePipe):
            return Pipeline(self.stages + [other.func], data=self.data)
        else:
            raise ValueError("Can only chain with pipeable functions")

    def __ror__(self, other):
        # Allows data to be piped into the pipeline: 'data | pipeline'
        return Pipeline(self.stages, data=other)

    def gather(self):
        # Runs the pipeline and collects all results
        try:
            return list(self._run_pipeline())
        finally:
            self._terminate_processes()

    def take(self, n):
        # Runs the pipeline and collects the first 'n' results
        gen = self._run_pipeline()
        result = []
        try:
            for _ in range(n):
                result.append(next(gen))
        except StopIteration:
            pass
        finally:
            self._terminate_processes()
        return result

    def _run_pipeline(self):
        data = self.data
        num_stages = len(self.stages)
        self.processes = []
        self.queues = [multiprocessing.Queue(maxsize=1000) for _ in range(num_stages + 1)]

        # Feeder process
        feeder_process = multiprocessing.Process(target=feeder, args=(data, self.queues[0]))
        feeder_process.start()
        self.processes.append(feeder_process)

        # Stage processes
        if os.name == "nt":
            # print("The system is Windows.")
            # pool = ft.ProcessPoolExecutor(
            #     max_workers=8
            # )
            for i, func in enumerate(self.stages):
                # func = dill.dumps(func)
                source = inspect.getsource(func)
                source = source.split("\n")[1:]
                source[0] = source[0].replace(source[0].split(" ")[1].split("(")[0], "func")
                source = "\n".join(source)
                # print(source)
                # result = pool.submit(stage_worker, (queues[i], queues[i+1], source)).result()
                p = multiprocessing.Process(target=stage_worker, args=(self.queues[i], self.queues[i+1], source))
                p.start()
                self.processes.append(p)
        else:
            for i, func in enumerate(self.stages):
                p = multiprocessing.Process(target=stage_worker, args=(self.queues[i], self.queues[i+1], func))
                p.start()
                self.processes.append(p)

        # Output collector
        return output_collector(self.queues[-1])

    def _terminate_processes(self):
        # Clean up all processes and queues
        for p in self.processes:
            if p.is_alive():
                p.terminate()
            p.join()
        for q in self.queues:
            q.close()
        self.processes = []
        self.queues = []

    def __del__(self):
        # Ensure resources are cleaned up when the object is destroyed
        self._terminate_processes()

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
    # Build the pipeline (no execution happens here)
    pipeline = range(5) | double | increment | to_string

    # Run the pipeline and collect all results
    result = pipeline.gather()
    print("Gather all results:", result)

    # Build another pipeline
    pipeline2 = range(100) | double | increment | to_string

    # Run the pipeline and collect the first 5 results
    first_five = pipeline2.take(5)
    print("First five results:", first_five)
