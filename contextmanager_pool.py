import multiprocessing
import inspect
from typing import Iterable, Union, cast

class MultiprocessedFunctions:
    """Context manager that runs functions defined in its block in separate processes."""
    def __init__(self):
        self.processes = []
        self.functions = []
        self.frame_locals_before = None
        self.result_queue = multiprocessing.Queue()
        self.results = {}
        self._is_active = True
        self._handled_functions = set()  # Track functions handled by nested contexts
    
    def __enter__(self):
        # Capture the caller's frame and its local variables before the block
        frame = inspect.currentframe()
        if frame is None:
            raise
        frame = frame.f_back
        if frame is None:
            raise
        self.frame_locals_before = set(frame.f_locals.keys())
        
        # Mark this context as active in the frame
        frame.f_locals['__multiprocessed_functions_active__'] = self
        
        return self.results
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Mark this context as inactive
        self._is_active = False
        
        # Get the caller's frame and check for new functions
        frame = inspect.currentframe()
        if frame is None:
            raise
        frame = frame.f_back
        if frame is None:
            raise
        frame_locals_after = frame.f_locals
        
        # Remove the active flag
        if '__multiprocessed_functions_active__' in frame_locals_after:
            del frame.f_locals['__multiprocessed_functions_active__']
        
        # Find newly defined functions (excluding those handled by nested ParallelFor)
        for name, obj in frame_locals_after.items():
            if self.frame_locals_before is None:
                continue
            if (name not in self.frame_locals_before and 
                callable(obj) and 
                inspect.isfunction(obj) and
                name not in self._handled_functions):  # Skip if already handled
                self.functions.append(obj)
        
        # Create and start a process for each function
        for func in self.functions:
            # Wrap function to put result in queue
            process = multiprocessing.Process(
                target=self._run_with_queue, 
                args=(func,),
                name=func.__name__
            )
            self.processes.append(process)
            process.start()
            print(f"Started process for: {func.__name__} (PID: {process.pid})")
        
        # Join all processes
        for process in self.processes:
            process.join()
            print(f"Joined process: {process.name} (PID: {process.pid})")
        
        # Collect results from queue and store in dictionary
        while not self.result_queue.empty():
            result = self.result_queue.get()
            func_name = result['function']
            if 'error' in result:
                self.results[func_name] = {'error': result['error'], 'pid': result['pid']}
            else:
                self.results[func_name] = result['data']
        
        print(f"\n--- Collected {len(self.results)} results ---")
        
        return False  # Don't suppress exceptions
    
    def _run_with_queue(self, func):
        """Wrapper to execute function and put result in queue."""
        try:
            result = func()
            self.result_queue.put({
                'function': func.__name__,
                'data': result,
                'pid': multiprocessing.current_process().pid
            })
        except Exception as e:
            self.result_queue.put({
                'function': func.__name__,
                'error': str(e),
                'pid': multiprocessing.current_process().pid
            })

class ParallelFor:
    """Context manager that parallelizes function execution over iterations or elements."""
    
    def __init__(self, items: Union[int, Iterable]):
        """
        Initialize ParallelFor context manager.
        
        Args:
            items: Either an integer (number of repetitions) or an iterable to iterate over
        """
        if isinstance(items, int):
            self.items = range(items)
        else:
            self.items = list(items)
        
        self.processes = []
        self.frame_locals_before = None
        self.result_queue = multiprocessing.Queue()
        self.results = {}
        self.parent_context = None
        self.parent_obj = None
        self.function_name = None
    
    def __enter__(self):
        # Capture the caller's frame and its local variables before the block
        frame = inspect.currentframe()
        if frame is None:
            raise
        frame = frame.f_back
        if frame is None:
            raise
        self.frame_locals_before = set(frame.f_locals.keys())
        
        # Check if we're inside an ACTIVE MultiprocessedFunctions context
        check_frame = frame
        while check_frame is not None:
            if '__multiprocessed_functions_active__' in check_frame.f_locals:
                parent_obj = check_frame.f_locals['__multiprocessed_functions_active__']
                if isinstance(parent_obj, MultiprocessedFunctions) and parent_obj._is_active:
                    self.parent_context = parent_obj.results
                    self.parent_obj = parent_obj
                    print("Detected active parent MultiprocessedFunctions context")
                    break
            check_frame = check_frame.f_back
        
        return self.results
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Get the caller's frame and check for new functions
        frame = inspect.currentframe()
        if frame is None:
            raise
        frame = frame.f_back
        if frame is None:
            raise
        frame_locals_after = frame.f_locals
        
        # Find ALL newly defined functions (not just the first one)
        functions = []
        for name, obj in frame_locals_after.items():
            if self.frame_locals_before is None:
                continue
            if (name not in self.frame_locals_before and 
                callable(obj) and 
                inspect.isfunction(obj)):
                functions.append((name, obj))
                # Mark this function as handled in parent context
                if self.parent_obj is not None:
                    self.parent_obj._handled_functions.add(name)
        
        if not functions:
            print("No functions found in ParallelFor block")
            return False
        
        print(f"\nParallelizing {len(functions)} function(s) over {len(self.items)} items...")
        
        # Process each function
        for func_name, func in functions:
            func_processes = []
            
            # Create and start a process for each item
            for idx, item in enumerate(self.items):
                process = multiprocessing.Process(
                    target=self._run_with_queue,
                    args=(func, item, idx, func_name),
                    name=f"{func_name}_{idx}"
                )
                func_processes.append(process)
                self.processes.append(process)
                process.start()
                print(f"Started process {idx}: {func_name}({item}) (PID: {process.pid})")
            
            # Join all processes for this function
            for process in func_processes:
                process.join()
            
            # Collect results from queue for this function
            func_results = []
            temp_results = []
            while not self.result_queue.empty():
                result = self.result_queue.get()
                if result['function'] == func_name:
                    temp_results.append(result)
                else:
                    # Put it back for other functions
                    self.result_queue.put(result)
            
            # Sort by index to maintain order
            temp_results.sort(key=lambda x: x['index'])
            
            # Store results in list
            for result in temp_results:
                if 'error' in result:
                    func_results.append({'error': result['error'], 'item': result['item']})
                else:
                    func_results.append(result['data'])
            
            print(f"Completed all {len(func_processes)} parallel executions for '{func_name}'")
            
            # If we detected a parent context, store results there indexed by function name
            if self.parent_context is not None:
                self.parent_context[func_name] = func_results
                print(f"Stored results in parent context as '{func_name}'")
            else:
                # Store in our own results dict if no parent
                print("Stored results in current context")
                if not isinstance(self.results, dict):
                    self.results = {}
                self.results = cast(dict, self.results)
                self.results[func_name] = func_results
        
        return False
    
    def _run_with_queue(self, func, item, idx, func_name):
        """Wrapper to execute function with item and put result in queue."""
        try:
            result = func(item)
            self.result_queue.put({
                'function': func_name,
                'index': idx,
                'item': item,
                'data': result,
                'pid': multiprocessing.current_process().pid
            })
        except Exception as e:
            self.result_queue.put({
                'function': func_name,
                'index': idx,
                'item': item,
                'error': str(e),
                'pid': multiprocessing.current_process().pid
            })


# Example usage
if __name__ == "__main__":
    import time
    import os
    
    print("=" * 60)
    print("Demo 1: MultiprocessedFunctions")
    print("=" * 60)
    
    with MultiprocessedFunctions() as results:
        def task1():
            pid = os.getpid()
            print(f"Task 1 starting in process PID: {pid}")
            time.sleep(2)
            print(f"Task 1 completed in PID: {pid}")
            return f"Result from task1 (PID: {pid})"
        
        def task2():
            pid = os.getpid()
            print(f"Task 2 starting in process PID: {pid}")
            time.sleep(1)
            print(f"Task 2 completed in PID: {pid}")
            return f"Result from task2 (PID: {pid})"
        
        def task3():
            pid = os.getpid()
            print(f"Task 3 starting in process PID: {pid}")
            time.sleep(1.5)
            # Simulate some computation
            result = sum(range(1000000))
            print(f"Task 3 completed in PID: {pid}")
            return f"Task3 computed sum: {result} (PID: {pid})"
    
    print("\nAll processes completed!")
    print("\n--- Final Results Dictionary ---")
    for func_name, result in results.items():
        print(f"{func_name}: {result}")
    
    print("\n" + "=" * 60)
    print("Demo 2: ParallelFor with repetitions (n times)")
    print("=" * 60)
    
    with ParallelFor(5) as results:
        def compute_square(n):
            pid = os.getpid()
            print(f"  Computing {n}² in PID {pid}")
            time.sleep(0.5)
            return n * n
    
    print(f"\n--- Results: {results}")
    
    print("\n" + "=" * 60)
    print("Demo 3: ParallelFor with iterable (array)")
    print("=" * 60)
    
    items = ['apple', 'banana', 'cherry', 'date']
    with ParallelFor(items) as results:
        def process_fruit(fruit):
            pid = os.getpid()
            print(f"  Processing '{fruit}' in PID {pid}")
            time.sleep(0.5)
            return f"{fruit.upper()} (processed by {pid})"
    
    print(f"\n--- Results: {results}")
    
    print("\n" + "=" * 60)
    print("Demo 4: Multiple functions in single ParallelFor")
    print("=" * 60)
    
    numbers = [1, 2, 3, 4, 5]
    with ParallelFor(numbers) as results:
        def square(n):
            pid = os.getpid()
            print(f"  Computing {n}² in PID {pid}")
            time.sleep(0.3)
            return n * n
        
        def cube(n):
            pid = os.getpid()
            print(f"  Computing {n}³ in PID {pid}")
            time.sleep(0.3)
            return n * n * n
        
        def factorial(n):
            pid = os.getpid()
            print(f"  Computing {n}! in PID {pid}")
            time.sleep(0.3)
            result = 1
            for i in range(1, n + 1):
                result *= i
            return result
    
    results = cast(dict, results)
    print("\n--- Results ---")
    print(f"Squares: {results['square']}")
    print(f"Cubes: {results['cube']}")
    print(f"Factorials: {results['factorial']}")
    
    print("\n" + "=" * 60)
    print("Demo 5: Nested with array iteration")
    print("=" * 60)
    
    with MultiprocessedFunctions() as outer_results:
        data = [10, 20, 30]
        with ParallelFor(data) as inner_results:
            def double(x):
                pid = os.getpid()
                print(f"  Doubling {x} in PID {pid}")
                return x * 2
            
            def triple(x):
                pid = os.getpid()
                print(f"  Tripling {x} in PID {pid}")
                return x * 3
    
    print("\n--- Nested Results (accessible from outer context) ---")
    print(f"outer_results: {outer_results}")
    print(f"inner_results: {inner_results}")
    print(f"outer_results['double']: {outer_results['double']}")
    print(f"outer_results['triple']: {outer_results['triple']}")
    print(f"outer_results['double'][1]: {outer_results['double'][1]}")
    print(f"outer_results['triple'][0]: {outer_results['triple'][0]}")
    
    print("\n" + "=" * 60)
    print("Demo 6: ParallelFor and regular functions in same context")
    print("=" * 60)
    
    with MultiprocessedFunctions() as outer_results:
        # Regular function (runs once in its own process)
        def task_a():
            pid = os.getpid()
            print(f"Task A running in PID {pid}")
            time.sleep(1)
            return "Task A completed"
        
        # Parallel for loop (runs function over multiple items)
        values = [100, 200, 300, 400]
        with ParallelFor(values):
            def process_value(val):
                pid = os.getpid()
                print(f"  Processing {val} in PID {pid}")
                time.sleep(0.5)
                return val + 50
        
        # Another regular function
        def task_b():
            pid = os.getpid()
            print(f"Task B running in PID {pid}")
            time.sleep(1)
            return "Task B completed"
    
    print("\n--- Mixed Results ---")
    print(f"task_a: {outer_results['task_a']}")
    print(f"process_value: {outer_results['process_value']}")
    print(f"process_value[2]: {outer_results['process_value'][2]}")
    print(f"task_b: {outer_results['task_b']}")