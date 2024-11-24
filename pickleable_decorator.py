import functools
import multiprocessing
from typing import Callable
import dill

# Solution 1: Using wrapper class to make function picklable
class PicklableWrapper:
    def __init__(self, func):
        self.func = func
        functools.update_wrapper(self, func)
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

# Example decorator that could cause pickling issues
def my_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Calling {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

# Solution 2: Make decorator return picklable function
def picklable_decorator(func):
    wrapped = PicklableWrapper(
        functools.wraps(func)(
            lambda *args, **kwargs: func(*args, **kwargs)
        )
    )
    return wrapped

# Example usage with Solution 1
@my_decorator
def process_data(x):
    return x * 2

def run_multiprocessing_with_wrapper():
    # Wrap the decorated function
    picklable_func = PicklableWrapper(process_data)
    
    with multiprocessing.Pool() as pool:
        result = pool.map(picklable_func, range(5))
    return result

# Example usage with Solution 2
@picklable_decorator
def process_data_2(x):
    return x * 2

def run_multiprocessing_direct():
    with multiprocessing.Pool() as pool:
        result = pool.map(process_data_2, range(5))
    return result

# Solution 3: Using dill instead of pickle
def run_with_dill():
    # Create a pool that uses dill for serialization
    with multiprocessing.get_context('spawn').Pool(
        initializer=lambda: setattr(multiprocessing.reduction, 'pickle', dill)
    ) as pool:
        result = pool.map(process_data, range(5))
    return result

# Helper function to check if a function is picklable
def is_picklable(func: Callable) -> bool:
    try:
        import pickle
        pickle.dumps(func)
        return True
    except (pickle.PicklingError, TypeError):
        return False

if __name__ == "__main__":
    print("Solution 1:")
    result1 = run_multiprocessing_with_wrapper()
    print(result1)

    print("\nSolution 2:")
    # result2 = run_multiprocessing_direct()
    # print(result2)

    print("\nSolution 3:")
    result3 = run_with_dill()
    print(result3)