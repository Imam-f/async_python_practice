from functools import wraps
from typing import TypeVar, Callable, Any, Union
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
import inspect
from dataclasses import dataclass
from enum import Enum, auto
import time

T = TypeVar('T')
R = TypeVar('R')

class ExecutionMode(Enum):
    SEQUENTIAL = auto()
    THREAD = auto()
    PROCESS = auto()

@dataclass
class PipeableResult:
    """Wrapper class to hold the result and execution mode of a pipeable operation"""
    value: Any
    mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    _future: Future = None
    
    def __or__(self, other):
        if not callable(other):
            return NotImplemented
            
        if self.mode == ExecutionMode.SEQUENTIAL:
            return other(self.value)
            
        # If we have a future, we need to wait for it before proceeding
        if self._future:
            self.value = self._future.result()
            self._future = None
            
        # Create new executor based on mode
        if self.mode == ExecutionMode.THREAD:
            with ThreadPoolExecutor() as executor:
                return PipeableResult(
                    value=None,
                    mode=self.mode,
                    _future=executor.submit(other, self.value)
                )
        elif self.mode == ExecutionMode.PROCESS:
            with ProcessPoolExecutor() as executor:
                return PipeableResult(
                    value=None,
                    mode=self.mode,
                    _future=executor.submit(other, self.value)
                )
      
    def __ror__(self, other):
        if isinstance(self.value, Callable):
            return self.value(other)
        return NotImplemented
                      
    def result(self):
        """Get the final result, resolving any pending futures"""
        if self._future:
            return self._future.result()
        return self.value

def pipeable(func: Union[Callable, ExecutionMode] = None, mode: ExecutionMode = ExecutionMode.SEQUENTIAL) -> Callable[..., Any]:
    """
    Decorator that makes a function pipeable using the | operator, with optional parallel execution.
    
    Args:
        func: The function to decorate, or ExecutionMode if used with parameters
        mode: ExecutionMode.SEQUENTIAL (default), ExecutionMode.THREAD, or ExecutionMode.PROCESS
    
    Example:
        @pipeable(mode=ExecutionMode.THREAD)
        def slow_double(x):
            time.sleep(1)  # Simulate slow operation
            return x * 2
            
        result = (5 | slow_double | slow_double).result()  # Executes in parallel
    """
    def decorator(func: Callable[..., R]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not args and not kwargs:
                # Return the wrapper itself if no arguments
                return wrapper
            
            if len(args) == 1 and isinstance(args[0], (PipeableResult, int, float, str, bool)):
                # If we get a single value (either PipeableResult or basic type),
                # it's being used in a pipe
                pipe_value = args[0]
                if isinstance(pipe_value, PipeableResult):
                    if pipe_value._future:
                        pipe_value = pipe_value.result()
                    else:
                        pipe_value = pipe_value.value
                        
                # Create a partial function with the stored arguments
                if hasattr(wrapper, '_partial_args'):
                    result = func(pipe_value, *wrapper._partial_args, **wrapper._partial_kwargs)
                else:
                    result = func(pipe_value, *args, **kwargs)
            else:
                # Store arguments for later use
                wrapper._partial_args = args
                wrapper._partial_kwargs = kwargs
                return wrapper
            return PipeableResult(result, mode=mode)
        
        # Add __or__ to the function itself for cases where it's the right operand
        def __or__(self, other):
            return wrapper(other)
            
        def __ror__(self, other):
            """Handle left pipe: value | func"""
            if hasattr(wrapper, '_partial_args'):
                return wrapper(other)
            return wrapper(other)
        
        wrapper.__ror__ = __ror__
        wrapper.__or__ = __or__
        return wrapper
    
    # Handle both @pipeable and @pipeable(mode=...) syntax
    if func is None:
        return decorator
    if isinstance(func, ExecutionMode):
        mode = func
        return decorator
    return decorator(func)

# Convenience decorators for different execution modes
def thread_pipeable(func):
    return pipeable(func, mode=ExecutionMode.THREAD)

def process_pipeable(func):
    return pipeable(func, mode=ExecutionMode.PROCESS)


# Sequential execution (default)
@pipeable
def add(x, y):
    return x + y

# Parallel execution using threads
@thread_pipeable
def slow_double(x):
    time.sleep(1)  # Simulate slow operation
    return x * 2

# Parallel execution using processes
@process_pipeable
def slow_square(x):
    time.sleep(1)  # Simulate slow operation
    return x ** 2

# You can also use the mode parameter
@pipeable(mode=ExecutionMode.THREAD)
def slow_add_one(x):
    time.sleep(1)
    return x + 1

if __name__ == '__main__':
    # Sequential execution
    result1 = 5 | add(3) | slow_double
    print(result1.value)  # 16

    # Parallel execution (operations run concurrently)
    result2 = (5 | slow_double | slow_double | slow_square).result()
    print(result2)  # Completes in ~2 seconds instead of 3

    # Mix and match execution modes
    result3 = (10 | slow_double | add(5) | slow_square).result()
    print(result3)