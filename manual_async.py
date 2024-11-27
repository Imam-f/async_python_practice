import heapq
import time
from typing import Generator, Any, Callable

class CancelledError(Exception):
    """Exception raised when a task is cancelled."""
    pass

class Future:
    def __init__(self):
        self._result = None
        self._done = False
        self._cancelled = False
        self._callbacks = []

    def set_result(self, result):
        if not self._cancelled:
            self._result = result
            self._done = True
            for callback in self._callbacks:
                callback(result)

    def cancel(self):
        self._cancelled = True
        self._done = True
        for callback in self._callbacks:
            callback(None)

    def add_done_callback(self, callback):
        if self._done:
            callback(self._result)
        else:
            self._callbacks.append(callback)

    def result(self):
        if self._cancelled:
            raise CancelledError()
        return self._result

    def done(self):
        return self._done

    def cancelled(self):
        return self._cancelled

class EventLoop:
    def __init__(self):
        self._tasks = []
        self._current_time = 0
        self._scheduled_tasks = []

    def call_later(self, delay: float, callback: Callable):
        future = Future()
        heapq.heappush(self._scheduled_tasks, 
                       (self._current_time + delay, callback, future))
        return future

    def create_task(self, coroutine: Generator):
        task = Task(coroutine, self)
        self._tasks.append(task)
        return task

    def run(self):
        while self._tasks or self._scheduled_tasks:
            # Process scheduled tasks first
            # print(self._current_time)
            while self._scheduled_tasks and \
                  self._scheduled_tasks[0][0] <= self._current_time:
                scheduled_time, callback, future = heapq.heappop(self._scheduled_tasks)
                if not future.cancelled():
                    future.set_result(callback())
                # try:
                #     if not future.cancelled():
                #         future.set_result(callback())
                # except CancelledError:
                #     # Explicitly handle CancelledError
                #     print("Cancelled scheduled task")
                #     future.cancel()
                # except Exception as e:
                #     # Optionally log or handle other exceptions
                #     print(f"Exception in scheduled task: {e}")
                #     future.cancel()

            # Process active tasks
            completed_tasks = []
            for task in self._tasks[:]:
                try:
                    # Skip cancelled tasks
                    if task._future.cancelled():
                        print("Task was cancelled")
                        completed_tasks.append(task)
                        continue

                    # Skip tasks waiting on cancelled futures
                    if task._waiting_future is not None and task._waiting_future.cancelled():
                        task._future.cancel()
                        completed_tasks.append(task)
                        continue

                    # Skip tasks waiting on unresolved futures
                    if task._waiting_future is not None and not task._waiting_future.done():
                        continue

                    # Resume task and send/throw result from previous yield
                    next_value = task.step()
                    
                    # Handle futures and scheduled tasks
                    if isinstance(next_value, Future):
                        task._waiting_future = next_value
                    
                    if task.done():
                        completed_tasks.append(task)
                except StopIteration:
                    completed_tasks.append(task)
                # except CancelledError:
                #     task._future.cancel()
                #     completed_tasks.append(task)

            # Remove completed tasks
            for task in completed_tasks:
                self._tasks.remove(task)

            # Advance time if no tasks are ready
            if not self._tasks:
                if self._scheduled_tasks:
                    self._current_time = self._scheduled_tasks[0][0]
                else:
                    break

            # Minimal time advancement
            self._current_time += 0.01

class Task:
    def __init__(self, coroutine: Generator, loop: EventLoop):
        self._coroutine = coroutine
        self._future = Future()
        self._last_yielded_value = None
        self._waiting_future = None
        self.loop = loop

    def step(self) -> Any:
        try:
            # Send last result and get next value
            if self._last_yielded_value is None:
                next_value = next(self._coroutine)
            else:
                next_value = self._coroutine.send(self._last_yielded_value)

            # Handle different types of yielded values
            if isinstance(next_value, Future):
                # If a Future is yielded, wait for its result
                def on_future_done(result):
                    self._last_yielded_value = result
                next_value.add_done_callback(on_future_done)
            elif isinstance(next_value, (int, float)):
                # If a number is yielded, interpret as sleep
                future = Future()
                self.loop.call_later(next_value, lambda: future.set_result(None))
                next_value = future
            else:
                # For other types, pass along
                self._last_yielded_value = next_value

            return next_value
        except StopIteration as e:
            # Coroutine completed
            self._future.set_result(e.value)
            return e.value

    def cancel(self):
        self._future.cancel()
        """Cancel the task and raise CancelledError in the coroutine."""
        # raise CancelledError()

    def done(self):
        return self._future.done()

    def result(self):
        return self._future.result()

# Global event loop
loop = EventLoop()

def async_fetch(url):
    future = Future()
    
    def mock_network_call():
        print(f"Fetching {url}")
        return f"Data from {url}"
    
    # Simulate network delay
    loop.call_later(1.0, lambda: future.set_result(mock_network_call()))
    return future

def async_task_example():
    def example_coroutine():
        print("1 Start of coroutine")
        
        yield 1.0  # Sleep for 1 second
        
        result1 = yield async_fetch("https://example.com")
        print(f"1 First fetch result: {result1}")
        
        yield 0.5  # Another short sleep
        
        result2 = yield async_fetch("https://another-example.com")
        print(f"1 Second fetch result: {result2}")
        
        return "1 Coroutine completed successfully"

    def example_coroutine_two():
        print("2 Start of coroutine two")
        
        yield 0.75  # Sleep for 1 second
        
        result1 = yield async_fetch("https://example_one.com")
        print(f"2 First fetch result: {result1}")
        
        yield 0.5  # Another short sleep
        
        result2 = yield async_fetch("https://another-example-two.com")
        print(f"2 Second fetch result: {result2}")
        
        return "2 Coroutine two completed successfully"
    
    # Create and run the task
    task = loop.create_task(example_coroutine())
    task2 = loop.create_task(example_coroutine_two())
    loop.call_later(2.0, lambda: print("Time's up!"))
    
    # Simulate cancellation after 0.5 seconds
    loop.call_later(2.1, lambda: task.cancel())
    # loop.call_later(0.1, task.cancel)
    
    loop.run()
    
    try:
        print("Task result:", task.result())
    except CancelledError:
        print("Task was cancelled")

    try:
        print("Task result:", task2.result())
    except CancelledError:
        print("Task was cancelled")

# Demonstrate the async system
if __name__ == "__main__":
    async_task_example()