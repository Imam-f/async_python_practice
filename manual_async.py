import heapq
import time
from typing import Generator, Any, Callable

class Future:
    def __init__(self):
        self._result = None
        self._done = False
        self._callbacks = []

    def set_result(self, result):
        self._result = result
        self._done = True
        for callback in self._callbacks:
            callback(result)

    def add_done_callback(self, callback):
        if self._done:
            callback(self._result)
        else:
            self._callbacks.append(callback)

    def result(self):
        return self._result

    def done(self):
        return self._done

class EventLoop:
    def __init__(self):
        self._tasks = []
        self._current_time = 0
        self._scheduled_tasks = []

    def call_later(self, delay: float, callback: Callable):
        future = Future()
        heapq.heappush(self._scheduled_tasks, (self._current_time + delay, callback, future))
        return future

    def create_task(self, coroutine: Generator):
        task = Task(coroutine)
        self._tasks.append(task)
        return task

    def run(self):
        while self._tasks or self._scheduled_tasks:
            # Process scheduled tasks first
            while self._scheduled_tasks and \
                  self._scheduled_tasks[0][0] <= self._current_time:
                scheduled_time, callback, future = heapq.heappop(self._scheduled_tasks)
                future.set_result(callback())

            # Process active tasks
            completed_tasks = []
            for task in self._tasks[:]:  # Create a copy to iterate safely
                try:
                    # If task is waiting for a future, skip it
                    # if hasattr(task, '_waiting_future') and not task._waiting_future.done():
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
    def __init__(self, coroutine: Generator):
        self._coroutine = coroutine
        self._future = Future()
        self._last_yielded_value = None
        self._waiting_future = None

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
                loop.call_later(next_value, lambda: future.set_result(None))
                next_value = future
            else:
                # For other types, pass along
                self._last_yielded_value = next_value

            return next_value
        except StopIteration as e:
            # Coroutine completed
            self._future.set_result(e.value)
            return e.value

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
        print("Start of coroutine")
        
        yield 1.0  # Sleep for 1 second
        
        result1 = yield async_fetch("https://example.com")
        print(f"First fetch result: {result1}")
        
        yield 0.5  # Another short sleep
        
        result2 = yield async_fetch("https://another-example.com")
        print(f"Second fetch result: {result2}")
        
        return "Coroutine completed successfully"

    # Create and run the task
    task = loop.create_task(example_coroutine())
    loop.run()
    print("Task result:", task.result())

# Demonstrate the async system
if __name__ == "__main__":
    async_task_example()