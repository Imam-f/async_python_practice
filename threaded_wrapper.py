from threading import Thread
from queue import Queue

def add_five(queue_in, queue_out):
    while True:
        x = queue_in.get()
        if x is None:
            break
        queue_out.put(x + 5)

queue_in = Queue()
queue_out = Queue()
thread_handle = Thread(target=add_five, args=(queue_in,queue_out))
thread_handle.start()

for i in range(10):
    queue_in.put(i)
    print(queue_out.get())

def wrapper(queue_in, queue_out):
    for i in range(5):
        queue_in.put(i)
        yield queue_out.get()

for i in wrapper(queue_in, queue_out):
    print(i)

def add_five_remote(x):
    queue_in.put(x)
    return queue_out.get()

print(add_five_remote(20))
queue_in.put(None)
