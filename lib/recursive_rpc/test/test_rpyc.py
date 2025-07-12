import rpyc
import os
import inspect

c = rpyc.classic.connect("localhost", port=18812)

state = [0]

class Holder:
    def __init__(self, x):
        self.x = x

def caller(func, printer, *args, **kwargs):
    import os
    func()
    printer(args, kwargs, os.getpid())
    
def setter_func():
    state[0] = 5

# getter = lambda: state[0]
def getter(printer):
    import traceback
    try:
        printer("getter")
        x = state[0]
        printer("getter2")
        return x
    except Exception as e:
        printer(e, "remote error")
        printer(traceback.format_exc())
        raise e

func = c.teleport(caller)
func(setter_func, print, state[0], state)
print(c.modules.os.getpid())

try:
    func_with_closure = c.teleport(getter)
    print(func_with_closure(print))
except Exception as e:
    print("local error", e)

print(getter(print), os.getpid())

try:
    class_example = c.teleport(Holder)
except Exception as e:
    print("error", e)

print(inspect.getsource(Holder))
c.execute(inspect.getsource(Holder))
print(c.eval("Holder(5).x"))

from textwrap import dedent

def indented():
    class Holder_2:
        def __init__(self, x):
            self.x = x

    status = [4]
    def otherfunc(x):
        return status[5]
    
    c.execute(dedent(inspect.getsource(Holder_2)))
    print(c.eval("Holder_2(5).x"))
    try:
        otherfunc = c.teleport(otherfunc)
        print(otherfunc(5))
    except Exception as e:
        print("error", e)

    c.execute(dedent(inspect.getsource(otherfunc)))
    try:
        print(c.eval("otherfunc(7)"))
    except Exception as e:
        print("error", e)
        
indented()

def wrapper(func):
    def wrapper2(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper2

print("Below is the wrapper")
try:
    c.teleport(wrapper(print))
except Exception as e:
    print("error", e)
