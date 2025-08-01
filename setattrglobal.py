import sys

def module_setattr(name, value):
    print(f"Setting module attribute {name} = {value}")
    # Custom logic here
    globals()[name] = value

# Replace the module's __setattr__
sys.modules[__name__].__setattr__ = module_setattr

print(globals())
x = "Hello"
print(globals())
print(x)