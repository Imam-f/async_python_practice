class ModuleWrapper:
    def __init__(self, module):
        self._module = module
    
    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
        else:
            print(f"Setting {name} = {value}")
            setattr(self._module, name, value)
    
    def __getattr__(self, name):
        return getattr(self._module, name)

# Replace module in sys.modules
import sys
sys.modules[__name__] = ModuleWrapper(sys.modules[__name__])

print(globals())
x = "Hello"
print(globals())
print(x)