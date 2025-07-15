import ast
import inspect
import textwrap
from types import FunctionType, CodeType

def make_pure_function_exec(closure_func):
    """Convert closure function to pure using exec approach - most reliable"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    # Get source and dedent
    source = inspect.getsource(closure_func)
    source = textwrap.dedent(source)
    
    # Create wrapper template
    wrapper_template = f"""
def pure_{closure_func.__name__}(*args, closure_vars_dict=None, **kwargs):
    # Set default closure values
    {'; '.join(f'{var} = {repr(val)}' for var, val in closure_dict.items())}
    
    # Update from dictionary
    if closure_vars_dict:
        {'; '.join(f"if '{var}' in closure_vars_dict: {var} = closure_vars_dict['{var}']" for var in closure_vars)}
    
    # Update from kwargs
    {'; '.join(f"if '{var}' in kwargs: {var} = kwargs.pop('{var}')" for var in closure_vars)}
    
    # Original function definition
{textwrap.indent(source, '    ')}
    
    # Call the original function
    return {closure_func.__name__}(*args, **kwargs)
"""
    
    # Execute the wrapper
    namespace = closure_func.__globals__.copy()
    exec(wrapper_template, namespace)
    
    pure_func = namespace[f'pure_{closure_func.__name__}']
    
    return pure_func, closure_dict

def make_pure_function_cells(closure_func):
    """Convert closure function to pure function by properly handling cells"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    # Get original function signature
    orig_sig = inspect.signature(closure_func)
    
    def pure_function(*args, closure_vars_dict=None, **kwargs):
        # Handle closure variables from dict or kwargs
        closure_values_to_use = closure_dict.copy()  # Start with defaults
        
        if closure_vars_dict:
            closure_values_to_use.update(closure_vars_dict)
        
        # Override with any closure vars passed as kwargs
        for var in closure_vars:
            if var in kwargs:
                closure_values_to_use[var] = kwargs.pop(var)
        
        # Create cells for closure variables
        from types import CellType
        cells = []
        for var in closure_vars:
            cell = CellType(closure_values_to_use[var])
            cells.append(cell)
        
        # Create function with proper closure
        pure_version = FunctionType(
            closure_func.__code__,
            closure_func.__globals__,
            closure_func.__name__,
            closure_func.__defaults__,
            tuple(cells)  # Proper closure cells
        )
        
        return pure_version(*args, **kwargs)
    
    # Create signature for documentation
    new_params = []
    
    # Add original parameters
    for param in orig_sig.parameters.values():
        new_params.append(param)
    
    # Add closure_vars_dict parameter
    closure_dict_param = inspect.Parameter(
        'closure_vars_dict', 
        inspect.Parameter.KEYWORD_ONLY,
        default=None
    )
    new_params.append(closure_dict_param)
    
    # Add individual closure variables as keyword-only with defaults
    for var, default_val in closure_dict.items():
        new_param = inspect.Parameter(
            var, 
            inspect.Parameter.KEYWORD_ONLY,
            default=default_val
        )
        new_params.append(new_param)
    
    pure_function.__signature__ = inspect.Signature(new_params)
    pure_function.__name__ = f"pure_{closure_func.__name__}"
    
    return pure_function, closure_dict

def make_pure_function_globals(closure_func):
    """Convert closure function to pure by putting closure vars in globals"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    # Get the function source and modify it to not use closure
    source = inspect.getsource(closure_func)
    source = textwrap.dedent(source)
    
    # Parse and modify AST to remove closure dependencies
    tree = ast.parse(source)
    
    # Create new function that takes closure vars as parameters
    func_def = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == closure_func.__name__:
            func_def = node
            break
    
    if func_def:
        # Add closure variables as parameters
        for var in closure_vars:
            arg = ast.arg(arg=var, annotation=None)
            func_def.args.args.append(arg)
    
    # Compile the modified function
    ast.fix_missing_locations(tree)
    compiled = compile(tree, '<string>', 'exec')
    
    def pure_function(*args, closure_vars_dict=None, **kwargs):
        # Handle closure variables
        closure_values_to_use = closure_dict.copy()
        
        if closure_vars_dict:
            closure_values_to_use.update(closure_vars_dict)
        
        for var in closure_vars:
            if var in kwargs:
                closure_values_to_use[var] = kwargs.pop(var)
        
        # Execute the modified function
        namespace = closure_func.__globals__.copy()
        exec(compiled, namespace)
        
        modified_func = namespace[closure_func.__name__]
        
        # Call with closure vars as additional arguments
        closure_args = [closure_values_to_use[var] for var in closure_vars]
        return modified_func(*args, *closure_args, **kwargs)
    
    return pure_function, closure_dict

# Simplest and most reliable approach
def make_pure_simple(closure_func):
    """Simple string manipulation approach"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    # Get function source
    source = inspect.getsource(closure_func)
    source = textwrap.dedent(source)
    
    # Create pure function template
    template = f"""
def pure_{closure_func.__name__}(*args, closure_vars_dict=None, **kwargs):
    # Default closure values
    closure_defaults = {repr(closure_dict)}
    
    # Update closure values
    closure_values = closure_defaults.copy()
    if closure_vars_dict:
        closure_values.update(closure_vars_dict)
    
    # Update from kwargs
    for var in {repr(closure_vars)}:
        if var in kwargs:
            closure_values[var] = kwargs.pop(var)
    
    # Set closure variables in local scope
    {'; '.join(f"{var} = closure_values['{var}']" for var in closure_vars)}
    
    # Original function (indented)
{textwrap.indent(source, '    ')}
    
    # Call the function
    return {closure_func.__name__}(*args, **kwargs)
"""
    
    # Execute template
    namespace = closure_func.__globals__.copy()
    exec(template, namespace)
    
    return namespace[f'pure_{closure_func.__name__}'], closure_dict

# Example usage
def create_formatter(prefix, suffix):
    separator = " | "
    
    def format_string(text):
        return f"{prefix}{separator}{text}{separator}{suffix}"
    
    return format_string

def create_calculator(operation):
    multiplier = 2
    base = 10
    
    def calculate(x, y):
        if operation == "add":
            return (x + y) * multiplier + base
        elif operation == "multiply":
            return (x * y) * multiplier + base
        return x + y
    
    return calculate

def create_complex_function():
    """Function with more complex closure"""
    config = {"debug": True, "factor": 3}
    prefix = "Result: "
    
    def process(data, transform=None):
        result = data * config["factor"]
        if transform:
            result = transform(result)
        output = f"{prefix}{result}"
        if config["debug"]:
            output += " [DEBUG]"
        return output
    
    return process

# Test the working approaches
print("=== Testing Simple String Approach ===")
formatter = create_formatter("START", "END")
pure_formatter, closure_info = make_pure_simple(formatter)

print(f"Closure vars: {closure_info}")
print(f"Original: {formatter('hello')}")
print(f"Pure (kwargs): {pure_formatter('hello', prefix='BEGIN', suffix='FINISH')}")

closure_override = {"prefix": "DICT_START", "suffix": "DICT_END", "separator": " >>> "}
print(f"Pure (dict): {pure_formatter('hello', closure_vars_dict=closure_override)}")

print("\n=== Testing Cell Approach ===")
calc = create_calculator("add")
pure_calc, calc_closure = make_pure_function_cells(calc)

print(f"Closure vars: {calc_closure}")
print(f"Original: {calc(5, 3)}")
print(f"Pure (kwargs): {pure_calc(5, 3, operation='multiply', multiplier=5)}")
print(f"Pure (dict): {pure_calc(5, 3, closure_vars_dict={'operation': 'multiply', 'multiplier': 10})}")

print("\n=== Testing Exec Approach ===")
formatter2 = create_formatter("EXEC_START", "EXEC_END")
pure_formatter2, closure_info2 = make_pure_function_exec(formatter2)

print(f"Closure vars: {closure_info2}")
print(f"Original: {formatter2('test')}")
print(f"Pure (kwargs): {pure_formatter2('test', prefix='NEW_START', suffix='NEW_END')}")
print(f"Pure (dict): {pure_formatter2('test', closure_vars_dict={'prefix': 'DICT_NEW', 'suffix': 'DICT_END'})}")

print("\n=== Testing Complex Function ===")
complex_func = create_complex_function()
pure_complex, complex_closure = make_pure_simple(complex_func)

print(f"Closure vars: {complex_closure}")
print(f"Original: {complex_func(10)}")
print(f"Pure (kwargs): {pure_complex(10, config={'debug': False, 'factor': 5}, prefix='New: ')}")

new_config = {"debug": True, "factor": 7}
print(f"Pure (dict): {pure_complex(10, closure_vars_dict={'config': new_config, 'prefix': 'Modified: '})}")