import ast
import inspect
import textwrap
from types import FunctionType

def closure_to_pure_ast(closure_func):
    """Convert closure function to pure function using AST transformation"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    # Get closure info
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    # Get source code and fix indentation
    source = inspect.getsource(closure_func)
    source = textwrap.dedent(source)
    
    # Parse to AST
    tree = ast.parse(source)
    
    # Find the function definition
    func_def = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == closure_func.__name__:
            func_def = node
            break
    
    if not func_def:
        raise ValueError("Could not find function definition")
    
    # Add closure variables as keyword-only parameters with defaults
    for var, default_val in closure_dict.items():
        arg = ast.arg(
            arg=var, 
            annotation=None,
            lineno=func_def.lineno,
            col_offset=func_def.col_offset
        )
        func_def.args.kwonlyargs.append(arg)
        
        # Add default value
        if isinstance(default_val, str):
            default_node = ast.Constant(value=default_val)
        elif isinstance(default_val, (int, float)):
            default_node = ast.Constant(value=default_val)
        elif isinstance(default_val, bool):
            default_node = ast.Constant(value=default_val)
        else:
            # For complex objects, we'll handle them in the wrapper
            default_node = ast.Constant(value=None)
        
        func_def.args.kw_defaults.append(default_node)
    
    # Fix AST - add missing fields
    ast.fix_missing_locations(tree)
    
    # Compile the modified AST
    compiled = compile(tree, '<string>', 'exec')
    
    # Execute to get the new function
    namespace = closure_func.__globals__.copy()
    exec(compiled, namespace)
    
    pure_func = namespace[closure_func.__name__]
    
    return pure_func, closure_dict

def make_pure_function(closure_func):
    """Convert a closure function to a pure function that accepts closure vars as kwargs"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    # Get closure variables and values
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
        
        # Create new globals with closure variables
        new_globals = closure_func.__globals__.copy()
        new_globals.update(closure_values_to_use)
        
        # Create function without closure (use empty tuple for closure)
        pure_version = FunctionType(
            closure_func.__code__,
            new_globals,
            closure_func.__name__,
            closure_func.__defaults__,
            ()  # Empty tuple for no closure
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

def extract_pure_function_simple(closure_func):
    """Simple approach - extract pure function with dict/kwargs support"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    def pure_wrapper(*args, closure_vars_dict=None, **kwargs):
        # Determine closure values to use
        final_closure_values = closure_dict.copy()
        
        if closure_vars_dict:
            final_closure_values.update(closure_vars_dict)
        
        # Extract closure vars from kwargs
        for var in closure_vars:
            if var in kwargs:
                final_closure_values[var] = kwargs.pop(var)
        
        # Create execution environment
        env = closure_func.__globals__.copy()
        env.update(final_closure_values)
        
        # Recreate function (use empty tuple for closure)
        new_func = FunctionType(
            closure_func.__code__,
            env,
            closure_func.__name__,
            closure_func.__defaults__,
            # tuple([None for _ in closure_vars])  # Empty tuple for no closure
            None
        )
        
        return new_func(*args, **kwargs)
    
    return pure_wrapper, closure_dict

# Alternative approach using exec for more reliability
def make_pure_function_exec(closure_func):
    """Convert closure function to pure using exec approach"""
    
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

# Test all approaches
print("=== Testing Simple Approach (Fixed) ===")
formatter = create_formatter("START", "END")
pure_formatter, closure_info = extract_pure_function_simple(formatter)

print(f"Closure vars: {closure_info}")
print(f"Original: {formatter('hello')}")

# Test with kwargs
print(f"Pure (kwargs): {pure_formatter('hello', prefix='BEGIN', suffix='FINISH')}")

# Test with dict
closure_override = {"prefix": "DICT_START", "suffix": "DICT_END", "separator": " >>> "}
print(f"Pure (dict): {pure_formatter('hello', closure_vars_dict=closure_override)}")

print("\n=== Testing Full Approach (Fixed) ===")
calc = create_calculator("add")
pure_calc, calc_closure = make_pure_function(calc)

print(f"Closure vars: {calc_closure}")
print(f"Original: {calc(5, 3)}")
print(f"Pure (kwargs): {pure_calc(5, 3, operation='multiply', multiplier=5)}")
print(f"Pure (dict): {pure_calc(5, 3, closure_vars_dict={'operation': 'multiply', 'multiplier': 10})}")
print(f"Pure signature: {inspect.signature(pure_calc)}")

print("\n=== Testing Exec Approach ===")
formatter2 = create_formatter("EXEC_START", "EXEC_END")
pure_formatter2, closure_info2 = make_pure_function_exec(formatter2)

print(f"Closure vars: {closure_info2}")
print(f"Original: {formatter2('test')}")
print(f"Pure (kwargs): {pure_formatter2('test', prefix='NEW_START', suffix='NEW_END')}")
print(f"Pure (dict): {pure_formatter2('test', closure_vars_dict={'prefix': 'DICT_NEW', 'suffix': 'DICT_END'})}")

print("\n=== Testing Complex Function ===")
complex_func = create_complex_function()
pure_complex, complex_closure = make_pure_function_exec(complex_func)

print(f"Closure vars: {complex_closure}")
print(f"Original: {complex_func(10)}")
print(f"Pure (kwargs): {pure_complex(10, config={'debug': False, 'factor': 5}, prefix='New: ')}")

# Test with dict
new_config = {"debug": True, "factor": 7}
print(f"Pure (dict): {pure_complex(10, closure_vars_dict={'config': new_config, 'prefix': 'Modified: '})}")