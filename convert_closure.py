import ast
import inspect
from types import FunctionType

def closure_to_pure_ast(closure_func):
    """Convert closure function to pure function using AST transformation"""
    
    if not closure_func.__closure__:
        return closure_func, {}
    
    # Get closure info
    closure_vars = closure_func.__code__.co_freevars
    closure_values = [cell.cell_contents for cell in closure_func.__closure__]
    closure_dict = dict(zip(closure_vars, closure_values))
    
    # Get source code
    source = inspect.getsource(closure_func)
    
    # Parse to AST
    from textwrap import dedent
    tree = ast.parse(dedent(source))
    
    # Find the function definition
    func_def = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == closure_func.__name__:
            func_def = node
            break
    
    if not func_def:
        raise ValueError("Could not find function definition")
    
    # Add closure variables as parameters with proper AST fields
    for var in closure_vars:
        arg = ast.arg(
            arg=var, 
            annotation=None,
            lineno=func_def.lineno,
            col_offset=func_def.col_offset
        )
        func_def.args.args.append(arg)
    
    # Fix AST - add missing fields
    ast.fix_missing_locations(tree)
    
    # Compile the modified AST
    compiled = compile(tree, '<string>', 'exec')
    
    # Execute to get the new function
    namespace = closure_func.__globals__.copy()
    exec(compiled, namespace)
    
    pure_func = namespace[closure_func.__name__]
    
    return pure_func, closure_dict

# Example
def create_formatter(prefix, suffix):
    separator = " | "
    
    def format_string(text):
        return f"{prefix}{separator}{text}{separator}{suffix}"
    
    return format_string

# Convert to pure
formatter = create_formatter("START", "END")
pure_formatter, closure_vars = closure_to_pure_ast(formatter)

print(f"Closure vars: {closure_vars}")
print(f"Original: {formatter('hello')}")
print(f"Pure: {pure_formatter('hello', 'START', ' | ', 'END')}")