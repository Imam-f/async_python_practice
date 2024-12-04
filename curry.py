import inspect

def curry(func):
    """
    A decorator that transforms a function into a curried function.
    
    Supports partial application with both positional and keyword arguments.
    """
    def curried(*args, **kwargs):
        # Get the function's signature
        sig = inspect.signature(func)
        bound_arguments = sig.bind_partial(*args, **kwargs)
        bound_arguments.apply_defaults()
        
        # Check if we have enough arguments to call the function
        remaining_params = [
            param for name, param in sig.parameters.items() 
            if name not in bound_arguments.arguments
        ]

        # Check if any of the kwargs is none and the default value is also not None
        no_any_none = True
        for key, value in sig.parameters.items():
            if bound_arguments.arguments.get(key, None) is None and value.default is not None:
                no_any_none &= False
            else:
                no_any_none &= True
        
        # If no remaining parameters, call the function
        if not remaining_params and no_any_none:
            return func(*args, **kwargs)
        
        # Otherwise, return a new function that can accept more arguments
        def inner(*more_args, **more_kwargs):
            # Merge previous and new arguments
            combined_args = args + more_args
            combined_kwargs = {**kwargs, **more_kwargs}
            
            # Recursively apply currying
            return curried(*combined_args, **combined_kwargs)
        
        return inner

    return curried

# Example usage with various argument types
@curry
def complex_function(a, b, c=10, *, d=20, e=30):
    return f"a={a}, b={b}, c={c}, d={d}, e={e}"

@curry
def user_profile(username, email, age=None, *, active=True, role='user'):
    return {
        'username': username,
        'email': email,
        'age': age,
        'active': active,
        'role': role
    }

def demonstrate_currying():
    # Demonstrating complex function with mixed arguments
    print("Complex Function Examples:")
    
    # Full argument application
    print("1. Full argument application:")
    print(complex_function(1, 2, 3, d=4, e=5))
    
    # Partial positional arguments
    print("\n2. Partial positional arguments:")
    partial_func1 = complex_function(1)
    partial_func2 = partial_func1(2)
    print(partial_func2)  # Using default values
    
    # Partial keyword arguments
    print("\n3. Partial keyword arguments:")
    partial_func3 = complex_function(a=1)
    partial_func4 = partial_func3(c=3, d=4)
    # partial_func4 = partial_func3(c=5, d=6)
    print(partial_func4(b=2))
    
    # Partial keyword arguments
    print("\n3a. Partial keyword arguments:")
    partial_func3 = complex_function(1)
    partial_func4 = partial_func3(c=3, d=4)
    print(partial_func4(2))
    
    # Partial keyword arguments
    print("\n3b. Partial keyword arguments:")
    partial_func3 = complex_function(1)
    partial_func4 = partial_func3(c=None, d=None)
    partial_func5 = partial_func4(2)
    print(partial_func5(c=6, d=7))
    
    # User profile function examples
    print("\nUser Profile Examples:")
    
    # Create a base user creator
    create_user = user_profile(role='admin', active=None)
    
    # Full application
    create_alex = create_user(username='alex', email='alex@example.com', active=True)
    print("Alex's profile:", create_alex)
    
    # Partially apply username
    _create_alice = create_user('alice', 'alice@example.com')
    def create_alice(active=True): return _create_alice(active=active)
    print("Alice's profile:", create_alice())
    
    # Create another user with different details
    create_bob = create_user('bob', 'bob@example.com')
    print("Bob's profile:", create_bob(age=30, active=False))

# Run the demonstration
if __name__ == "__main__":
    demonstrate_currying()