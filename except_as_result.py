from functools import wraps
import traceback

def return_exception(func):
    """
    A decorator that catches exceptions and returns them instead of raising.
    
    Returns:
    - The original function's return value if no exception occurs
    - An exception object if an exception is raised
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Create a custom exception object with traceback information
            error_info = {
                'exception': e,
                'type': type(e).__name__,
                'message': str(e),
                'traceback': traceback.format_exc()
            }
            return error_info
    
    return wrapper

# Demonstration of the decorator
def demonstrate_exception_decorator():
    # Example functions with different error scenarios
    @return_exception
    def divide_numbers(a, b):
        return a / b
    
    @return_exception
    def process_list(lst, index):
        return lst[index]
    
    @return_exception
    def complex_calculation(x):
        return 100 / (x - 5)
    
    print("Divide Numbers:")
    # Successful division
    print(divide_numbers(10, 2))
    # Division by zero
    result = divide_numbers(10, 0)
    print("Division by zero:", result['type'], result['message'])
    
    print("\nList Processing:")
    # Successful list access
    print(process_list([1, 2, 3], 1))
    # Index out of range
    result = process_list([1, 2, 3], 5)
    print("Index error:", result['type'], result['message'])
    
    print("\nComplex Calculation:")
    # Calculation that will raise an exception
    result = complex_calculation(5)
    print("Zero division:", result['type'], result['message'])
    
    # Optional: Printing full traceback for debugging
    print("\nFull Traceback for Last Error:")
    if isinstance(result, dict) and 'traceback' in result:
        print(result['traceback'])

# Run the demonstration
if __name__ == "__main__":
    demonstrate_exception_decorator()