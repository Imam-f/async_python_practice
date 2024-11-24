class CallablePipe:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        # Make the instance callable
        return self.func(*args, **kwargs)

    def __ror__(self, other):
        # Allows 'other | self', where 'other' is a generator/iterator
        return self.func(other)

    def __or__(self, other):
        # Allows chaining: 'self | other'
        if callable(other):
            def composed(data):
                return other(self.func(data))
            return CallablePipe(composed)
        else:
            # If 'other' is not callable, apply the function directly
            return self.func(other)

def pipeable(func):
    # Decorator to make functions pipeable
    return CallablePipe(func)

# Example functions
@pipeable
def double(nums):
    for num in nums:
        yield num * 2

@pipeable
def increment(nums):
    for num in nums:
        yield num + 1

@pipeable
def to_string(nums):
    for num in nums:
        yield str(num)

if __name__ == "__main__":
    # Chaining the functions
    result = range(5) | double | increment | to_string

    # Consuming the result
    print(list(result))
