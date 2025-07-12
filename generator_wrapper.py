class GeneratorWrapper:
    def __init__(self, generator_func, initial_state=None):
        """
        Initialize the generator wrapper.
        
        Args:
            generator_func: A generator function that yields outputs
            initial_state: Initial state for the generator (optional)
        """
        self.generator_func = generator_func
        self.state = initial_state or {}
        self.generator = None
        self._initialize_generator()
    
    def _initialize_generator(self):
        """Initialize or restart the generator."""
        self.generator = self.generator_func(self.state)
        # Prime the generator
        try:
            next(self.generator)
        except StopIteration:
            pass
    
    def send_input(self, input_data):
        """
        Send input to the generator and get output.
        
        Args:
            input_data: Data to send to the generator
            
        Returns:
            The yielded output from the generator
        """
        try:
            if self.generator is None:
                self._initialize_generator()
            
            # Send input and get output
            output = self.generator.send(input_data)
            return output
        except StopIteration as e:
            # Generator finished, return final value if any
            return getattr(e, 'value', None)
    
    def get_state(self):
        """Get current state."""
        return self.state.copy()
    
    def update_state(self, **kwargs):
        """Update the state directly."""
        self.state.update(kwargs)
    
    def reset(self, new_state=None):
        """Reset the generator with optional new state."""
        if new_state is not None:
            self.state = new_state
        self._initialize_generator()
    
    def close(self):
        """Close the generator."""
        if self.generator:
            self.generator.close()

# Example usage with a simple counter generator
def counter_generator(state):
    """Example generator that maintains a counter."""
    count = state.get('count', 0)
    
    output = {'count': count, 'message': f'Current count: {count}'}
    
    while True:
        # Receive input
        input_data = yield output
        
        # Update state based on input
        if input_data is not None:
            if isinstance(input_data, dict):
                increment = input_data.get('increment', 1)
                reset = input_data.get('reset', False)
                
                if reset:
                    count = 0
                else:
                    count += increment
            else:
                count += 1
        
        # Update state
        state['count'] = count
        
        # Yield output
        output = {'count': count, 'message': f'Current count: {count}'}

# Example usage
if __name__ == "__main__":
    # Create wrapper
    wrapper = GeneratorWrapper(counter_generator, {'count': 0})
    wrapper.send_input(None)
    
    # Send inputs and get outputs
    result1 = wrapper.send_input({'increment': 5})
    print(result1)  # {'count': 5, 'message': 'Current count: 5'}
    
    result2 = wrapper.send_input({'increment': 3})
    print(result2)  # {'count': 8, 'message': 'Current count: 8'}
    
    result3 = wrapper.send_input({'reset': True})
    print(result3)  # {'count': 0, 'message': 'Current count: 0'}
    
    # Check state
    print(wrapper.get_state())  # {'count': 0}
