import pickle
import math

# Function relies on the math module
def calculate_circle_area(radius):
    return math.pi * radius ** 2

# Serialize the function
serialized_function = pickle.dumps(calculate_circle_area)

# Save the pickle
with open("serdes.pkl", "wb") as f:
    f.write(serialized_function)
