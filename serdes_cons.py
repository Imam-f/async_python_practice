import pickle

# Open the pickle
with open("serdes.pkl", "rb") as f:
    serialized_function = f.read()

# Deserialize in a clean environment without importing math
deserialized_function = pickle.loads(serialized_function)

# This will raise NameError because math is not imported
try:
    print(deserialized_function(5))
except NameError as e:
    print(f"Error: {e}")
