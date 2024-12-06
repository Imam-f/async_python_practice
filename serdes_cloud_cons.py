import pickle

# Open the pickle
with open("serdes_cloud.pkl", "rb") as f:
    serialized_function = f.read()

# Deserialize in a clean environment without importing math
f_lambda, f_closure = pickle.loads(serialized_function)

try:
    print(f_lambda(5), f_closure(5))
except NameError as e:
    print(f"Error: {e}")
