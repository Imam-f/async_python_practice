import cloudpickle
import math

squared = lambda x: math.pow(x, 2)

CONSTANT = 42
def my_function(data: int) -> int:
    return data + CONSTANT

pickled_lambda = cloudpickle.dumps((squared, my_function))

# Save the pickle
with open("serdes_cloud.pkl", "wb") as f:
    f.write(pickled_lambda)
