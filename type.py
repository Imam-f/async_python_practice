from typing import Generic, Literal, get_type_hints

x: Literal[1] = 1
newval = type(1)
typex = type(x)
y = newval(1)

print(x)
print(type(x))
try:
    print(get_type_hints(x))
except Exception as e:
    print(e)
try:
    print(x.__annotations__)
except Exception as e:
    print(e)
print(newval)
print(type(newval))

print(typex)
print(type(typex))

print(y)
print(type(y))