# Factorial function when you learn about recursion
def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)
    
# Lambda form
fact = lambda n, *args: n * fact(n - 1) if n > 0 else 1

# Lambda form but a combinator
fac = lambda n, f, *args: n * f(n - 1, f) if n > 0 else 1

# Lambda form but a combinator
fa  = lambda n: fac(n, fac)

# Generic recursion
z = lambda f: (lambda n, *args: f(n, f, *args))

function = z(fac)

# Inlined lambda function
f = (lambda f: (lambda n: f(n, f)))(lambda n, f: n * f(n - 1, f) if n > 0 else 1)

# Left side and right side equal
f_eval = (lambda n, f: n * f(n - 1, f) if n > 0 else 1)(5, lambda n, f: n * f(n - 1, f) if n > 0 else 1)

# Flattened f
f_new_eval = (lambda n: (lambda n, f: n * f(n - 1, f) if n > 0 else 1)(n, (lambda n, f: n * f(n - 1, f) if n > 0 else 1)))

# Inlined eval
n = "lambda n, f: n * f(n - 1, f) if n > 0 else 1"
f_use_eval = (eval(n))(5, eval(n))

# Z combinator but with eval
eval_combinator = lambda n, e: (lambda v: e(n)(v, e(n)))

# Function factory
eval_factorial = eval_combinator(n, eval)

print("Normal recursive function :", factorial(5))
print("Lambda recursive pattern", fact(5))
print ("Fac defined", fa(5))
print("Z combinator pattern", function(5))
print("Look ma no hands", f(5))
print("Actual inlined eval", f_eval)
print("Flattened eval", f_new_eval(5))
print("Using python eval", f_use_eval)
print("Combinator eval", eval_factorial(5))

Y = (lambda f: (lambda x: f(x(x)))(lambda x: f(x(x))))
F = lambda f: lambda x: 1 if x == 0 else x * f(x - 1)
G = lambda g: lambda a: 1 if a == 0 else a * g(a - 1)
Z = (lambda f:
        (lambda x: f(lambda v: x(x)(v)))
        (lambda x: f(lambda v: x(x)(v)))
    )
Fac = Z(F)
Fac_inline = Z(lambda f: lambda x: 1 if x == 0 else x * f(x - 1))
Z_inline = \
        (lambda x: (lambda g: lambda a: 1 if a == 0 else a * g(a - 1))(lambda v: x(x)(v))) \
        (lambda x: (lambda g: lambda a: 1 if a == 0 else a * g(a - 1))(lambda v: x(x)(v)))

Z_string = "(lambda x: (lambda g: lambda a: 1 if a == 0 else a * g(a - 1))(lambda v: x(x)(v)))"
Z_eval = eval(Z_string)(eval(Z_string))

print("Z combinator by theorem", Z(F)(5))
print("Z combinator by theorem", Fac(6))
print("Z combinator by theorem", Fac_inline(7))
print("Z combinator by theorem", Z_inline(8))
print("Z combinator by theorem", Z_eval(9))
