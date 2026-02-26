"""
Bytecode Analyzer & Closure Factory
=====================================
Analyzes a function's bytecode and closure, then creates a factory
that accepts new closure values and returns an identical function
with the closure replaced.
"""

import dis
import inspect
import types
import ctypes
from typing import Any, Callable, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────
#  Core helpers
# ─────────────────────────────────────────────

def _make_cell(value: Any):
    """Create a new cell object containing *value*."""
    # The only portable way: compile a closure that captures a local.
    def _factory(v):
        def _inner():
            return v
        return _inner.__closure__[0]
    return _factory(value)


# ─────────────────────────────────────────────
#  Analyzer
# ─────────────────────────────────────────────

class ClosureInfo:
    """Holds the result of analyzing a function's closure."""

    def __init__(self, func: Callable):
        self.func = func
        self.freevars: Tuple[str, ...] = func.__code__.co_freevars
        self.cells: Dict[str, Any] = {}

        if func.__closure__:
            for name, cell in zip(self.freevars, func.__closure__):
                try:
                    self.cells[name] = cell.cell_contents
                except ValueError:
                    self.cells[name] = "<empty cell>"

    # ── pretty printing ──────────────────────

    def summary(self) -> str:
        lines = [
            f"Function : {self.func.__qualname__}",
            f"Module   : {self.func.__module__}",
            f"Freevars : {list(self.freevars)}",
            "",
            "Captured closure values:",
        ]
        if self.cells:
            for name, val in self.cells.items():
                lines.append(f"  {name!r:20s} = {val!r}")
        else:
            lines.append("  (none)")
        return "\n".join(lines)

    def bytecode_summary(self) -> str:
        """Return a human-readable bytecode listing."""
        import io
        buf = io.StringIO()
        dis.dis(self.func, file=buf)
        return buf.getvalue()

    def __repr__(self) -> str:
        return f"<ClosureInfo func={self.func.__qualname__!r} freevars={list(self.freevars)}>"


def analyze(func: Callable) -> ClosureInfo:
    """
    Analyze *func* and return a :class:`ClosureInfo` describing its
    bytecode and captured closure variables.
    """
    if not callable(func):
        raise TypeError(f"Expected a callable, got {type(func)!r}")
    return ClosureInfo(func)


# ─────────────────────────────────────────────
#  Factory builder
# ─────────────────────────────────────────────

def make_closure_factory(func: Callable) -> Callable:
    """
    Analyse *func* and return a **factory** function.

    The factory signature is::

        factory(**new_closure_values) -> Callable

    *new_closure_values* must cover every free variable declared in
    *func*.  Any variable not supplied keeps its original value.

    Returns a new function object that is byte-for-byte identical to
    *func* but uses the supplied closure values.
    """
    info = analyze(func)
    original_cells = dict(info.cells)          # name -> original value
    freevars       = info.freevars             # ordered tuple

    def factory(**overrides) -> Callable:
        # Validate keys
        unknown = set(overrides) - set(freevars)
        if unknown:
            raise ValueError(
                f"Unknown closure variable(s): {unknown}. "
                f"Valid names: {set(freevars)}"
            )

        # Build ordered cell tuple (new values override originals)
        new_cells = tuple(
            _make_cell(overrides.get(name, original_cells.get(name)))
            for name in freevars
        )

        # Reconstruct the function with the new closure
        new_func = types.FunctionType(
            func.__code__,
            func.__globals__,
            func.__name__,
            func.__defaults__,
            new_cells if new_cells else None,
        )
        new_func.__kwdefaults__ = func.__kwdefaults__
        new_func.__annotations__ = func.__annotations__
        new_func.__doc__         = func.__doc__
        return new_func

    factory.__name__ = f"{func.__name__}_closure_factory"
    factory.__doc__  = (
        f"Factory for '{func.__qualname__}'.\n"
        f"Keyword args (all optional): {list(freevars)}\n"
        f"Omitted vars keep their original values."
    )
    return factory


# ─────────────────────────────────────────────
#  Convenience wrapper
# ─────────────────────────────────────────────

def replace_closure(func: Callable, **new_values) -> Callable:
    """
    One-shot helper: return a copy of *func* with closure vars replaced
    by *new_values*.

    Example::

        def make_adder(n):
            def add(x):
                return x + n
            return add

        add5  = make_adder(5)
        add10 = replace_closure(add5, n=10)
        assert add10(3) == 13
    """
    return make_closure_factory(func)(**new_values)


# ─────────────────────────────────────────────
#  Demo
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # ── Example 1: simple adder ──────────────
    print("=" * 60)
    print("EXAMPLE 1 – simple closure replacement")
    print("=" * 60)

    def make_adder(n):
        def add(x):
            return x + n
        return add

    add5 = make_adder(5)
    info = analyze(add5)
    print(info.summary())
    print()
    print("Bytecode:")
    print(info.bytecode_summary())

    factory = make_closure_factory(add5)
    add10   = factory(n=10)
    add100  = factory(n=100)

    print(f"add5(3)   = {add5(3)}")    # 8
    print(f"add10(3)  = {add10(3)}")   # 13
    print(f"add100(3) = {add100(3)}") # 103
    print()

    # ── Example 2: multiple free vars ───────
    print("=" * 60)
    print("EXAMPLE 2 – multiple closure variables")
    print("=" * 60)

    def make_greeting(prefix, suffix):
        def greet(name):
            return f"{prefix}, {name}{suffix}"
        return greet

    hello_exclaim = make_greeting("Hello", "!")
    print(analyze(hello_exclaim).summary())
    print()

    hi_question = replace_closure(hello_exclaim, prefix="Hi", suffix="?")
    print(f"hello_exclaim('Alice') = {hello_exclaim('Alice')}")
    print(f"hi_question('Alice')   = {hi_question('Alice')}")
    print()

    # ── Example 3: partial override ─────────
    print("=" * 60)
    print("EXAMPLE 3 – partial override (only suffix changed)")
    print("=" * 60)

    hello_question = replace_closure(hello_exclaim, suffix="?")
    print(f"hello_exclaim('Bob')  = {hello_exclaim('Bob')}")
    print(f"hello_question('Bob') = {hello_question('Bob')}")
    print()

    # ── Example 4: multiplier ───────────────
    print("=" * 60)
    print("EXAMPLE 4 – multiplier via factory reuse")
    print("=" * 60)

    def make_multiplier(factor, offset):
        def multiply(x):
            return x * factor + offset
        return multiply

    mul3_plus1 = make_multiplier(3, 1)
    factory2   = make_closure_factory(mul3_plus1)

    mul5_plus0  = factory2(factor=5, offset=0)
    mul3_plus10 = factory2(offset=10)          # factor stays 3

    print(f"mul3_plus1(4)   = {mul3_plus1(4)}")    # 13
    print(f"mul5_plus0(4)   = {mul5_plus0(4)}")    # 20
    print(f"mul3_plus10(4)  = {mul3_plus10(4)}")   # 22