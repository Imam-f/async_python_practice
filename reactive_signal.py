import enum

class SignalMode(enum.Enum):
    LAZY = "lazy"
    EAGER = "eager"


class Signal:
    _current_computation = None

    def __init__(self, initial_value=None, mode=SignalMode.LAZY):
        self._value = initial_value
        self._dirty = False
        self._dependents = set()
        self._compute_func = None
        self._mode = mode

        if self._mode == SignalMode.EAGER and self._compute_func:
            self._update()

    def set(self, value):
        if self._value != value:
            self._value = value
            self._mark_dependents_dirty()

    def get(self):
        if Signal._current_computation is not None:
            self._dependents.add(Signal._current_computation)

        if self._dirty and (self._mode == SignalMode.LAZY or self._compute_func is None):
            if self._compute_func:
                self._update()
            else:
                self._dirty = False
        return self._value

    def _update(self):
        old_computation = Signal._current_computation
        Signal._current_computation = self

        try:
            if self._compute_func:
                self._dependents = set()

            new_value = self._compute_func()

            if self._value != new_value:
                self._value = new_value
            
            self._dirty = False

        finally:
            Signal._current_computation = old_computation

    def _mark_dependents_dirty(self):
        for dependent in list(self._dependents):
            if not dependent._dirty:
                dependent._dirty = True
                dependent._mark_dependents_dirty()

                if dependent._mode == SignalMode.EAGER and dependent._compute_func:
                    dependent._update()

    def connect(self, dependent_signal):
        if isinstance(dependent_signal, Signal):
            self._dependents.add(dependent_signal)
            if self._dirty and dependent_signal._mode == SignalMode.EAGER and dependent_signal._compute_func:
                dependent_signal._update()
        else:
            raise TypeError("Can only connect to another Signal instance.")

    def disconnect(self, dependent_signal):
        if isinstance(dependent_signal, Signal):
            self._dependents.discard(dependent_signal)

    @staticmethod
    def computed(func, mode=SignalMode.LAZY):
        signal = Signal(mode=mode)
        signal._compute_func = func
        signal._dirty = True
        if mode == SignalMode.EAGER:
            signal._update()
        return signal

    @staticmethod
    def effect(func, mode=SignalMode.LAZY):
        signal = Signal(mode=mode)
        signal._compute_func = func
        signal._dirty = True
        signal._update()
        return signal


class ReactiveBase:
    def __init__(self, mode=SignalMode.LAZY):
        self._signal = Signal(self, mode=mode) 

    def get_signal(self):
        return self._signal

    def _notify_change(self):
        self._signal.set(self)


class ReactiveList(ReactiveBase):
    def __init__(self, initial_list=None, mode=SignalMode.LAZY):
        super().__init__(mode=mode)
        self._list = list(initial_list) if initial_list is not None else []

    def __getitem__(self, index):
        self._signal.get()
        return self._list[index]

    def __setitem__(self, index, value):
        if 0 <= index < len(self._list) and self._list[index] != value:
            self._list[index] = value
            self._notify_change()
        elif not (0 <= index < len(self._list)):
            self._list[index] = value
            self._notify_change()

    def __delitem__(self, index):
        del self._list[index]
        self._notify_change()

    def __len__(self):
        self._signal.get()
        return len(self._list)

    def append(self, item):
        self._list.append(item)
        self._notify_change()

    def extend(self, iterable):
        self._list.extend(iterable)
        self._notify_change()

    def insert(self, index, item):
        self._list.insert(index, item)
        self._notify_change()

    def remove(self, value):
        self._list.remove(value)
        self._notify_change()

    def pop(self, index=-1):
        item = self._list.pop(index)
        self._notify_change()
        return item

    def clear(self):
        if self._list:
            self._list.clear()
            self._notify_change()

    def __iter__(self):
        self._signal.get()
        return iter(self._list)

    def __repr__(self):
        self._signal.get()
        return f"ReactiveList({repr(self._list)})"

    def get_list(self):
        self._signal.get()
        return self._list


class ReactiveDict(ReactiveBase):
    def __init__(self, initial_dict=None, mode=SignalMode.LAZY):
        super().__init__(mode=mode)
        self._dict = dict(initial_dict) if initial_dict is not None else {}

    def __getitem__(self, key):
        self._signal.get()
        return self._dict[key]

    def __setitem__(self, key, value):
        if key not in self._dict or self._dict[key] != value:
            self._dict[key] = value
            self._notify_change()

    def __delitem__(self, key):
        if key in self._dict:
            del self._dict[key]
            self._notify_change()

    def __len__(self):
        self._signal.get()
        return len(self._dict)

    def __iter__(self):
        self._signal.get()
        return iter(self._dict)

    def __contains__(self, key):
        self._signal.get()
        return key in self._dict

    def get(self, key, default=None):
        self._signal.get()
        return self._dict.get(key, default)

    def keys(self):
        self._signal.get()
        return self._dict.keys()

    def values(self):
        self._signal.get()
        return self._dict.values()

    def items(self):
        self._signal.get()
        return self._dict.items()

    def update(self, other=None, **kwargs):
        changed = False
        if other:
            if hasattr(other, 'items'):
                for k, v in other.items():
                    if k not in self._dict or self._dict[k] != v:
                        self._dict[k] = v
                        changed = True
            else:
                for k, v in other:
                    if k not in self._dict or self._dict[k] != v:
                        self._dict[k] = v
                        changed = True
        if kwargs:
            for k, v in kwargs.items():
                if k not in self._dict or self._dict[k] != v:
                    self._dict[k] = v
                    changed = True

        if changed:
            self._notify_change()

    def clear(self):
        if self._dict:
            self._dict.clear()
            self._notify_change()

    def __repr__(self):
        self._signal.get()
        return f"ReactiveDict({repr(self._dict)})"

    def get_dict(self):
        self._signal.get()
        return self._dict


class ReactiveStruct(ReactiveBase):
    def __init__(self, mode=SignalMode.LAZY, **kwargs):
        super().__init__(mode=mode)
        self._attributes = {}
        for key, value in kwargs.items():
            self._set_attribute(key, value)

    def _set_attribute(self, name, value):
        current_attr_val = self._attributes.get(name)

        if isinstance(current_attr_val, ReactiveBase):
            current_attr_val.get_signal().disconnect(self._signal)

        self._attributes[name] = value

        if isinstance(value, ReactiveBase):
            value.get_signal().connect(self._signal)
        
        self._notify_change()

    def __getattr__(self, name):
        self._signal.get()
        if name in self._attributes:
            return self._attributes[name]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name == "_attributes" or name == "_signal":
            super().__setattr__(name, value)
        else:
            self._set_attribute(name, value)

    def __repr__(self):
        self._signal.get()
        attrs = ", ".join(f"{name}={repr(value)}" for name, value in self._attributes.items())
        return f"ReactiveStruct({attrs})"


class ReactiveEnum(ReactiveBase):
    def __init__(self, initial_variant, mode=SignalMode.LAZY):
        super().__init__(mode=mode)
        self._current_variant = initial_variant
        self._variant_signals = {} # Dictionary to hold signals for each variant
        # Initialize the signal for the initial variant
        if initial_variant not in self._variant_signals:
            self._variant_signals[initial_variant] = Signal(True, mode=mode)


    def set_variant(self, new_variant):
        if self._current_variant != new_variant:
            old_variant = self._current_variant
            self._current_variant = new_variant

            # Notify signals for the old and new variants
            if old_variant in self._variant_signals:
                self._variant_signals[old_variant].set(False)
            if new_variant not in self._variant_signals:
                self._variant_signals[new_variant] = Signal(False, mode=self._signal._mode) # Create new variant signal
            self._variant_signals[new_variant].set(True)
            self._notify_change() # Notify general enum change

    def get_variant(self):
        self._signal.get() # Depend on the overall enum change
        return self._current_variant

    def on_variant_change(self, variant):
        if variant not in self._variant_signals:
            self._variant_signals[variant] = Signal(self._current_variant == variant, mode=self._signal._mode)
        return self._variant_signals[variant]

    def __repr__(self):
        self._signal.get()
        return f"ReactiveEnum({repr(self._current_variant)})"


class ReactiveContainer(ReactiveBase):
    def __init__(self, initial_content=None, mode=SignalMode.LAZY):
        super().__init__(mode=mode)
        self._content = None
        self.set_content(initial_content)

    def set_content(self, new_content):
        if isinstance(self._content, ReactiveBase):
            self._content.get_signal().disconnect(self._signal)
        elif isinstance(self._content, Signal): # Handle direct Signal objects
            self._content.disconnect(self._signal)

        self._content = new_content

        if isinstance(self._content, ReactiveBase):
            self._content.get_signal().connect(self._signal)
        elif isinstance(self._content, Signal): # Handle direct Signal objects
            self._content.connect(self._signal)
        
        self._notify_change()

    def get_content(self):
        self._signal.get()
        return self._content

    def __repr__(self):
        self._signal.get()
        return f"ReactiveContainer({repr(self._content)})"


if __name__ == "__main__":
    print("--- EAGER Signal Example ---")
    x_eager = Signal(1, mode=SignalMode.EAGER)
    y_eager = Signal.computed(lambda: x_eager.get() * 2, mode=SignalMode.EAGER)
    z_eager = Signal.computed(lambda: y_eager.get() + 5, mode=SignalMode.EAGER)

    effect_x = Signal.effect(lambda: print(f"Eager X updated: {x_eager.get()}"), mode=SignalMode.EAGER)
    effect_y = Signal.effect(lambda: print(f"Eager Y updated: {y_eager.get()}"))
    effect_z = Signal.effect(lambda: print(f"Eager Z updated: {z_eager.get()}"), mode=SignalMode.EAGER)

    print("Setting x_eager to 5...")
    x_eager.set(5)

    print("\n--- LAZY Signal Example ---")
    x_lazy = Signal(1, mode=SignalMode.LAZY)
    y_lazy = Signal.computed(lambda: x_lazy.get() * 2, mode=SignalMode.LAZY)
    z_lazy = Signal.computed(lambda: y_lazy.get() + 5, mode=SignalMode.LAZY)

    effect_y_lazy = Signal.effect(lambda: print(f"Lazy Y accessed: {y_lazy.get()}"))
    effect_z_lazy = Signal.effect(lambda: print(f"Lazy Z accessed: {z_lazy.get()}"))

    print("Setting x_lazy to 5...")
    x_lazy.set(5)
    print("Manually accessing y_lazy...")
    print(f"Direct access to y_lazy: {y_lazy.get()}")
    print("Manually accessing z_lazy...")
    print(f"Direct access to z_lazy: {z_lazy.get()}")

    print("\n--- Eager ReactiveContainer and ReactiveList ---")
    eager_list = ReactiveList([10, 20], mode=SignalMode.EAGER)
    eager_container = ReactiveContainer(eager_list, mode=SignalMode.EAGER)

    eager_list_effect = Signal.effect(lambda: print(f"Eager Container List: {eager_container.get_content().get_list()}"), mode=SignalMode.EAGER)

    print("Appending to eager_list...")
    eager_list.append(30)

    print("\n--- Replacing content of ReactiveContainer ---")
    new_list = ReactiveList(['a', 'b'], mode=SignalMode.EAGER)
    eager_container.set_content(new_list)

    new_list.append('c')

    print("\n--- ReactiveContainer with ReactiveStruct (nested) ---")
    class Department(ReactiveStruct):
        def __init__(self, name, employees, mode=SignalMode.LAZY):
            super().__init__(name=name, employees=employees, mode=mode)

    employees_list = ReactiveList(["Alice", "Bob"], mode=SignalMode.EAGER)
    hr_dept = Department(name="HR", employees=employees_list, mode=SignalMode.EAGER) # Eager struct

    container_dept = ReactiveContainer(hr_dept, mode=SignalMode.EAGER)

    effect_dept = Signal.effect(lambda: print(f"Department Container: {container_dept.get_content().name} has {container_dept.get_content().employees.get_list()}"), mode=SignalMode.EAGER)

    hr_dept.employees.append("Charlie")
    hr_dept.name = "Human Resources"

    print("\n--- Replacing Nested Struct ---")
    new_dept = Department(name="IT", employees=ReactiveList(["Dave", "Eve"], mode=SignalMode.EAGER), mode=SignalMode.EAGER)
    container_dept.set_content(new_dept)

    new_dept.employees.append("Frank")

    hr_dept.employees.append("OLD") 
    print(f"Old HR Dept (no container effect expected): {hr_dept.employees.get_list()}")


    print("\n--- Mixed Eager/Lazy ---")
    source_signal = Signal(100)
    eager_dependent = Signal.computed(lambda: source_signal.get() * 2, mode=SignalMode.EAGER)
    lazy_dependent = Signal.computed(lambda: eager_dependent.get() + 5, mode=SignalMode.LAZY)

    effect_mixed_eager = Signal.effect(lambda: print(f"Mixed Eager Dependent: {eager_dependent.get()}"), mode=SignalMode.EAGER)
    effect_mixed_lazy = Signal.effect(lambda: print(f"Mixed Lazy Dependent: {lazy_dependent.get()}"))

    print("Setting source_signal to 50...")
    source_signal.set(50)

    print("Accessing lazy_dependent...")
    print(f"Accessed lazy_dependent: {lazy_dependent.get()}")


    print("\n--- ReactiveEnum Example (Mixed Eager/Lazy) ---")
    class TrafficLightState(enum.Enum):
        RED = "red"
        YELLOW = "yellow"
        GREEN = "green"

    traffic_light = ReactiveEnum(TrafficLightState.RED, mode=SignalMode.EAGER)

    # Eager effect for RED
    effect_red_light = Signal.effect(
        lambda: print(f"🚦 RED Light Effect (Eager): Stop is {traffic_light.on_variant_change(TrafficLightState.RED).get()}"),
        mode=SignalMode.EAGER
    )
    # Lazy effect for GREEN
    effect_green_light = Signal.effect(
        lambda: print(f"🚦 GREEN Light Effect (Lazy): Go is {traffic_light.on_variant_change(TrafficLightState.GREEN).get()}"),
        mode=SignalMode.LAZY
    )

    # Eager effect for overall state
    effect_overall = Signal.effect(
        lambda: print(f"🚦 Overall State (Eager): {traffic_light.get_variant()}"),
        mode=SignalMode.EAGER
    )

    print("\nChanging traffic light to YELLOW (Eager)...")
    traffic_light.set_variant(TrafficLightState.YELLOW)

    print("\nChanging traffic light to GREEN (Eager)...")
    traffic_light.set_variant(TrafficLightState.GREEN)
    print("Accessing green light effect (which is lazy, will trigger now if dirty):")
    # Manually access the lazy effect, forcing its update
    _ = effect_green_light.get() # This implicitly calls its compute_func if dirty

    print("\nChanging traffic light to RED (Eager)...")
    traffic_light.set_variant(TrafficLightState.RED)
