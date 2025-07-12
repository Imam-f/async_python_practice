from typing import Generator, Tuple, NamedTuple
from enum import Enum, auto

class OpCode(Enum):
    HOLD = auto()
    INCREMENT = auto()
    RESET = auto()

class PipelineData(NamedTuple):
    op: OpCode
    value: int
from typing import Generator, Tuple, NamedTuple
from enum import Enum, auto

class OpCode(Enum):
    HOLD = auto()
    INCREMENT = auto()
    RESET = auto()

class PipelineData(NamedTuple):
    op: OpCode
    value: int

def pipelined_counter_module(
    width: int = 4,
) -> Generator[int, Tuple[int, int], None]:
    """
    An optimized 3-stage pipelined counter.

    - Stage 1 holds the authoritative state and decodes the operation.
    - Stage 2 executes the operation.
    - The output is the result from Stage 2 after a 2-cycle delay.
    """
    # --- State and Pipeline Registers ---
    # Stage 1 holds the authoritative state of the counter.
    s1_state: int = 0

    # Register between Stage 1 and Stage 2.
    s1_s2_reg: PipelineData = PipelineData(OpCode.HOLD, 0)

    # Register between Stage 2 and the output. This is the final pipeline register.
    s2_s3_reg: int = 0

    max_val: int = 2**width

    # Prime the co-routine. The initial output is the value in the final register.
    (reset, start) = yield s2_s3_reg

    while True:
        # --- COMBINATIONAL LOGIC (Calculate next values for all registers) ---

        # Stage 2 Logic: Calculate the next value for the final output register.
        # This is the "Execute" stage.
        op_from_s1, val_from_s1 = s1_s2_reg
        if op_from_s1 == OpCode.RESET:
            s2_s3_reg_next = 0
        elif op_from_s1 == OpCode.INCREMENT:
            s2_s3_reg_next = (val_from_s1 + 1) % max_val
        else:  # HOLD
            s2_s3_reg_next = val_from_s1

        # Stage 1 Logic: Decide the operation and calculate the next internal state.
        # This is the "Decode" stage.
        if reset == 1:
            op_next = OpCode.RESET
            s1_state_next = 0
        elif start == 1:
            op_next = OpCode.INCREMENT
            s1_state_next = (s1_state + 1) % max_val
        else:
            op_next = OpCode.HOLD
            s1_state_next = s1_state
        # Stage 1 passes its *current* state and the chosen operation down the pipe.
        s1_s2_reg_next = PipelineData(op_next, s1_state)

        # --- SEQUENTIAL LOGIC (Update all registers on the "clock edge") ---
        s1_state = s1_state_next
        s1_s2_reg = s1_s2_reg_next
        s2_s3_reg = s2_s3_reg_next

        # --- Yield output and get inputs for the NEXT cycle ---
        # The visible output is the value from the end of the pipeline.
        (reset, start) = yield s2_s3_reg

def wait_cycles(
    num_cycles: int, signals: Tuple[int, int]
) -> Generator[Tuple[int, int], None, None]:
    """
    A helper generator that yields the same signal state for num_cycles.
    """
    for _ in range(num_cycles):
        yield signals

def wait_until_cycles(
    num_cycles: int, signals: Tuple[int, int]
) -> Generator[Tuple[int, int], None, None]:
    """
    A helper generator that yields the same signal state for num_cycles.
    """
    global time
    for _ in range(num_cycles - time):
        yield signals

def testbench_module_old(
    num_cycles: int = 30,
) -> Generator[Tuple[int, int], None, None]:
    """
    A generator that produces a sequence of stimuli (reset, start) for the DUT.
    """
    reset_signal = 0
    start_signal = 0

    for t in range(num_cycles):
        # Set the signal values based on the current time step
        if t == 2:
            print("TB: Asserting RESET.")
            reset_signal = 1
        elif t == 3:
            print("TB: De-asserting RESET.")
            reset_signal = 0
        elif t == 6:
            print("TB: Asserting START.")
            start_signal = 1
        elif t == 16:
            print("TB: De-asserting START.")
            start_signal = 0

        yield (reset_signal, start_signal)
        
def testbench_module(num_cycles: int = 30) -> Generator[Tuple[int, int], None, None]:
    """
    A generator that produces stimuli using relative time delays.
    It uses a helper generator with 'yield from' to manage waits.
    """
    global time
    # Initial signal states
    reset_signal = 0
    start_signal = 0
    current_signals = (reset_signal, start_signal)

    # --- Phase 1: Initial State ---
    print("TB: Waiting for 2 cycles in initial state.")
    yield from wait_cycles(2, current_signals)

    # --- Phase 2: Reset Sequence ---
    print("TB: Asserting RESET for 1 cycle.")
    reset_signal = 1
    current_signals = (reset_signal, start_signal)
    yield current_signals  # Assert for one cycle

    print("TB: De-asserting RESET and waiting 3 cycles.")
    reset_signal = 0
    current_signals = (reset_signal, start_signal)
    yield from wait_cycles(3, current_signals)

    # --- Phase 3: Counting Sequence ---
    print("TB: Asserting START and running counter for 10 cycles.")
    start_signal = 1
    current_signals = (reset_signal, start_signal)
    yield from wait_cycles(10, current_signals)

    # --- Phase 4: Hold Sequence ---
    print("TB: De-asserting START and holding value for 5 cycles.")
    start_signal = 0
    current_signals = (reset_signal, start_signal)
    yield from wait_until_cycles(num_cycles, current_signals)        

# --- Simulator Section ---
if __name__ == "__main__":
    print("--- Starting Generator-based Simulation (Stage-based Reg Names) ---")

    # 1. Instantiate the DUT and the Testbench generators.
    dut = pipelined_counter_module(width=4)
    testbench = testbench_module(num_cycles=30)

    # 2. Prime the DUT co-routine.
    dut_output = dut.send(None)

    # 3. Run the simulation loop.
    print("Pipeline Latency: 2 cycles from input change to output change.")
    print("Time | Rst In | Start In | Count Out")
    print("-----+--------+----------+-----------")

    global time
    for t, inputs_from_tb in enumerate(testbench):
        time = t
        reset_in, start_in = inputs_from_tb

        # Print the state of all signals for the current clock cycle.
        print(f"{t:4d} | {reset_in:6d} | {start_in:8d} | {dut_output:9d}")

        # "posedge clk": Send new inputs to DUT, get new output.
        dut_output = dut.send(inputs_from_tb)

    print("--- Simulation Finished ---")