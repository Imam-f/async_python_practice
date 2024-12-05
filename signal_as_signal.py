import os
import signal
from multiprocessing import Process
import time


def handle_sigusr1(signum, frame):
    """Handle SIGUSR1 signal in the parent process."""
    print(f"[Parent] Received SIGUSR1 (signal {signum}). Child is waiting for data.")


def child_process():
    """Child process sends SIGUSR1 to the parent to indicate readiness."""
    parent_pid = os.getppid()  # Get parent process ID
    print(f"[Child] Sending SIGUSR1 to parent process (PID: {parent_pid})")
    os.kill(parent_pid, signal.SIGUSR1)
    time.sleep(2)  # Simulate waiting for data


if __name__ == "__main__":
    # Register a custom signal handler for SIGUSR1 in the parent process
    signal.signal(signal.SIGUSR1, handle_sigusr1)

    # Create and start the child process
    child = Process(target=child_process)
    child.start()

    # Wait for the child process to complete
    child.join()

    print("[Parent] Child process has finished.")
