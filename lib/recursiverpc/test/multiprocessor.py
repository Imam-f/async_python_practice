import os
import sys
from concurrent.futures import ProcessPoolExecutor
import rpyc

from uv import find_uv_bin
# def remote_process(x):
#     print(x)
#     pass
# 
# def start_remote_server():
#     pool = ProcessPoolExecutor(max_workers=2)
#     pool.submit(remote_process, 1)
#     pool.submit(remote_process, 3)
#     pool.submit(remote_process, 7)
#     pool.shutdown()

# class RPC_Future:
#     def __init__(self, result, scheduler: "Runner"): ...
#     def check_scheduler(self): ...
#     def get(self): ...
#     def status(self): ...

def start_rpyc_server(port: int = 18812):
    run_uv(["run", "rpyc_classic.py", "-p", str(port), "-m", "oneshot"])

class Pool:
    def __init__(self, max_workers: int):
        # start rpyc server using multiprocessing
        self.pool = ProcessPoolExecutor(max_workers=max_workers)
        
        # connect to rpyc server
        self.max_workers = self.pool._max_workers # type: ignore
        self.scheduler = []
        for i in range(max_workers):
            self.pool.submit(start_rpyc_server, 18812 + i)
            conn = rpyc.classic.connect("localhost", port=18812 + i)
            self.scheduler.append(conn)
            
        # assign to scheduler
        self.scheduler_index = 0
        self.scheduler_status: list[bool] = [False] * self.max_workers
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _schedule(self):
        print(self.scheduler_status, self.scheduler_index)
        index = self.scheduler_index
        for i in range(len(self.scheduler_status)):
            if not self.scheduler_status[(index + i) % len(self.scheduler_status)]:
                index = (index + i) % len(self.scheduler_status)
                for s in range(len(self.scheduler_status)):
                    self.scheduler_status[s] = False
                self.scheduler_status[index] = True
                print(self.scheduler_status, self.scheduler_index)
                break
            else:
                index = (index + i) % len(self.scheduler_status)
                print(self.scheduler_status, self.scheduler_index)
        self.scheduler_index = index
        print(index, "===========", len(self.scheduler), self.scheduler_status)
        return self.scheduler[index]
    
    def apply_async(self, func, *args, **kwargs):
        runner = self._schedule()
        # try:
        function = runner.teleport(func)
        function_async = rpyc.async_(function)
        result = function_async(*args, **kwargs)
        # print(result.ready)
        # runner.namespace["printer"] = print
        # runner.execute("printer(\"Hello my name is inigo montoya\")")
        return result
        # finally:
        #     print("=>>> close runner")
        #     runner.close()
        #     index = self.scheduler.index(runner)
        #     self.scheduler.remove(runner)
        #     self.scheduler_status.pop(index)
        # return RPC_Future(result, self)

    def close(self):
        for i in self.scheduler:
            i.close()
        self.pool.shutdown()
    
    def __del__(self):
        self.close()


def _detect_virtualenv() -> str:
    """
    Find the virtual environment path for the current Python executable.
    """

    # If it's already set, then just use it
    value = os.getenv("VIRTUAL_ENV")
    if value:
        return value

    # Otherwise, check if we're in a venv
    venv_marker = os.path.join(sys.prefix, "pyvenv.cfg")

    if os.path.exists(venv_marker):
        return sys.prefix

    return ""

def run_uv(args) -> None:
    uv = os.fsdecode(find_uv_bin())

    env = os.environ.copy()
    venv = _detect_virtualenv()
    if venv:
        env.setdefault("VIRTUAL_ENV", venv)

    # Let `uv` know that it was spawned by this Python interpreter
    env["UV_INTERNAL__PARENT_INTERPRETER"] = sys.executable

    if sys.platform == "win32":
        import subprocess

        completed_process = subprocess.run([uv, *args], env=env)
        sys.exit(completed_process.returncode)
    else:
        os.execvpe(uv, [uv, *args], env=env)
