from recursiverpc import Recursive_RPC, localprocess, networkprocess, proxyprocess, RPC_Future
import time
import os
from dotenv import load_dotenv
load_dotenv()
import traceback

from typing import Callable
from queue import Queue
import paramiko
from plumbum.machines.paramiko_machine import ParamikoMachine
from plumbum import local

# import faulthandler
# faulthandler.enable()
# faulthandler.dump_traceback_later(timeout=10)

#################################################################

def worker_func(number):
    sum_num = 0
    for i in range(30000000):
    # for i in range(300000):
    # for i in range(3000000):
        sum_num += i
    return number * number

def main():
    # Create a pool of worker processes
    # The number of processes is set to the number of CPU cores
    
    HOSTNAME = os.getenv("HOSTNAME")
    if HOSTNAME is None:
        HOSTNAME = "localhost"
    USER = os.getenv("USER")
    if USER is None:
        USER = "root"
    PORT = os.getenv("PORT")
    if PORT is None:
        PORT = 22
    else:
        PORT = int(PORT)
    PASSWORD = os.getenv("PASSWORD")
    if PASSWORD is None:
        PASSWORD = None
    
    import random
    from plumbum.commands import BaseCommand
    from plumbum.machines.session import ShellSession, ShellSessionError, MarkedPipe, shell_logger, SessionPopen
    def popen(self: ShellSession, cmd):
        """Runs the given command in the shell, adding some decoration around it. Only a single
        command can be executed at any given time.

        :param cmd: The command (string or :class:`Command <plumbum.commands.BaseCommand>` object)
                    to run
        :returns: A :class:`SessionPopen <plumbum.session.SessionPopen>` instance
        """
        if self.proc is None:
            raise ShellSessionError("Shell session has already been closed")
        if self._current and not self._current._done:
            raise ShellSessionError("Each shell may start only one process at a time")

        full_cmd = "function prompt {\" \"}; "
        full_cmd += cmd.formulate(1) if isinstance(cmd, BaseCommand) else cmd
        marker = f"--.END{time.time() * random.random()}.--"
        if full_cmd.strip():
            full_cmd += " ; "
        else:
            full_cmd = "function prompt {\" \"}; "
            full_cmd = "true ; "
        # full_cmd += f"echo $? ; echo '{marker}'"
        full_cmd += f"Write-Output $LASTEXITCODE; Write-Output '{marker}'"
        if not self.isatty:
            full_cmd += f" ; [Console]::Error.WriteLine('{marker}')"
        if self.custom_encoding:
            full_cmd = full_cmd.encode(self.custom_encoding)
        shell_logger.debug("Running %r", full_cmd)
        self.proc.stdin.write(full_cmd + b"\n") # type: ignore
        self.proc.stdin.flush()
        print("=>>>>", full_cmd)
        self._current = SessionPopen(
            self.proc,
            full_cmd,
            self.isatty,
            self.proc.stdin,
            MarkedPipe(self.proc.stdout, marker),
            MarkedPipe(self.proc.stderr, marker),
            self.custom_encoding,
            host=self.host,
        )
        return self._current

        
    # ShellSession.popen = types.MethodType(ShellSession.popen, ShellSession)
    ShellSession.popen = popen
    # HOSTNAME_FORWARD = HOSTNAME
    # PORT_FORWARD = PORT
    # ssh_login = (USER, PASSWORD)
    
    # print("connecting")
    # print(HOSTNAME, USER, PORT, PASSWORD)
    sshmachine = ParamikoMachine(host=HOSTNAME,
                                 user=USER, 
                                 port=PORT, 
                                 password=PASSWORD,
                                 missing_host_policy=paramiko.AutoAddPolicy())
    
    print("ls")
    # print(ls_cmd())
    # print(*sshmachine.env["PATH"].split(":"), sep="\n") 
    print("================")
    print(sshmachine.cwd)
    print(local["ls"]())
    try:
        # env_cmd = sshmachine["/usr/bin/env"]
        # env_cmd = sshmachine["Env"]
        env_cmd = sshmachine["env.exe"]
        print('a')
        print("env", env_cmd)
        print("env", env_cmd())
        print("\n\n\n\n\n\n")
        ls_cmd = sshmachine["ls.exe"]
        print(ls_cmd())
    except Exception as e:
        import traceback
        traceback.print_exc()
        # print(e)
    print(os.system("asdfhjk"), os.system("pwd"))
    
    print("================")
    sshmachine.close()

if __name__ == "__main__": 
    main()
