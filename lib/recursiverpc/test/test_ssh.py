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
    print(*sshmachine.env["PATH"].split(":"), sep="\n") 
    print("================")
    print(sshmachine.cwd)
    print(local["ls"]())
    try:
        env_cmd = sshmachine["/usr/bin/env"]
        print("env", env_cmd())
        ls_cmd = sshmachine["/usr/bin/env"]
        print(ls_cmd())
    except Exception as e:
        print(e)
    print(os.system("asdfhjk"), os.system("pwd"))
    
    print("================")
    sshmachine.close()

if __name__ == "__main__": 
    main()
