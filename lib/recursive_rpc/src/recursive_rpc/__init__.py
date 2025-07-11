from .recursive_rpc import tupleprocess, localprocess, networkprocess, proxyprocess
from .recursive_rpc import Recursive_RPC, RPC_Future
from .recursive_rpc import ProcessRunner, NetworkRunner, ProxyRunner

def hello() -> str:
    return "Hello from recursive-rpc!"
