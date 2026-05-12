import ast
import inspect
import multiprocessing
import pickle
import socket
import struct
import textwrap
import threading


# =============================================================================
# PICKLE-BASED FRAMING OVER TCP
# =============================================================================
def _send_msg(sock, obj):
    data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    hdr = struct.pack("!I", len(data))
    sock.sendall(hdr + data)


def _recv_msg(sock):
    hdr = sock.recv(4)
    if not hdr:
        return None
    (size,) = struct.unpack("!I", hdr)
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return pickle.loads(buf)


# =============================================================================
# PARENT-SIDE SERVER: MUTATES THE PARENT'S GLOBALS DIRECTLY
# =============================================================================
class ParentGlobalsServer:
    """Handles one client at a time (iterative).  Each child process blocks
    until the previous one finishes its DB conversation, which naturally
    serialises the read-modify-write cycles and keeps the counter correct."""
    
    def __init__(self, parent_globals, host="127.0.0.1", port=0):
        self.g = parent_globals          # <-- the parent's live globals dict
        self.host = host
        self.port = port
        self._sock = None
        self._running = False

    def start(self, ready_event):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self.host, self.port = self._sock.getsockname()
        self._sock.listen(5)

        ready_event.set()
        self._running = True

        while self._running:
            try:
                self._sock.settimeout(1.0)
                conn, _ = self._sock.accept()
                self._handle(conn)
                conn.close()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle(self, conn):
        while True:
            msg = _recv_msg(conn)
            if msg is None:
                break
            cmd = msg.get("cmd")
            key = msg.get("key")

            if cmd == "GET":
                # Read directly from the parent's globals
                value = self.g.get(key)
                resp = {"ok": True, "value": value}
            elif cmd == "SET":
                # Write directly into the parent's globals
                self.g[key] = msg.get("value")
                resp = {"ok": True}
            else:
                resp = {"ok": False, "error": "unknown command"}

            _send_msg(conn, resp)

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass


# =============================================================================
# CHILD-SIDE CLIENT
# =============================================================================
class ParentGlobalsClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = None

    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))

    def close(self):
        if self._sock:
            self._sock.close()

    def get(self, key):
        _send_msg(self._sock, {"cmd": "GET", "key": key})
        resp = _recv_msg(self._sock)
        return resp.get("value") if resp and resp.get("ok") else None

    def set(self, key, value):
        _send_msg(self._sock, {"cmd": "SET", "key": key, "value": value})
        _recv_msg(self._sock)  # consume ACK


# =============================================================================
# FUNCTION WHOSE GLOBALS WE WANT TO SHARE
# =============================================================================
def process_items(items):
    import time
    global item_count, total_sum

    time.sleep(0.5)
    item_count = item_count + len(items)
    total_sum = total_sum + sum(items)
    return {"count": item_count, "total": total_sum}


# =============================================================================
# AST REWRITER:  turns 'global x' into explicit _db_get / _db_set calls
# =============================================================================
class _GlobalRewriter(ast.NodeTransformer):
    def __init__(self):
        self.global_names = set()

    def visit_FunctionDef(self, node):
        # Discover every name declared 'global'
        for child in ast.walk(node):
            if isinstance(child, ast.Global):
                self.global_names.update(child.names)
        # Strip the 'global' statements (they are meaningless once we rewrite)
        node.body = [s for s in node.body if not isinstance(s, ast.Global)]
        self.generic_visit(node)
        return node

    def visit_Name(self, node):
        if node.id not in self.global_names:
            return node
        if isinstance(node.ctx, ast.Load):
            return ast.Call(
                func=ast.Name(id="_db_get", ctx=ast.Load()),
                args=[ast.Constant(value=node.id)],
                keywords=[],
            )
        return node

    def visit_Assign(self, node):
        if (
            len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in self.global_names
        ):
            return ast.Expr(
                ast.Call(
                    func=ast.Name(id="_db_set", ctx=ast.Load()),
                    args=[
                        ast.Constant(value=node.targets[0].id),
                        self.visit(node.value),
                    ],
                    keywords=[],
                )
            )
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Name) and node.target.id in self.global_names:
            return ast.Expr(
                ast.Call(
                    func=ast.Name(id="_db_set", ctx=ast.Load()),
                    args=[
                        ast.Constant(value=node.target.id),
                        ast.BinOp(
                            left=ast.Call(
                                func=ast.Name(id="_db_get", ctx=ast.Load()),
                                args=[ast.Constant(value=node.target.id)],
                                keywords=[],
                            ),
                            op=node.op,
                            right=self.visit(node.value),
                        ),
                    ],
                    keywords=[],
                )
            )
        return self.generic_visit(node)


def rewrite_globals_via_ast(func, db_get, db_set):
    source = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(source)
    new_tree = _GlobalRewriter().visit(tree)
    ast.fix_missing_locations(new_tree)

    namespace = {
        "__builtins__": __builtins__,
        "_db_get": db_get,
        "_db_set": db_set,
    }
    exec(compile(new_tree, func.__code__.co_filename, "exec"), namespace)
    return namespace[func.__name__]


# =============================================================================
# WORKER ENTRY POINT (runs in a fresh Windows spawn process)
# =============================================================================
def worker_entry(db_host, db_port, worker_id, items):
    client = ParentGlobalsClient(db_host, db_port)
    client.connect()
    
    print(f"Worker {worker_id} with PID {multiprocessing.current_process().pid} processing items: {items}")

    def db_get(name):
        # If the parent hasn't created the variable yet, default to 0
        val = client.get(name)
        return 0 if val is None else val

    def db_set(name, value):
        client.set(name, value)

    try:
        # Build a local function whose 'globals' are actually socket calls
        patched = rewrite_globals_via_ast(process_items, db_get, db_set)
        result = patched(items)
        return {"worker": worker_id, "items": items, "result": result}
    finally:
        client.close()


# =============================================================================
# MAIN (parent process)
# =============================================================================
if __name__ == "__main__":
    # These live in the PARENT's global namespace.  The socket server will
    # mutate these variables directly when children ask it to.
    item_count = 0
    total_sum = 0

    ready_event = multiprocessing.Event()
    server = ParentGlobalsServer(globals())   # <-- hands over parent globals
    server_thread = threading.Thread(target=server.start, args=(ready_event,))
    server_thread.daemon = True
    server_thread.start()
    ready_event.wait()

    print(f"Server listening on {server.host}:{server.port}")
    print(f"BEFORE pool: item_count={item_count}, total_sum={total_sum}\n")

    tasks = [
        (server.host, server.port, 0, [10, 20]),
        (server.host, server.port, 1, [5, 15]),
        (server.host, server.port, 2, [100]),
        (server.host, server.port, 3, [1, 2, 3]),
    ]

    with multiprocessing.Pool(processes=4) as pool:
        results = pool.starmap(worker_entry, tasks)

    print("Worker return values:")
    for r in results:
        print(f"  {r}")

    print(f"\nAFTER pool: item_count={item_count}, total_sum={total_sum}")