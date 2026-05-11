import ast
import inspect
import json
import multiprocessing
import socket
import textwrap
import threading


# =============================================================================
# SOCKET DB SERVER (threaded, handles multiple workers in parallel)
# =============================================================================
class GlobalsDBServer:
    def __init__(self, host="127.0.0.1", port=0):
        self.host = host
        self.port = port
        self.storage = {}
        self.lock = threading.Lock()
        self.sock = None
        self.running = False

    def start(self, ready_event):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.host, self.port = self.sock.getsockname()
        self.sock.listen(8)

        ready_event.set()
        self.running = True

        while self.running:
            try:
                self.sock.settimeout(1.0)
                conn, _ = self.sock.accept()
                t = threading.Thread(target=self._handle_client, args=(conn,))
                t.daemon = True
                t.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_client(self, conn):
        try:
            with conn:
                file = conn.makefile("r", encoding="utf-8")
                for line in file:
                    req = json.loads(line)
                    cmd = req.get("cmd")
                    key = req.get("key")

                    with self.lock:
                        if cmd == "GET":
                            resp = {"status": "OK", "value": self.storage[key]} if key in self.storage else {"status": "NOTFOUND"}
                        elif cmd == "SET":
                            self.storage[key] = req.get("value")
                            resp = {"status": "OK"}
                        elif cmd == "QUIT":
                            resp = {"status": "BYE"}
                            conn.sendall((json.dumps(resp) + "\n").encode())
                            break
                        else:
                            resp = {"status": "ERR"}

                    conn.sendall((json.dumps(resp) + "\n").encode())
        except Exception:
            pass

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


# =============================================================================
# SOCKET DB CLIENT
# =============================================================================
class GlobalsDBClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._file = None
        self._sock = None

    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._file = self._sock.makefile("r", encoding="utf-8")

    def close(self):
        if self._sock:
            try:
                self._send({"cmd": "QUIT"})
            except Exception:
                pass
            self._sock.close()

    def _send(self, req):
        if self._sock is None:
            raise RuntimeError("Not connected to DB server")
        self._sock.sendall((json.dumps(req) + "\n").encode())
        if self._file is None:
            raise RuntimeError("No file stream for reading responses")
        return json.loads(self._file.readline().strip())

    def get(self, key):
        resp = self._send({"cmd": "GET", "key": key})
        return resp.get("value") if resp.get("status") == "OK" else None

    def set(self, key, value):
        self._send({"cmd": "SET", "key": key, "value": value})


# =============================================================================
# ORIGINAL FUNCTION (uses globals normally)
# =============================================================================
item_count = 0
total_sum = 0

def process_items(items):
    import time
    global item_count, total_sum

    time.sleep(0.5)
    item_count = item_count + len(items)
    total_sum = total_sum + sum(items)

    return {"count": item_count, "total": total_sum}


# =============================================================================
# AST REWRITER: turns 'global x / x = x + 1' into DB calls
# =============================================================================
def rewrite_globals_via_ast(func, db_get, db_set):
    """Return a new function where tracked globals are routed through db_get/db_set."""
    source = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(source)

    class Transformer(ast.NodeTransformer):
        def __init__(self):
            self.global_names = set()

        def visit_FunctionDef(self, node):
            # 1. Discover every name declared 'global' in this function
            for child in ast.walk(node):
                if isinstance(child, ast.Global):
                    self.global_names.update(child.names)

            # 2. Remove the 'global' statements themselves
            node.body = [stmt for stmt in node.body if not isinstance(stmt, ast.Global)]

            # 3. Continue generic walk to transform names/assignments
            return self.generic_visit(node)

        def visit_Name(self, node):
            if node.id not in self.global_names:
                return node
            if isinstance(node.ctx, ast.Load):
                # x  ->  _db_get('x')
                return ast.Call(
                    func=ast.Name(id="_db_get", ctx=ast.Load()),
                    args=[ast.Constant(value=node.id)],
                    keywords=[],
                )
            return node

        def visit_Assign(self, node):
            # Single-target assignment to a global?
            if (
                len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in self.global_names
            ):
                # x = expr  ->  _db_set('x', expr)
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
            # x += expr  ->  _db_set('x', _db_get('x') + expr)
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

    new_tree = Transformer().visit(tree)
    ast.fix_missing_locations(new_tree)

    # Compile in a namespace that supplies the DB helpers and builtins
    namespace = {
        "__builtins__": __builtins__,
        "_db_get": db_get,
        "_db_set": db_set,
    }
    exec(compile(new_tree, func.__code__.co_filename, "exec"), namespace)
    return namespace[func.__name__]


# =============================================================================
# MULTIPROCESSING WORKER (runs in fresh Windows spawn process)
# =============================================================================
def worker_entry(db_host, db_port, worker_id, items):
    client = GlobalsDBClient(db_host, db_port)
    client.connect()
    
    print(f"Worker {worker_id} with PID {multiprocessing.current_process().pid} processing items: {items}")

    def db_get(name):
        val = client.get(f"shared:{name}")
        return 0 if val is None else val

    def db_set(name, value):
        client.set(f"shared:{name}", value)

    try:
        # Rewrite process_items so globals become socket calls
        patched = rewrite_globals_via_ast(process_items, db_get, db_set)
        result = patched(items)
        return {"worker": worker_id, "items": items, "result": result}
    finally:
        client.close()


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    ready_event = multiprocessing.Event()
    server = GlobalsDBServer()
    server_thread = threading.Thread(target=server.start, args=(ready_event,))
    server_thread.daemon = True
    server_thread.start()
    ready_event.wait()

    print(f"DB server listening on {server.host}:{server.port}\n")

    tasks = [
        (server.host, server.port, 0, [10, 20]),
        (server.host, server.port, 1, [5, 15]),
        (server.host, server.port, 2, [100]),
        (server.host, server.port, 3, [1, 2, 3]),
    ]

    with multiprocessing.Pool(processes=4) as pool:
        results = pool.starmap(worker_entry, tasks)

    print("Worker results:")
    for r in results:
        print(f"  {r}")

    # Verify shared accumulated state
    verify = GlobalsDBClient(server.host, server.port)
    verify.connect()
    final_count = verify.get("shared:item_count")
    final_total = verify.get("shared:total_sum")
    verify.close()

    print("\nFinal DB state:")
    print(f"  item_count = {final_count}")   # 8
    print(f"  total_sum  = {final_total}")   # 156

    server.stop()
    server_thread.join(timeout=2)
