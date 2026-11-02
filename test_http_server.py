import http.server, socketserver, functools, threading, time, sys
sys.stdout.reconfigure(encoding="utf-8")
print("Starting HTTP server on 8082...", flush=True)
class R(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
h = functools.partial(http.server.SimpleHTTPRequestHandler, directory=r'E:\Deepseek\ReTools\moex-screener')
try:
    s = R(("", 8082), h)
    print(f"Server bound to port 8082: {s.server_address}", flush=True)
    t = threading.Thread(target=s.serve_forever, daemon=True)
    t.start()
    print("Server thread started, waiting 12s...", flush=True)
    time.sleep(12)
except Exception as e:
    print(f"ERROR: {e}", flush=True)
