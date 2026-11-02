"""Verify HTTP server works in this environment."""
import http.server, socketserver, functools, threading, time, os, json, sys
sys.stdout.reconfigure(encoding="utf-8")

class R(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

h = functools.partial(http.server.SimpleHTTPRequestHandler, directory=r'E:\Deepseek\ReTools\moex-screener')
try:
    s = R(("", 8099), h)
    t = threading.Thread(target=s.serve_forever, daemon=True)
    t.start()
    time.sleep(1)
    
    import urllib.request
    resp = urllib.request.urlopen("http://127.0.0.1:8099/dashboard.html", timeout=5)
    print(f"dashboard.html: HTTP {resp.status}")
    
    resp2 = urllib.request.urlopen("http://127.0.0.1:8099/data/screener.json", timeout=5)
    data = json.loads(resp2.read().decode())
    print(f"screener.json: HTTP {resp2.status}, signals={len(data.get('signals',[]))}, robots={len(data.get('robots',[]))}")
except Exception as e:
    print(f"ERROR: {e}")
