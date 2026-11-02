"""Standalone HTTP server for the MOEX screener dashboard.

Run this alongside the screener (or instead of --http) when the screener
itself cannot bind a socket:

    python server.py [port]

It serves the project directory so the browser can fetch data/screener.json.
"""
import http.server
import socketserver
import sys
import os

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
DIRECTORY = os.path.dirname(os.path.abspath(__file__))


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def end_headers(self):
        # Allow the dashboard to fetch from any origin (file://, other ports)
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def log_message(self, *args):
        pass  # silence per-request logging


class ReuseTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    httpd = ReuseTCPServer(("0.0.0.0", PORT), Handler)
    print(f"Serving {DIRECTORY} at http://localhost:{PORT}/dashboard.html")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
