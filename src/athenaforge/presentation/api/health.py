from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from athenaforge import __version__


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "healthy", "version": __version__}
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging


def start_health_server(port: int = 8080) -> HTTPServer:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    return server
