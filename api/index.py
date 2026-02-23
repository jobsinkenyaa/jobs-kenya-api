from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            'status':  'running',
            'service': '🇰🇪 Jobs Kenya API',
            'endpoints': {
                'GET  /jobs':   'Get scraped jobs (?county=Nairobi&type=NGO&q=accountant)',
                'GET  /status': 'Check scraper status',
                'GET  /scrape': 'Trigger scrape (runs automatically every hour via cron)',
            }
        }, indent=2)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body.encode())
