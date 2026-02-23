from http.server import BaseHTTPRequestHandler
from api.helpers import load_jobs, json_response

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data = load_jobs()
            if not data:
                json_response(self, {
                    'status':     'no_data',
                    'total_jobs': 0,
                    'last_run':   None,
                    'message':    'Scraper has not run yet — it runs every hour automatically'
                })
                return
            json_response(self, {
                'status':     'ok',
                'total_jobs': data.get('total', 0),
                'last_run':   data.get('scraped_at'),
                'message':    'Scraper runs every hour via Vercel cron'
            })
        except Exception as e:
            json_response(self, {'status': 'error', 'error': str(e)}, 500)

    def do_OPTIONS(self):
        json_response(self, {})
