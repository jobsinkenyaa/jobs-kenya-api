from http.server import BaseHTTPRequestHandler
from api.helpers import run_all_scrapers, json_response
import os

ADMIN_SECRET = os.getenv('ADMIN_SECRET', 'jobskenya-secret-2025')

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Called automatically by Vercel cron every hour
        try:
            result = run_all_scrapers()
            json_response(self, {
                'success':    True,
                'total_jobs': result.get('total', 0),
                'scraped_at': result.get('scraped_at'),
                'message':    'Scrape complete'
            })
        except Exception as e:
            json_response(self, {'success': False, 'error': str(e)}, 500)

    def do_POST(self):
        # Manual trigger — requires admin token
        token = self.headers.get('X-Admin-Token', '')
        if token != ADMIN_SECRET:
            json_response(self, {'error': 'Unauthorized'}, 401)
            return
        try:
            result = run_all_scrapers()
            json_response(self, {
                'success':    True,
                'total_jobs': result.get('total', 0),
                'scraped_at': result.get('scraped_at'),
            })
        except Exception as e:
            json_response(self, {'success': False, 'error': str(e)}, 500)

    def do_OPTIONS(self):
        json_response(self, {})
