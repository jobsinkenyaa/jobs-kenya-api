from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from api.helpers import load_jobs, json_response

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Parse query params
            params  = parse_qs(urlparse(self.path).query)
            county  = params.get('county',  [''])[0].lower()
            jtype   = params.get('type',    [''])[0].lower()
            keyword = params.get('q',       [''])[0].lower()
            limit   = min(int(params.get('limit', ['80'])[0]), 200)

            data = load_jobs()
            if not data:
                json_response(self, {'total': 0, 'jobs': [], 'message': 'No jobs yet — scraper runs every hour'})
                return

            jobs = data.get('jobs', [])

            # Apply filters
            if county:  jobs = [j for j in jobs if county  in j.get('county', '').lower()]
            if jtype:   jobs = [j for j in jobs if jtype   in j.get('type',   '').lower()]
            if keyword: jobs = [j for j in jobs if keyword in (j.get('title','') + ' ' + j.get('company','')).lower()]

            json_response(self, {
                'total':      len(jobs),
                'scraped_at': data.get('scraped_at'),
                'jobs':       jobs[:limit]
            })
        except Exception as e:
            json_response(self, {'error': str(e), 'total': 0, 'jobs': []}, 500)

    def do_OPTIONS(self):
        json_response(self, {})
