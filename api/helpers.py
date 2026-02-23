"""
Shared helpers for Jobs Kenya Vercel API
"""
import re, json, os, requests
from datetime import datetime
import xml.etree.ElementTree as ET

# ── KV STORE (Vercel KV via REST API) ───────────────────────────
KV_URL   = os.getenv('KV_REST_API_URL', '')
KV_TOKEN = os.getenv('KV_REST_API_TOKEN', '')
JOBS_KEY = 'jobs_kenya_v1'

def kv_set(key, value):
    """Save data to Vercel KV database"""
    if not KV_URL or not KV_TOKEN:
        return False
    try:
        res = requests.post(
            f'{KV_URL}/set/{key}',
            headers={'Authorization': f'Bearer {KV_TOKEN}'},
            json=value,
            timeout=10
        )
        return res.ok
    except Exception as e:
        print(f'KV set error: {e}')
        return False

def kv_get(key):
    """Read data from Vercel KV database"""
    if not KV_URL or not KV_TOKEN:
        return None
    try:
        res = requests.get(
            f'{KV_URL}/get/{key}',
            headers={'Authorization': f'Bearer {KV_TOKEN}'},
            timeout=10
        )
        if res.ok:
            data = res.json()
            return data.get('result')
        return None
    except Exception as e:
        print(f'KV get error: {e}')
        return None

def save_jobs(jobs):
    payload = {
        'total':      len(jobs),
        'scraped_at': datetime.now().isoformat(),
        'jobs':       jobs
    }
    kv_set(JOBS_KEY, payload)
    return payload

def load_jobs():
    return kv_get(JOBS_KEY)


# ── TEXT HELPERS ─────────────────────────────────────────────────
def clean(t):
    return ' '.join((t or '').strip().split())

def strip_html(t):
    return re.sub(r'<[^>]+>', ' ', t or '').strip()

def extract_email(text):
    emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text or '')
    bad = ['noreply','no-reply','donotreply','example','sentry','test@']
    return next((e for e in emails if not any(b in e.lower() for b in bad)), '')

def extract_county(text):
    counties = ['Nairobi','Mombasa','Kisumu','Nakuru','Eldoret','Kiambu',
                'Machakos','Nyeri','Meru','Kakamega','Kisii','Kilifi',
                'Embu','Garissa','Bungoma','Kajiado','Kericho','Turkana',
                'Homa Bay','Nyamira','Narok','Vihiga','Thika','Lamu','Siaya']
    t = (text or '').lower()
    for c in counties:
        if c.lower() in t:
            return c
    if 'remote' in t or 'online' in t:
        return 'Remote'
    return 'Nairobi'

def detect_type(text):
    t = (text or '').lower()
    if any(w in t for w in ['intern','attachment','graduate trainee']): return 'Internship'
    if any(w in t for w in ['part-time','part time','casual']): return 'Part-Time'
    if any(w in t for w in ['government','county','ministry','public service','psc']): return 'Government'
    if any(w in t for w in ['ngo','unicef','undp','wfp','unhcr','oxfam','non-profit','nonprofit']): return 'NGO'
    if any(w in t for w in ['remote','work from home','wfh']): return 'Remote'
    if any(w in t for w in ['contract','consultant','temporary','freelance']): return 'Contract'
    return 'Full-Time'

def detect_sector(text):
    t = (text or '').lower()
    if any(w in t for w in ['software','developer','ict','data','cyber','tech','systems']): return 'ICT & Technology'
    if any(w in t for w in ['nurse','doctor','medical','health','clinical','pharmacy']): return 'Health & Medicine'
    if any(w in t for w in ['finance','account','audit','tax','banking']): return 'Finance & Banking'
    if any(w in t for w in ['engineer','civil','mechanical','electrical']): return 'Engineering'
    if any(w in t for w in ['teach','tutor','lecturer','school','education']): return 'Education'
    if any(w in t for w in ['farm','agri','crop','livestock','food']): return 'Agriculture'
    if any(w in t for w in ['market','sales','brand','advertis','digital']): return 'Marketing & Sales'
    if any(w in t for w in ['ngo','humanitarian','relief','programme']): return 'NGO / Non-Profit'
    if any(w in t for w in ['legal','lawyer','advocate','compliance']): return 'Legal'
    if any(w in t for w in ['driver','transport','logistics','supply']): return 'Transport & Logistics'
    return 'General'

def deduplicate(jobs):
    seen, unique = set(), []
    for j in jobs:
        key = f"{j.get('title','').lower()[:40]}|{j.get('company','').lower()[:25]}"
        if key not in seen:
            seen.add(key)
            unique.append(j)
    return unique


# ── SCRAPER FUNCTIONS ────────────────────────────────────────────

def scrape_reliefweb():
    """ReliefWeb public API — NGO/UN jobs in Kenya"""
    print('[ReliefWeb] Fetching...')
    jobs = []
    try:
        url = (
            'https://api.reliefweb.int/v1/jobs'
            '?appname=jobskenya'
            '&filter[field]=country.name&filter[value]=Kenya'
            '&limit=50'
            '&fields[include][]=title'
            '&fields[include][]=body'
            '&fields[include][]=source'
            '&fields[include][]=date'
            '&fields[include][]=url'
        )
        res = requests.get(url, timeout=25)
        if not res.ok:
            print(f'[ReliefWeb] HTTP {res.status_code}')
            return []
        for item in res.json().get('data', []):
            try:
                f       = item.get('fields', {})
                title   = clean(f.get('title', ''))
                if not title: continue
                sources = f.get('source', [])
                company = sources[0].get('name', 'NGO') if sources else 'NGO'
                body    = clean(strip_html(f.get('body', '')))
                email   = extract_email(body)
                date    = f.get('date', {}).get('created', datetime.now().isoformat())
                jobs.append({
                    'id':          f"rw-{item.get('id', len(jobs))}",
                    'title':       title,
                    'company':     company,
                    'location':    extract_county(title+' '+body)+', Kenya',
                    'county':      extract_county(title+' '+body),
                    'type':        detect_type(title+' '+body),
                    'sector':      detect_sector(title),
                    'salary':      'Not stated',
                    'deadline':    '',
                    'link':        f.get('url', ''),
                    'apply_email': email,
                    'description': body[:2000],
                    'source':      'ReliefWeb',
                    'scraped_at':  date,
                })
            except: continue
        print(f'[ReliefWeb] ✅ {len(jobs)} jobs')
    except Exception as e:
        print(f'[ReliefWeb] ❌ {e}')
    return jobs


def scrape_remotive():
    """Remotive free API — remote jobs open to Kenya"""
    print('[Remotive] Fetching...')
    jobs = []
    try:
        res = requests.get('https://remotive.com/api/remote-jobs?limit=50', timeout=25)
        if not res.ok: return []
        for j in res.json().get('jobs', []):
            try:
                title = clean(j.get('title', ''))
                if not title: continue
                desc  = clean(strip_html(j.get('description', '')))
                jobs.append({
                    'id':          f"rem-{len(jobs)}",
                    'title':       title,
                    'company':     clean(j.get('company_name', '')),
                    'location':    'Remote / Online',
                    'county':      'Remote',
                    'type':        'Remote',
                    'sector':      detect_sector(title+' '+j.get('category','')),
                    'salary':      j.get('salary','') or 'Not stated',
                    'deadline':    '',
                    'link':        j.get('url', ''),
                    'apply_email': '',
                    'description': desc[:2000],
                    'source':      'Remotive (Remote)',
                    'scraped_at':  j.get('publication_date', datetime.now().isoformat()),
                })
            except: continue
        print(f'[Remotive] ✅ {len(jobs)} jobs')
    except Exception as e:
        print(f'[Remotive] ❌ {e}')
    return jobs


def parse_rss(name, url):
    """Parse RSS/Atom feed from any Kenyan job site"""
    print(f'[RSS] {name}...')
    jobs = []
    try:
        res = requests.get(url, timeout=25, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; JobsKenyaBot/1.0; +https://jobskenya.co.ke)'
        })
        if not res.ok:
            print(f'[RSS] {name} HTTP {res.status_code}')
            return []

        root  = ET.fromstring(res.content)
        ns    = {'atom': 'http://www.w3.org/2005/Atom'}
        items = root.findall('.//item') or root.findall('.//entry') or root.findall('.//atom:entry', ns)

        for item in items[:40]:
            try:
                def get(tag):
                    el = item.find(tag) or item.find(f'atom:{tag}', ns)
                    return clean(el.text or '') if el is not None and el.text else ''

                title = get('title')
                if not title or len(title) < 4: continue

                desc  = clean(strip_html(get('description') or get('summary') or get('content') or ''))
                link  = get('link')
                if not link:
                    el = item.find('link') or item.find('atom:link', ns)
                    link = el.get('href', '') if el is not None else ''

                email   = extract_email(desc)
                company = name

                # Extract company from title patterns
                for sep in [' at ', ' - ', ' | ']:
                    if sep in title:
                        parts   = title.split(sep, 1)
                        title   = parts[0].strip()
                        company = parts[1].strip()
                        break

                jobs.append({
                    'id':          f"{re.sub('[^a-z]','',name.lower())[:6]}-{len(jobs)}",
                    'title':       title,
                    'company':     company,
                    'location':    extract_county(title+' '+desc)+', Kenya',
                    'county':      extract_county(title+' '+desc),
                    'type':        detect_type(title+' '+desc),
                    'sector':      detect_sector(title+' '+desc),
                    'salary':      'Not stated',
                    'deadline':    '',
                    'link':        link,
                    'apply_email': email,
                    'description': desc[:2000],
                    'source':      name,
                    'scraped_at':  datetime.now().isoformat(),
                })
            except: continue

        print(f'[RSS] {name}: ✅ {len(jobs)} jobs')
    except Exception as e:
        print(f'[RSS] {name}: ❌ {e}')
    return jobs


RSS_SOURCES = [
    ('NGO Jobs Kenya',     'https://www.ngojobskenya.com/feed/'),
    ('Career Point Kenya', 'https://www.careerpointkenya.co.ke/feed/'),
    ('Jobs in Kenya',      'https://www.jobsinkenya.co.ke/feed/'),
    ('UN Jobs Nairobi',    'https://unjobs.org/duty_stations/nairobi/rss'),
    ('BrighterMonday',     'https://www.brightermonday.co.ke/rss/jobs'),
]


def run_all_scrapers():
    """Run all scrapers and save to KV"""
    print('='*50)
    print(f'🇰🇪 Scraping started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*50)

    all_jobs = []

    try: all_jobs.extend(scrape_reliefweb())
    except Exception as e: print(f'❌ ReliefWeb: {e}')

    try: all_jobs.extend(scrape_remotive())
    except Exception as e: print(f'❌ Remotive: {e}')

    for name, url in RSS_SOURCES:
        try: all_jobs.extend(parse_rss(name, url))
        except Exception as e: print(f'❌ {name}: {e}')

    before   = len(all_jobs)
    all_jobs = deduplicate(all_jobs)
    all_jobs.sort(key=lambda j: j.get('scraped_at', ''), reverse=True)
    print(f'🧹 {before} → {len(all_jobs)} unique jobs')

    result = save_jobs(all_jobs)
    print(f'✅ {len(all_jobs)} jobs saved to KV!')
    return result


def json_response(handler, data, status=200):
    body = json.dumps(data, ensure_ascii=False).encode()
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Access-Control-Allow-Origin', '*')
    handler.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    handler.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Token')
    handler.end_headers()
    handler.wfile.write(body)
