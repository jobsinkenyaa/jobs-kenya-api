"""
Shared helpers for Jobs Kenya Vercel API — uses Neon Postgres
"""
import re, json, os, requests
from datetime import datetime
import xml.etree.ElementTree as ET

# ── NEON POSTGRES CONNECTION ─────────────────────────────────────
# Vercel auto-adds POSTGRES_URL when you connect Neon
DATABASE_URL = os.getenv('POSTGRES_URL', '')

def get_conn():
    """Get a Postgres connection"""
    import psycopg2
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    """Create jobs table if it doesn't exist"""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS scraped_jobs (
                id          TEXT PRIMARY KEY,
                title       TEXT,
                company     TEXT,
                location    TEXT,
                county      TEXT,
                type        TEXT,
                sector      TEXT,
                salary      TEXT,
                deadline    TEXT,
                link        TEXT,
                apply_email TEXT,
                description TEXT,
                source      TEXT,
                scraped_at  TEXT
            );
            CREATE TABLE IF NOT EXISTS scraper_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        ''')
        conn.commit()
        cur.close()
        conn.close()
        print('DB initialized')
    except Exception as e:
        print(f'init_db error: {e}')

def save_jobs(jobs):
    """Save all jobs to Neon Postgres"""
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Clear old jobs
        cur.execute('DELETE FROM scraped_jobs')

        # Insert new jobs
        for j in jobs:
            cur.execute('''
                INSERT INTO scraped_jobs
                (id, title, company, location, county, type, sector,
                 salary, deadline, link, apply_email, description, source, scraped_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    title=EXCLUDED.title, company=EXCLUDED.company,
                    scraped_at=EXCLUDED.scraped_at
            ''', (
                j.get('id',''), j.get('title',''), j.get('company',''),
                j.get('location',''), j.get('county',''), j.get('type',''),
                j.get('sector',''), j.get('salary',''), j.get('deadline',''),
                j.get('link',''), j.get('apply_email',''),
                j.get('description','')[:2000], j.get('source',''),
                j.get('scraped_at','')
            ))

        # Save last run time
        now = datetime.now().isoformat()
        cur.execute('''
            INSERT INTO scraper_meta (key, value) VALUES ('last_run', %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        ''', (now,))
        cur.execute('''
            INSERT INTO scraper_meta (key, value) VALUES ('total_jobs', %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        ''', (str(len(jobs)),))

        conn.commit()
        cur.close()
        conn.close()
        print(f'✅ {len(jobs)} jobs saved to Neon Postgres')
        return {'total': len(jobs), 'scraped_at': now}
    except Exception as e:
        print(f'save_jobs error: {e}')
        return {'total': 0, 'scraped_at': datetime.now().isoformat()}

def load_jobs(county='', jtype='', keyword='', limit=80):
    """Load jobs from Neon Postgres with optional filters"""
    try:
        conn   = get_conn()
        cur    = conn.cursor()

        query  = 'SELECT id,title,company,location,county,type,sector,salary,deadline,link,apply_email,description,source,scraped_at FROM scraped_jobs WHERE 1=1'
        params = []

        if county:
            query += ' AND LOWER(county) LIKE %s'
            params.append(f'%{county.lower()}%')
        if jtype:
            query += ' AND LOWER(type) LIKE %s'
            params.append(f'%{jtype.lower()}%')
        if keyword:
            query += ' AND (LOWER(title) LIKE %s OR LOWER(company) LIKE %s)'
            params.extend([f'%{keyword.lower()}%', f'%{keyword.lower()}%'])

        query += ' ORDER BY scraped_at DESC LIMIT %s'
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cols = ['id','title','company','location','county','type','sector',
                'salary','deadline','link','apply_email','description','source','scraped_at']
        jobs = [dict(zip(cols, row)) for row in rows]

        # Get meta
        cur.execute("SELECT value FROM scraper_meta WHERE key='last_run'")
        row      = cur.fetchone()
        last_run = row[0] if row else None

        cur.close()
        conn.close()
        return {'total': len(jobs), 'scraped_at': last_run, 'jobs': jobs}
    except Exception as e:
        print(f'load_jobs error: {e}')
        return {'total': 0, 'scraped_at': None, 'jobs': []}

def get_status():
    """Get scraper status from DB"""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT value FROM scraper_meta WHERE key='last_run'")
        r1       = cur.fetchone()
        cur.execute("SELECT value FROM scraper_meta WHERE key='total_jobs'")
        r2       = cur.fetchone()
        cur.close()
        conn.close()
        return {
            'status':     'ok' if r1 else 'no_data',
            'total_jobs': int(r2[0]) if r2 else 0,
            'last_run':   r1[0] if r1 else None,
        }
    except Exception as e:
        return {'status': 'error', 'error': str(e), 'total_jobs': 0, 'last_run': None}


# ── TEXT HELPERS ─────────────────────────────────────────────────
def clean(t):
    return ' '.join((t or '').strip().split())

def strip_html(t):
    return re.sub(r'<[^>]+>', ' ', t or '').strip()

def extract_email(text):
    emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text or '')
    bad    = ['noreply','no-reply','donotreply','example','sentry','test@']
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
    if any(w in t for w in ['part-time','part time','casual']):         return 'Part-Time'
    if any(w in t for w in ['government','county','ministry','psc']):   return 'Government'
    if any(w in t for w in ['ngo','unicef','undp','oxfam','non-profit']):return 'NGO'
    if any(w in t for w in ['remote','work from home','wfh']):          return 'Remote'
    if any(w in t for w in ['contract','consultant','temporary']):       return 'Contract'
    return 'Full-Time'

def detect_sector(text):
    t = (text or '').lower()
    if any(w in t for w in ['software','developer','ict','data','cyber','tech']): return 'ICT & Technology'
    if any(w in t for w in ['nurse','doctor','medical','health','clinical']):     return 'Health & Medicine'
    if any(w in t for w in ['finance','account','audit','tax','banking']):        return 'Finance & Banking'
    if any(w in t for w in ['engineer','civil','mechanical','electrical']):       return 'Engineering'
    if any(w in t for w in ['teach','tutor','lecturer','school','education']):    return 'Education'
    if any(w in t for w in ['farm','agri','crop','livestock','food']):            return 'Agriculture'
    if any(w in t for w in ['market','sales','brand','advertis']):                return 'Marketing & Sales'
    if any(w in t for w in ['ngo','humanitarian','relief','programme']):          return 'NGO / Non-Profit'
    if any(w in t for w in ['legal','lawyer','advocate','compliance']):           return 'Legal'
    if any(w in t for w in ['driver','transport','logistics','supply']):          return 'Transport & Logistics'
    return 'General'

def deduplicate(jobs):
    seen, unique = set(), []
    for j in jobs:
        key = f"{j.get('title','').lower()[:40]}|{j.get('company','').lower()[:25]}"
        if key not in seen:
            seen.add(key)
            unique.append(j)
    return unique


# ── SCRAPERS ─────────────────────────────────────────────────────

def scrape_reliefweb():
    print('[ReliefWeb] Fetching NGO/UN jobs...')
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
        if not res.ok: return []
        for item in res.json().get('data', []):
            try:
                f       = item.get('fields', {})
                title   = clean(f.get('title', ''))
                if not title: continue
                sources = f.get('source', [])
                company = sources[0].get('name', 'NGO') if sources else 'NGO'
                body    = clean(strip_html(f.get('body', '')))
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
                    'apply_email': extract_email(body),
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
    print('[Remotive] Fetching remote jobs...')
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
    print(f'[RSS] {name}...')
    jobs = []
    try:
        res = requests.get(url, timeout=25, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; JobsKenyaBot/1.0)'
        })
        if not res.ok: return []
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
                desc    = clean(strip_html(get('description') or get('summary') or ''))
                link    = get('link')
                if not link:
                    el   = item.find('link') or item.find('atom:link', ns)
                    link = el.get('href','') if el is not None else ''
                company = name
                for sep in [' at ', ' - ', ' | ']:
                    if sep in title:
                        parts = title.split(sep, 1)
                        title   = parts[0].strip()
                        company = parts[1].strip()
                        break
                slug = re.sub(r'[^a-z]', '', name.lower())[:6]
                jobs.append({
                    'id':          f"{slug}-{len(jobs)}",
                    'title':       title,
                    'company':     company,
                    'location':    extract_county(title+' '+desc)+', Kenya',
                    'county':      extract_county(title+' '+desc),
                    'type':        detect_type(title+' '+desc),
                    'sector':      detect_sector(title+' '+desc),
                    'salary':      'Not stated',
                    'deadline':    '',
                    'link':        link,
                    'apply_email': extract_email(desc),
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
    """Run all scrapers and save results to Neon Postgres"""
    print('='*50)
    print(f'🇰🇪 Scraping: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*50)

    # Ensure table exists
    init_db()

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
    print(f'🧹 {before} → {len(all_jobs)} unique jobs')

    return save_jobs(all_jobs)


def json_response(handler, data, status=200):
    body = json.dumps(data, ensure_ascii=False, default=str).encode()
    handler.send_response(status)
    handler.send_header('Content-Type',                 'application/json')
    handler.send_header('Access-Control-Allow-Origin',  '*')
    handler.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    handler.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Token')
    handler.end_headers()
    handler.wfile.write(body)
