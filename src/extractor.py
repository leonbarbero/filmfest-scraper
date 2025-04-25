#!/usr/bin/env python3
# src/extractor.py

from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime, timezone
from dateutil.parser import parse
import re

def now_iso():
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()

def normalize_date(text):
    """Fuzzy-parse human date to ISO YYYY-MM-DD or return None."""
    try:
        return parse(text, fuzzy=True).date().isoformat()
    except:
        return None

# ———————————————————————————————————————————————————————
# Site-specific extractors

def extract_from_blog(html, url):
    """asianfilmfestivals.com style: <article class='post-archive'> listings."""
    soup = BeautifulSoup(html, 'html.parser')
    records = []
    for art in soup.select('div.main-post-list article.post-archive'):
        title_el = art.find(['h1','h2'])
        name = title_el.get_text(strip=True) if title_el else None

        p = art.find('p')
        text = p.get_text(" ", strip=True) if p else ""
        m = re.search(r'until\s+([A-Za-z0-9 ,]+)\s+\d{4}', text, flags=re.I)
        deadline = normalize_date(m.group(1)) if m else None

        date_el = art.find(string=re.compile(r'On\s+\w+', flags=re.I))
        article_date = normalize_date(date_el) if date_el else None

        if name and deadline:
            records.append({
                "name": name,
                "deadlines": [deadline],
                "opening_date": None,
                "article_date": article_date,
                "source_url": url,
                "extracted_at": now_iso()
            })
    return records

def extract_from_ffd(html, url):
    """filmfestivalsdeadlines.com table style: <table class='table'>."""
    soup = BeautifulSoup(html, 'html.parser')
    records = []
    table = soup.find('table', attrs={'class':'table','id':False})
    if not table:
        return records

    for row in table.find_all('tr')[1:]:
        cols = [td.get_text(" ", strip=True) for td in row.find_all('td')]
        if len(cols) < 3:
            continue
        name, open_date, deadline = cols[0], cols[1], cols[2]
        d_iso = normalize_date(deadline)
        if name and d_iso:
            records.append({
                "name": name,
                "opening_date": normalize_date(open_date),
                "deadlines": [d_iso],
                "source_url": url,
                "extracted_at": now_iso()
            })
    return records

def extract_from_filmfreeway(html, url):
    """
    FilmFreeway detail-page extractor: parses the 'Dates & Deadlines' sidebar.
    Assumes that the crawler has already queued and fetched the festival detail URLs.
    """
    soup = BeautifulSoup(html, 'html.parser')

    sidebar = soup.select_one('aside.sidebar--festival-submission-info')
    if not sidebar:
        return []

    dates_ul = sidebar.select_one('ul.ProfileFestival-datesDeadlines')
    if not dates_ul:
        return []

    items = []
    for li in dates_ul.select('li.ProfileFestival-datesDeadlines-dateGroup'):
        # date
        time_el = li.select_one('time.ProfileFestival-datesDeadlines-time')
        date_iso = None
        if time_el and time_el.has_attr('datetime'):
            date_iso = time_el['datetime']
        elif time_el:
            date_iso = normalize_date(time_el.get_text(strip=True))

        # label
        label_el = li.select_one('div.ProfileFestival-datesDeadlines-deadline')
        label = label_el.get_text(strip=True) if label_el else None

        if date_iso and label:
            items.append((date_iso, label))

    if not items:
        return []

    # Festival name
    name_el = soup.select_one('h1.ProfileFestival-profileTitle') or soup.find('h1')
    name = name_el.get_text(strip=True) if name_el else None

    return [{
        "name": name,
        "opening_date": next((d for d,l in items if 'open' in l.lower()), None),
        "deadlines": [d for d,l in items if 'deadline' in l.lower()],
        "source_url": url,
        "extracted_at": now_iso(),
        "all_date_items": [{"date": d, "label": l} for d,l in items]
    }]

def extract_generic(html, url):
    """Fallback: finds <h1>…Festival…</h1> + 'Deadline:' pattern in text."""
    soup = BeautifulSoup(html, 'html.parser')
    h1 = soup.find('h1', string=re.compile(r'Festival', flags=re.I))
    if not h1:
        return None

    name = h1.get_text(strip=True)
    text = soup.get_text(" ", strip=True)
    m = re.search(r'Deadline[:\-]\s*([A-Za-z0-9 ,\-]+)', text)
    d_iso = normalize_date(m.group(1)) if m else None

    return {
        "name": name,
        "opening_date": None,
        "deadlines": [d_iso] if d_iso else [],
        "source_url": url,
        "extracted_at": now_iso()
    }

# ———————————————————————————————————————————————————————
# Dispatcher

def extract_festival_info(html, url):
    """
    Dispatch to the correct site-specific extractor.
    Always returns a list of zero-or-more festival records.
    """
    domain = urlparse(url).netloc.lower()
    if "asianfilmfestivals.com" in domain:
        return extract_from_blog(html, url)
    if "filmfestivalsdeadlines.com" in domain:
        return extract_from_ffd(html, url)
    if "filmfreeway.com" in domain:
        return extract_from_filmfreeway(html, url)
    generic = extract_generic(html, url)
    return [generic] if generic else []

def smoke_test():
    print("  ▶ Running extractor.smoke_test()…")
    # Basic generic test
    sample = "<html><body><h1>Foo Film Festival</h1><p>Deadline: March 31, 2025</p></body></html>"
    recs = extract_festival_info(sample, "https://example.com/fake")
    assert isinstance(recs, list) and recs, "Expected at least one record"
    assert recs[0]["name"] == "Foo Film Festival"
    assert "2025-03-31" in recs[0]["deadlines"]
    print("  ✓ Extractor module smoke test passed")
