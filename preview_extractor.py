#!/usr/bin/env python3
# preview_extractor.py

import argparse
import json
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
import re

def now_iso():
    return datetime.now(timezone.utc).isoformat() + "Z"

def normalize_date(text):
    try:
        return parse(text, fuzzy=True).date().isoformat()
    except:
        return None

# ———————————————————————————————————————————————————————
# Site‐specific extractors

def extract_from_blog(html, url):
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
                "article_date": article_date,
                "source_url": url,
                "extracted_at": now_iso()
            })
    return records

def extract_from_ffd(html, url):
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
    soup = BeautifulSoup(html, 'html.parser')
    records = []
    for card in soup.select('article.BrowseFestivalsCard'):
        a = card.select_one('a.BrowseFestivalsLink')
        name = a.get_text(strip=True) if a else None

        deadline_el = card.find(string=re.compile(r'Deadline', flags=re.I))
        d_iso = None
        if deadline_el:
            m = re.search(r':\s*([A-Za-z0-9 ,]+)', deadline_el)
            if m:
                d_iso = normalize_date(m.group(1))

        loc_el = card.select_one('div.GridCell-5 > div')
        location = loc_el.get_text(strip=True) if loc_el else None

        if name and d_iso:
            records.append({
                "name": name,
                "deadlines": [d_iso],
                "location": location,
                "source_url": url,
                "extracted_at": now_iso()
            })
    return records

def extract_generic(html, url):
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
        "deadlines": [d_iso] if d_iso else [],
        "source_url": url,
        "extracted_at": now_iso()
    }

def extract_for_url(html, url):
    domain = urlparse(url).netloc.lower()
    if "asianfilmfestivals.com" in domain:
        return extract_from_blog(html, url)
    if "filmfestivalsdeadlines.com" in domain:
        return extract_from_ffd(html, url)
    if "filmfreeway.com" in domain:
        return extract_from_filmfreeway(html, url)
    gen = extract_generic(html, url)
    return [gen] if gen else []

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--urls',   required=True, help='File with one URL per line')
    p.add_argument('--output', required=True, help='Write JSONL here')
    args = p.parse_args()

    with open(args.urls) as f, open(args.output, 'w', encoding='utf-8') as out:
        for line in f:
            url = line.strip()
            if not url or url.startswith('#'):
                continue

            print(f"Fetching {url}…", file=sys.stderr)
            try:
                r = requests.get(url, 
                    timeout=15, 
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/113.0.0.0 Safari/537.36"
                        )
                    }
                )
                r.raise_for_status()
                recs = extract_for_url(r.text, url)
                for rec in recs:
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"  ⚠️ Error {e}", file=sys.stderr)

    print("Done.", file=sys.stderr)

if __name__ == '__main__':
    main()