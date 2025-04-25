# src/parser.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def extract_links(html_content, base_url):
    """
    Extract all same-domain HTTP(S) links from html_content,
    resolving relative URLs against base_url.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    base_domain = urlparse(base_url).netloc

    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if href.startswith('#') or href.startswith('mailto:') or href.startswith('javascript:'):
            continue
        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)
        if parsed.scheme in ('http', 'https') and parsed.netloc == base_domain:
            links.add(abs_url)
    return list(links)

def find_next_page(html_content, base_url):
    """
    Identify a pagination “next” link via rel="next" or text heuristics.
    Returns the absolute URL or None.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # First, try rel="next"
    link = soup.find('a', rel='next')
    if link and link.get('href'):
        return urljoin(base_url, link['href'].strip())

    # Then common “next” texts
    for token in ('next', '>', '»', 'próximo', 'seguinte'):
        candidate = soup.find('a', string=lambda t: t and token in t.lower())
        if candidate and candidate.get('href'):
            return urljoin(base_url, candidate['href'].strip())

    return None

def smoke_test():
    """
    Quick inline test: link extraction + next-page detection.
    """
    html = """
    <html><body>
      <a href="/page1.html">Page 1</a>
      <a href="http://other.com/page2.html">External</a>
      <a href="/page3.html">Next</a>
      <a rel="next" href="/page4.html">Next Page</a>
    </body></html>
    """
    base = 'http://example.com/index.html'
    links = extract_links(html, base)
    assert 'http://example.com/page1.html' in links, "page1.html missing"
    assert 'http://example.com/page3.html' in links, "page3.html missing"
    assert all('other.com' not in url for url in links), "External links leaked"

    next_url = find_next_page(html, base)
    assert next_url == 'http://example.com/page4.html', f"Expected page4, got {next_url}"

    print("  ✓ Parser module smoke test passed")
