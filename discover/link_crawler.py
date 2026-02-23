import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import random

HEADERS = {"User-Agent": "Mozilla/5.0 (LeadRadar/1.0)"}


def crawl_links(start_url: str, max_pages=30, delay=(0.5, 1.5)):
    """Crawl links from a starting URL, returning discovered pages.

    Returns a dict: {url: {'text': ..., 'path': path_segment}}
    Also identifies candidate pages by path keywords like contact, servicios, proyectos.
    """
    to_visit = [start_url]
    visited = set()
    results = {}

    start_domain = urlparse(start_url).netloc
    keywords = ["contact", "contacto", "servicio", "servicios", "proyecto", "proyectos", "cliente", "clientes", "about", "nosotros", "portfolio", "proyectos"]

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            html = resp.text
            visited.add(url)
            soup = BeautifulSoup(html, 'lxml')
            text = ' '.join([t.get_text(strip=True) for t in soup.find_all(['p','h1','h2','h3','li']) if t.get_text(strip=True)])
            path = urlparse(url).path.lower()
            results[url] = {'text': text, 'path': path}

            # find links
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith('#') or href.startswith('mailto:') or href.startswith('javascript:'):
                    continue
                full = urljoin(url, href)
                p = urlparse(full)
                if p.netloc != start_domain:
                    continue
                if full not in visited and full not in to_visit:
                    to_visit.append(full)

        except Exception:
            visited.add(url)
        time.sleep(random.uniform(delay[0], delay[1]))

    # identify candidate pages
    candidates = []
    for u, meta in results.items():
        if any(k in meta['path'] for k in keywords) or any(k in meta['text'].lower() for k in keywords):
            candidates.append(u)

    return {'pages': list(results.keys()), 'candidates': candidates}
