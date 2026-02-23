import os
import requests

API_KEY = os.environ.get('SERPAPI_API_KEY')


def serpapi_search(query: str, num=10):
    """Use SerpAPI to get organic result URLs.

    Requires environment variable `SERPAPI_API_KEY` to be set.
    Returns list of URLs (may be empty).
    """
    if not API_KEY:
        return []
    params = {
        'engine': 'google',
        'q': query,
        'api_key': API_KEY,
        'num': num,
    }
    try:
        resp = requests.get('https://serpapi.com/search', params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for r in data.get('organic_results', []):
            link = r.get('link') or r.get('url')
            if link:
                results.append(link)
            if len(results) >= num:
                break
        return results
    except Exception:
        return []
