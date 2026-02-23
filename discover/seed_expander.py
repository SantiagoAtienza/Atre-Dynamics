import unicodedata
from urllib.parse import urljoin, urlparse

from config.keywords import BUSINESS_TERMS, KEYWORDS

COMMON_PATHS = [
    "/",
    "/contact",
    "/contacto",
    "/contact-us",
    "/servicios",
    "/servicio",
    "/productos",
    "/paneles",
    "/paneles-electricos",
    "/cuadros-electricos",
    "/proyectos",
    "/about",
    "/nosotros",
    "/empresa",
    "/portfolio",
    "/clientes",
    "/casos",
    "/news",
    "/noticias",
    "/blog",
    "/opiniones",
    "/resenas",
]


def _slugify(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    slug = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = slug.lower().strip()
    return slug.replace(" ", "-").replace("/", "-")


def expand_site_to_urls(site_url: str, max_per_site=50):
    parsed = urlparse(site_url)
    if not parsed.scheme:
        base = f"https://{site_url}"
    else:
        base = f"{parsed.scheme}://{parsed.netloc}"

    candidates = [base]
    for path in COMMON_PATHS:
        candidates.append(urljoin(base, path))

    for term in KEYWORDS + BUSINESS_TERMS:
        slug = _slugify(term)
        candidates.append(urljoin(base, f"/{slug}"))
        candidates.append(urljoin(base, f"/servicios/{slug}"))
        candidates.append(urljoin(base, f"/proyectos/{slug}"))
        candidates.append(urljoin(base, f"/blog/{slug}"))
        candidates.append(urljoin(base, f"/casos/{slug}"))

    unique_candidates = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
        if len(unique_candidates) >= max_per_site:
            break
    return unique_candidates
