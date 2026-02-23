import os


def _as_bool(value: str | None):
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


# Defaults for large crawls
CONCURRENCY = int(os.environ.get('LR_CONCURRENCY', '8'))
PAGES_PER_SITE = int(os.environ.get('LR_PAGES_PER_SITE', '15'))
MAX_SITES = int(os.environ.get('LR_MAX_SITES', '1000'))

# Optional integrations
USE_SERPAPI = _as_bool(os.environ.get('LR_USE_SERPAPI', 'true')) and bool(os.environ.get('SERPAPI_API_KEY'))
USE_OPENAI = _as_bool(os.environ.get('LR_USE_OPENAI', 'true')) and bool(os.environ.get('OPENAI_API_KEY'))
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-4o-mini')

# Crawling behavior
REQUEST_TIMEOUT = int(os.environ.get('LR_REQUEST_TIMEOUT', '12'))
DELAY_MIN = float(os.environ.get('LR_DELAY_MIN', '0.5'))
DELAY_MAX = float(os.environ.get('LR_DELAY_MAX', '1.5'))
LR_PROXY = os.environ.get('LR_PROXY')

# Discovery behavior
DISCOVERY_MAX_QUERIES = int(os.environ.get('LR_DISCOVERY_MAX_QUERIES', '40'))
DISCOVERY_CONNECT_TIMEOUT = float(os.environ.get('LR_DISCOVERY_CONNECT_TIMEOUT', '4'))
DISCOVERY_READ_TIMEOUT = float(os.environ.get('LR_DISCOVERY_READ_TIMEOUT', '8'))
DISCOVERY_PER_SOURCE = int(os.environ.get('LR_DISCOVERY_PER_SOURCE', '12'))
DISCOVERY_DEEP_CRAWL_PAGES = int(os.environ.get('LR_DISCOVERY_DEEP_CRAWL_PAGES', '24'))
DISCOVERY_LINK_EXPANSION_DEPTH = int(os.environ.get('LR_DISCOVERY_LINK_EXPANSION_DEPTH', '2'))
DISCOVERY_LINKS_PER_PAGE = int(os.environ.get('LR_DISCOVERY_LINKS_PER_PAGE', '24'))
DISCOVERY_MAX_SEARCH_BUDGET = int(os.environ.get('LR_DISCOVERY_MAX_SEARCH_BUDGET', '500'))
TARGET_MIN_LEADS = int(os.environ.get('LR_TARGET_MIN_LEADS', '1'))

# AI classification behavior
AI_TEXT_LIMIT = int(os.environ.get('LR_AI_TEXT_LIMIT', '4000'))
MIN_NEED_SCORE = int(os.environ.get('LR_MIN_NEED_SCORE', '45'))
SUPPLIER_MAX_PROB = float(os.environ.get('LR_SUPPLIER_MAX_PROB', '0.45'))
