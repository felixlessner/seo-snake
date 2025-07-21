
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag, urlparse

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SEOChecker/1.0; +https://example.com/bot)"
}

def normalize_domain(domain: str) -> str:
    domain = domain.strip().lower()
    if not domain.startswith("http"):
        domain = "https://" + domain
    return domain.rstrip("/")

def strip_www(hostname: str) -> str:
    return hostname[4:] if hostname.startswith("www.") else hostname

def is_valid_link(href: str) -> bool:
    return not any(href.startswith(proto) for proto in ("mailto:", "tel:", "javascript:"))

async def resolve_redirect(url):
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=10, allow_redirects=True, ssl=False) as resp:
                return str(resp.url)
    except Exception:
        return url

async def fetch(session, url):
    try:
        async with session.get(url, timeout=20, ssl=False) as response:
            if response.status == 200 and "text/html" in response.headers.get("Content-Type", ""):
                return await response.text()
    except Exception:
        return None

async def crawl_domain(start_input, max_urls=100):
    original_start = normalize_domain(start_input)
    start_url = await resolve_redirect(original_start)
    parsed_start = urlparse(start_url)
    base_netloc = strip_www(parsed_start.netloc)
    exact_netloc = parsed_start.netloc
    domain_root = f"{parsed_start.scheme}://{parsed_start.netloc}"
    visited = set()
    to_visit = {domain_root}
    all_internal_urls = set()

    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn) as session:
        while to_visit and len(visited) < max_urls:
            url = to_visit.pop()
            if url in visited:
                continue
            visited.add(url)
            html = await fetch(session, url)
            if not html:
                continue
            parsed_url = urlparse(url)
            if parsed_url.netloc == exact_netloc:
                all_internal_urls.add(url)
            soup = BeautifulSoup(html, "lxml")
            for tag in soup.find_all("a", href=True):
                href = tag.get("href")
                if not is_valid_link(href):
                    continue
                full_url = urljoin(url, href)
                full_url = urldefrag(full_url)[0].rstrip("/")
                parsed = urlparse(full_url)
                if strip_www(parsed.netloc) == base_netloc and full_url not in visited and len(visited) + len(to_visit) < max_urls:
                    to_visit.add(full_url)
    return sorted(all_internal_urls)
