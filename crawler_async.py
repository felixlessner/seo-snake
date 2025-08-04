import asyncio, aiohttp, pandas as pd, ssl, urllib.parse, re, time, argparse
from bs4 import BeautifulSoup
from typing import List, Callable, Awaitable

TIMEOUT = aiohttp.ClientTimeout(total=25)
UNSAFE_SSL = ssl.create_default_context()
UNSAFE_SSL.check_hostname = False
UNSAFE_SSL.verify_mode = ssl.CERT_NONE

# Erlaubte externe Domains zusätzlich zu internen (inkl. Subdomains)
ALLOWED_EXTERNALS = {
    "berendsohn-digitalservice.de",
    "berendsohn-digital.de",
}

# Helper zum Normieren der Domain (www. ignorieren)
def normalize_netloc(netloc: str) -> str:
    netloc = netloc.lower()
    if netloc.startswith("www."):
        return netloc[4:]
    return netloc

def is_allowed_external(link_norm: str, base_norm: str) -> bool:
    if link_norm == base_norm:
        return True
    for allowed in ALLOWED_EXTERNALS:
        if link_norm == allowed or link_norm.endswith("." + allowed):
            return True
    return False

def strip_html_for_wc(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    if soup.head:
        soup.head.decompose()
    for t in soup(["script", "style", "noscript", "template"]):
        t.decompose()
    return re.sub(r"\s+", " ", soup.get_text(" ")).strip()

def word_count(html: str) -> int:
    return len(strip_html_for_wc(html).split())

def detect_cms(html: str, headers, url: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    gen = soup.find("meta", attrs={"name": "generator"})
    if gen and gen.get("content"):
        return gen["content"].split()[0]
    for pat, name in [
        ("wp-content|wp-includes", "WordPress"),
        ("/administrator/", "Joomla"),
        ("/sites/default/", "Drupal"),
        ("/typo3conf/", "TYPO3"),
        ("cdn.shopify.com", "Shopify"),
    ]:
        if re.search(pat, html, re.I) or re.search(pat, url, re.I):
            return name
    return "Unbekannt"

def parse_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    meta = soup.find("meta", attrs={"name": "description"})
    meta_desc = meta.get("content", "").strip() if meta else ""
    h1 = soup.find("h1")
    h1_txt = h1.get_text(separator=" ", strip=True) if h1 else ""
    wc = word_count(html)
    return title, meta_desc, h1_txt, wc

async def fetch(session: aiohttp.ClientSession, url: str, retries=3):
    last_exc = None
    for attempt in range(retries):
        try:
            async with session.get(url, allow_redirects=True) as r:
                text = await r.text()
                return r.status, text, r.headers
        except Exception as e:
            last_exc = e
            await asyncio.sleep(1.5 * (attempt + 1))
    raise last_exc

async def check_robots(session, page_url):
    p = urllib.parse.urlparse(page_url)
    robots_url = f"{p.scheme}://{p.netloc}/robots.txt"
    try:
        _, txt, _ = await fetch(session, robots_url, retries=2)
    except Exception:
        return "robots.txt error"
    if txt.lower().startswith("404"):
        return "robots.txt not found"

    ua, allow, dis = False, [], []
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("user-agent"):
            ua = "*" in line.split(":", 1)[1]
            continue
        if not ua:
            continue
        d, _, val = line.partition(":")
        d, val = d.lower().strip(), val.strip()
        if d == "disallow" and val:
            dis.append(val)
        elif d == "allow":
            allow.append(val)

    path = urllib.parse.unquote(p.path or "/")
    best_a = max((x for x in allow if path.startswith(x)), default="", key=len)
    best_d = max((x for x in dis if path.startswith(x)), default="", key=len)
    return "Allowed" if len(best_a) >= len(best_d) else "Disallowed"

def check_noindex(html: str, headers) -> str:
    if "X-Robots-Tag" in headers and "noindex" in headers["X-Robots-Tag"].lower():
        return "NOINDEX via Header"
    soup = BeautifulSoup(html, "lxml")
    meta = soup.find("meta", attrs={"name": "robots"})
    if meta and "noindex" in (meta.get("content") or "").lower():
        return "NOINDEX via Meta"
    return "Indexable"

async def check_link(session, link):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        )
    }

    try:
        async with session.head(link, allow_redirects=True, timeout=10, headers=headers) as resp:
            if 200 <= resp.status < 400 or resp.status == 429:
                return None
            else:
                return link
    except Exception:
        # Fallback auf GET
        try:
            async with session.get(link, allow_redirects=True, timeout=10, headers=headers) as resp:
                if 200 <= resp.status < 400 or resp.status == 429:
                    return None
                else:
                    return link
        except Exception:
            return link

async def find_broken_links(html: str, base_url: str, session) -> str:
    soup = BeautifulSoup(html, "lxml")
    links_with_text = {}

    base_parsed = urllib.parse.urlparse(base_url)
    base_norm = normalize_netloc(base_parsed.netloc)

    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("#"):
            continue

        full_link = urllib.parse.urljoin(base_url, href)
        parsed = urllib.parse.urlparse(full_link)
        link_norm = normalize_netloc(parsed.netloc)

        # Nur interne Links oder erlaubte externe Domains inkl. Subdomains prüfen
        if not is_allowed_external(link_norm, base_norm):
            continue

        anchor = tag.get_text(strip=True)
        links_with_text[full_link] = anchor

    if not links_with_text:
        return 0

    # parallele Prüfung
    tasks = [check_link(session, link) for link in links_with_text]
    results = await asyncio.gather(*tasks)

    broken = []
    for link, result in zip(links_with_text.keys(), results):
        if result:
            anchor_display = f'"{links_with_text[link]}"' if links_with_text[link] else "[kein Text]"
            broken.append(f"{link} (Text: {anchor_display})")

    if not broken:
        return 0
    return ", ".join(broken)

async def worker(url: str, session, sem, progress_cb=None):
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    async with sem:
        if progress_cb:
            progress_cb("fetch", url)
        try:
            status_code, html, headers = await fetch(session, url)
        except Exception as e:
            return {"URL": url, "Status": f"Error: {e}"}

        seo_status = check_noindex(html, headers)
        title, meta_desc, h1, wc = parse_page(html)
        robots = await check_robots(session, url)
        cms = detect_cms(html, headers, url)
        broken_links = await find_broken_links(html, url, session)

        return {
            "URL": url,
            "Status": seo_status,
            "Robots Policy": robots,
            "Title": title,
            "Meta Description": meta_desc,
            "H1": h1,
            "Wörter": wc,
            "CMS": cms,
            "Broken Links": broken_links,
        }

async def crawl(
    urls: List[str],
    concurrency: int = 20,
    progress_cb: Callable[[str, str], Awaitable[None]] | None = None,
) -> pd.DataFrame:
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=UNSAFE_SSL)
    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as sess:
        tasks = [worker(u, sess, sem, progress_cb) for u in urls]
        results = await asyncio.gather(*tasks)
    return pd.DataFrame(results)

if __name__ == "__main__":
    ap = argparse.ArgumentParser("Asynchroner SEO-Crawler")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--concurrency", type=int, default=20, help="gleichzeitige Requests")
    args = ap.parse_args()

    urls = [u.strip() for u in open(args.input).read().splitlines() if u.strip()]
    df = asyncio.run(crawl(urls, concurrency=args.concurrency))
    df.to_csv(args.output, index=False)
    print(df.head())
