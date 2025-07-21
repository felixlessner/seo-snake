#!/usr/bin/env python3
"""
Asynchroner SEO-Crawler – optimiert für große URL-Mengen
--------------------------------------------------------
Spalten:
• URL • HTTP Status • Status (noindex) • Robots Policy • Title • Meta Description
• H1 • Wörter • CMS • Hinweis
Concurrency per CLI oder Funktions­parameter einstellbar.
"""

import asyncio, aiohttp, pandas as pd, ssl, urllib.parse, re, time, argparse
from bs4 import BeautifulSoup
from typing import List, Callable, Awaitable

TIMEOUT = aiohttp.ClientTimeout(total=25)
UNSAFE_SSL = ssl.create_default_context()
UNSAFE_SSL.check_hostname = False
UNSAFE_SSL.verify_mode = ssl.CERT_NONE

# ---------- Helpers ---------------------------------------------------------
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
    h1_txt = h1.get_text(strip=True) if h1 else ""
    wc = word_count(html)
    return title, meta_desc, h1_txt, wc

# ---------- Core ------------------------------------------------------------
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
    best_d = max((x for x in dis if path.startswith(x)),   default="", key=len)
    return "Allowed" if len(best_a) >= len(best_d) else "Disallowed"

def check_noindex(html: str, headers) -> str:
    if "X-Robots-Tag" in headers and "noindex" in headers["X-Robots-Tag"].lower():
        return "NOINDEX via Header"
    soup = BeautifulSoup(html, "lxml")
    meta = soup.find("meta", attrs={"name": "robots"})
    if meta and "noindex" in (meta.get("content") or "").lower():
        return "NOINDEX via Meta"
    return "Indexable"

# ---------- Worker ----------------------------------------------------------
async def worker(url: str, session, sem, progress_cb=None):
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    async with sem:
        if progress_cb:
            progress_cb("fetch", url)
        try:
            status_code, html, headers = await fetch(session, url)
        except Exception as e:
            return {"URL": url, "HTTP Status": "-", "Status": f"Error: {e}"}

        seo_status = check_noindex(html, headers)
        title, meta_desc, h1, wc = parse_page(html)
        robots = await check_robots(session, url)
        cms = detect_cms(html, headers, url)

        return {
            "URL": url,
            "HTTP Status": status_code,
            "Status": seo_status,
            "Robots Policy": robots,
            "Title": title,
            "Meta Description": meta_desc,
            "H1": h1,
            "Wörter": wc,
            "CMS": cms,
        }

# ---------- Public crawl() --------------------------------------------------
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

# ---------- CLI -------------------------------------------------------------
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
