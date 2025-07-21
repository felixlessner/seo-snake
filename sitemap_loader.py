#!/usr/bin/env python3
"""Sitemap-Loader mit robustem Error‑Handling & HTML‑Filter"""

import requests, gzip, urllib.parse, time
from bs4 import BeautifulSoup
from typing import List

NON_HTML_EXT = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".mp3", ".mp4", ".avi",
    ".mov", ".wmv", ".webm", ".ico", ".rss"
}

HEADERS = {
    "User-Agent": "NoIndexCheckerBot/1.0 (+https://example.com/bot-info)"
}

def is_html_url(url: str) -> bool:
    path = urllib.parse.urlparse(url).path.lower()
    if not path or path.endswith(("/", "#")):
        return True  # wahrscheinlich HTML-Dokument ohne Endung
    ext = "." + path.rsplit(".", 1)[-1] if "." in path else ""
    return ext in {"", ".html", ".htm", ".php", ".asp", ".aspx"}

def _fetch_url(url: str, retries: int = 3, backoff: float = 1.5) -> bytes:
    """Lädt URL mit Retry bei ConnectionReset/Timeout."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=15, headers=HEADERS, stream=True)
            resp.raise_for_status()
            return resp.content
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
                continue
            raise e  # nach max Retries Fehler weitergeben

def load_sitemap(url: str) -> List[str]:
    """Gibt reine HTML‑Links aus Sitemap zurück. Fehlertolerant."""
    try:
        raw = _fetch_url(url)
    except Exception as e:
        print(f"Fehler beim Abrufen der Sitemap: {e}")
        return []

    content = gzip.decompress(raw) if url.endswith(".gz") else raw
    soup = BeautifulSoup(content, "xml")
    urls = [loc.text.strip() for loc in soup.find_all("loc")]
    return [u for u in urls if is_html_url(u)]

if __name__ == "__main__":
    import sys, json
    print(json.dumps(load_sitemap(sys.argv[1]), indent=2))