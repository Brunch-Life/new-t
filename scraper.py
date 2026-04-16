#!/usr/bin/env python3
"""
Web scraper for https://new-t.github.io/
Continuously scrapes content, structure, and all images (including external ones like risu.io).
"""

import os
import re
import sys
import json
import time
import hashlib
import logging
import argparse
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://new-t.github.io/"
DEFAULT_INTERVAL = 300  # seconds between scrape cycles
OUTPUT_DIR = Path("scraped_data")
IMAGES_DIR = OUTPUT_DIR / "images"
PAGES_DIR = OUTPUT_DIR / "pages"
HISTORY_DIR = OUTPUT_DIR / "history"
SNAPSHOT_FILE = OUTPUT_DIR / "latest_snapshot.json"
CHANGE_LOG = OUTPUT_DIR / "changelog.json"

# Domains from which we also download images
IMAGE_DOMAINS_ALLOWLIST = [
    "new-t.github.io",
    "risu.io",
    "raw.githubusercontent.com",
    "githubusercontent.com",
    "i.imgur.com",
    "imgur.com",
    "cdn.discordapp.com",
    "media.discordapp.net",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,ja;q=0.6",
}

REQUEST_TIMEOUT = 30
IMAGE_TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_dirs():
    for d in [OUTPUT_DIR, IMAGES_DIR, PAGES_DIR, HISTORY_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_filename(url: str) -> str:
    """Convert a URL into a safe local filename while keeping the extension."""
    parsed = urlparse(url)
    path = unquote(parsed.path).strip("/")
    if not path:
        path = "index"
    # Replace path separators and special chars
    safe = re.sub(r'[^\w.\-]', '_', path)
    # Truncate if too long
    if len(safe) > 180:
        safe = safe[:150] + "_" + sha256(url.encode())[:16]
    return safe


def image_local_path(url: str) -> Path:
    """Determine local path for an image based on its URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.replace(":", "_")
    domain_dir = IMAGES_DIR / domain
    domain_dir.mkdir(parents=True, exist_ok=True)
    fname = safe_filename(url)
    return domain_dir / fname


def is_allowed_image_domain(url: str) -> bool:
    """Check whether we should download images from this domain."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    # Always allow relative URLs (same domain)
    if not host:
        return True
    return any(host == d or host.endswith("." + d) for d in IMAGE_DOMAINS_ALLOWLIST)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Session builder
# ---------------------------------------------------------------------------

def build_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)

    # Try multiple authentication approaches
    # GitHub Pages private repos sometimes use a cookie or query param
    s.cookies.set("token", token)
    s.cookies.set("access_token", token)
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["X-Access-Token"] = token

    return s


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_page(session: requests.Session, url: str, token: str) -> requests.Response | None:
    """Fetch a page, trying multiple auth strategies."""
    strategies = [
        ("cookies+bearer (session default)", lambda u: session.get(u, timeout=REQUEST_TIMEOUT)),
        ("?token= query param",              lambda u: session.get(u, params={"token": token}, timeout=REQUEST_TIMEOUT)),
        ("?access_token= query param",       lambda u: session.get(u, params={"access_token": token}, timeout=REQUEST_TIMEOUT)),
        ("X-Auth-Token header",              lambda u: session.get(u, headers={"X-Auth-Token": token}, timeout=REQUEST_TIMEOUT)),
        ("HTTP Basic auth",                  lambda u: session.get(u, auth=("", token), timeout=REQUEST_TIMEOUT)),
    ]

    last_status = None
    last_body = ""
    for name, strategy in strategies:
        try:
            resp = strategy(url)
            last_status = resp.status_code
            last_body = resp.text[:200]
            if resp.status_code == 200:
                log.info(f"Auth OK ({name}): {url}")
                return resp
            log.debug(f"Auth [{name}] -> {resp.status_code}: {resp.text[:120]}")
        except requests.RequestException as e:
            log.debug(f"Auth [{name}] error: {e}")

    # Last resort: try without any auth
    try:
        clean_session = requests.Session()
        clean_session.headers.update(HEADERS)
        resp = clean_session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            log.info(f"Unauthenticated request succeeded for {url}")
            return resp
        last_status = resp.status_code
        last_body = resp.text[:200]
    except requests.RequestException as e:
        log.debug(f"Unauthenticated request failed: {e}")

    log.error(f"All auth strategies failed for {url} (last status={last_status}, body={last_body!r})")
    return None


def download_image(session: requests.Session, url: str, token: str) -> Path | None:
    """Download an image and save it locally. Returns local path or None."""
    local = image_local_path(url)
    try:
        # For same-domain images, use the session with auth
        parsed = urlparse(url)
        if "new-t.github.io" in parsed.netloc or not parsed.netloc:
            resp = fetch_page(session, url, token)
        else:
            # External images usually don't need auth
            resp = requests.get(url, headers=HEADERS, timeout=IMAGE_TIMEOUT)
            if resp.status_code != 200:
                log.warning(f"Image download failed ({resp.status_code}): {url}")
                return None

        if resp is None:
            return None

        content = resp.content
        if not content or len(content) < 100:
            log.warning(f"Image too small or empty: {url}")
            return None

        # Ensure the file has a proper extension
        content_type = resp.headers.get("Content-Type", "")
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) if content_type else None
        if ext and not str(local).lower().endswith(ext):
            # Only add extension if file doesn't already have one that makes sense
            current_ext = local.suffix.lower()
            image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico", ".avif"}
            if current_ext not in image_exts:
                local = local.with_suffix(ext)

        local.write_bytes(content)
        log.info(f"Saved image: {url} -> {local}")
        return local

    except Exception as e:
        log.error(f"Error downloading image {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------

def extract_all_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Extract all internal page links."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        # Only follow links within the same domain
        if "new-t.github.io" in parsed.netloc:
            # Remove fragment
            clean = parsed._replace(fragment="").geturl()
            links.add(clean)
    return sorted(links)


def extract_image_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Extract all image URLs from the page."""
    urls = set()

    # <img> tags
    for img in soup.find_all("img", src=True):
        urls.add(urljoin(base_url, img["src"]))
    for img in soup.find_all("img", attrs={"data-src": True}):
        urls.add(urljoin(base_url, img["data-src"]))

    # <source> tags (picture elements)
    for source in soup.find_all("source", srcset=True):
        for part in source["srcset"].split(","):
            src = part.strip().split()[0]
            if src:
                urls.add(urljoin(base_url, src))

    # CSS background-image
    for tag in soup.find_all(style=True):
        style = tag["style"]
        for match in re.findall(r'url\(["\']?(.*?)["\']?\)', style):
            urls.add(urljoin(base_url, match))

    # <style> blocks
    for style_tag in soup.find_all("style"):
        if style_tag.string:
            for match in re.findall(r'url\(["\']?(.*?)["\']?\)', style_tag.string):
                urls.add(urljoin(base_url, match))

    # <link> with image rels (favicons, icons, etc.)
    for link in soup.find_all("link", href=True):
        rel = " ".join(link.get("rel", []))
        if "icon" in rel or "image" in rel or "apple-touch" in rel:
            urls.add(urljoin(base_url, link["href"]))

    # Open Graph / Twitter card images
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "") or meta.get("name", "")
        content = meta.get("content", "")
        if content and ("image" in prop.lower() or "og:image" in prop.lower()):
            urls.add(urljoin(base_url, content))

    # Inline style attributes on any element
    for tag in soup.find_all(True):
        style = tag.get("style", "")
        if "url(" in style:
            for match in re.findall(r'url\(["\']?(.*?)["\']?\)', style):
                if match and not match.startswith("data:"):
                    urls.add(urljoin(base_url, match))

    # Filter out data URIs
    urls = {u for u in urls if not u.startswith("data:")}

    return sorted(urls)


def extract_text_content(soup: BeautifulSoup) -> str:
    """Extract clean text content from the page."""
    # Remove script and style elements
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def extract_structured_content(soup: BeautifulSoup, base_url: str) -> dict:
    """Extract structured content preserving hierarchy."""
    data = {
        "title": "",
        "meta": {},
        "headings": [],
        "paragraphs": [],
        "links": [],
        "images": [],
        "lists": [],
        "tables": [],
    }

    # Title
    title_tag = soup.find("title")
    if title_tag:
        data["title"] = title_tag.get_text(strip=True)

    # Meta tags
    for meta in soup.find_all("meta"):
        name = meta.get("name") or meta.get("property") or ""
        content = meta.get("content", "")
        if name and content:
            data["meta"][name] = content

    # Headings
    for level in range(1, 7):
        for h in soup.find_all(f"h{level}"):
            data["headings"].append({
                "level": level,
                "text": h.get_text(strip=True),
            })

    # Paragraphs
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if text:
            data["paragraphs"].append(text)

    # Links
    for a in soup.find_all("a", href=True):
        data["links"].append({
            "text": a.get_text(strip=True),
            "href": urljoin(base_url, a["href"]),
        })

    # Images
    for img in soup.find_all("img"):
        data["images"].append({
            "src": urljoin(base_url, img.get("src", "")),
            "alt": img.get("alt", ""),
            "title": img.get("title", ""),
        })

    # Lists
    for ul in soup.find_all(["ul", "ol"]):
        items = [li.get_text(strip=True) for li in ul.find_all("li", recursive=False)]
        if items:
            data["lists"].append({
                "type": ul.name,
                "items": items,
            })

    # Tables
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)
        if rows:
            data["tables"].append(rows)

    return data


# ---------------------------------------------------------------------------
# Core scraping logic
# ---------------------------------------------------------------------------

def scrape_page(session: requests.Session, url: str, token: str,
                visited: set, all_images: dict, depth: int = 0, max_depth: int = 5) -> dict | None:
    """Scrape a single page and return its data."""
    if url in visited or depth > max_depth:
        return None
    visited.add(url)

    log.info(f"Scraping: {url} (depth={depth})")
    resp = fetch_page(session, url, token)
    if resp is None:
        log.error(f"Failed to fetch: {url}")
        return None

    html = resp.text
    content_hash = sha256(html.encode())

    soup = BeautifulSoup(html, "lxml")

    # Extract content
    text_content = extract_text_content(BeautifulSoup(html, "lxml"))  # fresh copy
    structured = extract_structured_content(soup, url)
    image_urls = extract_image_urls(BeautifulSoup(html, "lxml"), url)
    internal_links = extract_all_links(BeautifulSoup(html, "lxml"), url)

    # Download images
    downloaded_images = {}
    for img_url in image_urls:
        if img_url in all_images:
            downloaded_images[img_url] = all_images[img_url]
            continue
        if is_allowed_image_domain(img_url):
            local = download_image(session, img_url, token)
            if local:
                rel_path = str(local)
                downloaded_images[img_url] = rel_path
                all_images[img_url] = rel_path
        else:
            log.info(f"Skipping image from non-allowlisted domain: {img_url}")
            # Still record the URL even if we don't download
            downloaded_images[img_url] = f"[external, not downloaded] {img_url}"
            all_images[img_url] = downloaded_images[img_url]

    page_data = {
        "url": url,
        "fetched_at": now_iso(),
        "content_hash": content_hash,
        "title": structured["title"],
        "text_content": text_content,
        "structured_content": structured,
        "images": downloaded_images,
        "internal_links": internal_links,
        "html_length": len(html),
    }

    # Save raw HTML
    page_fname = safe_filename(url)
    html_path = PAGES_DIR / f"{page_fname}.html"
    html_path.write_text(html, encoding="utf-8")
    page_data["saved_html"] = str(html_path)

    # Save text content
    txt_path = PAGES_DIR / f"{page_fname}.txt"
    txt_path.write_text(text_content, encoding="utf-8")
    page_data["saved_text"] = str(txt_path)

    # Save structured JSON
    json_path = PAGES_DIR / f"{page_fname}.json"
    json_path.write_text(json.dumps(structured, ensure_ascii=False, indent=2), encoding="utf-8")
    page_data["saved_json"] = str(json_path)

    # Recursively scrape internal links
    sub_pages = []
    for link in internal_links:
        sub = scrape_page(session, link, token, visited, all_images, depth + 1, max_depth)
        if sub:
            sub_pages.append(sub)
    page_data["sub_pages"] = sub_pages

    return page_data


def detect_changes(new_snapshot: dict, old_snapshot: dict | None) -> list[dict]:
    """Compare snapshots and return list of changes."""
    changes = []
    if old_snapshot is None:
        changes.append({
            "type": "initial_scrape",
            "timestamp": now_iso(),
            "detail": "First scrape - no previous data to compare",
        })
        return changes

    def flatten_pages(snapshot, pages=None):
        if pages is None:
            pages = {}
        if snapshot:
            pages[snapshot["url"]] = snapshot["content_hash"]
            for sub in snapshot.get("sub_pages", []):
                flatten_pages(sub, pages)
        return pages

    old_pages = flatten_pages(old_snapshot)
    new_pages = flatten_pages(new_snapshot)

    for url, h in new_pages.items():
        if url not in old_pages:
            changes.append({
                "type": "new_page",
                "timestamp": now_iso(),
                "url": url,
            })
        elif old_pages[url] != h:
            changes.append({
                "type": "content_changed",
                "timestamp": now_iso(),
                "url": url,
                "old_hash": old_pages[url],
                "new_hash": h,
            })

    for url in old_pages:
        if url not in new_pages:
            changes.append({
                "type": "page_removed",
                "timestamp": now_iso(),
                "url": url,
            })

    return changes


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_once(token: str) -> dict | None:
    """Run a single scrape cycle."""
    ensure_dirs()
    session = build_session(token)

    visited: set[str] = set()
    all_images: dict[str, str] = {}

    log.info("=" * 60)
    log.info(f"Starting scrape cycle at {now_iso()}")
    log.info("=" * 60)

    snapshot = scrape_page(session, BASE_URL, token, visited, all_images)
    if snapshot is None:
        log.error("Failed to scrape the main page.")
        return None

    # Load previous snapshot for comparison
    old_snapshot = None
    if SNAPSHOT_FILE.exists():
        try:
            old_snapshot = json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    # Detect changes
    changes = detect_changes(snapshot, old_snapshot)
    if changes:
        for c in changes:
            log.info(f"Change detected: {c['type']} - {c.get('url', c.get('detail', ''))}")

        # Append to changelog
        changelog = []
        if CHANGE_LOG.exists():
            try:
                changelog = json.loads(CHANGE_LOG.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        changelog.extend(changes)
        CHANGE_LOG.write_text(json.dumps(changelog, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        log.info("No changes detected since last scrape.")

    # Save snapshot
    SNAPSHOT_FILE.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    # Save timestamped history copy
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    history_path = HISTORY_DIR / f"snapshot_{ts}.json"
    history_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary
    def count_pages(s):
        if s is None:
            return 0
        return 1 + sum(count_pages(sub) for sub in s.get("sub_pages", []))

    total_pages = count_pages(snapshot)
    total_images = len(all_images)
    log.info(f"Scrape complete: {total_pages} pages, {total_images} images, {len(changes)} changes")

    return snapshot


def run_loop(token: str, interval: int):
    """Run scraping in a continuous loop."""
    log.info(f"Starting continuous scraping (interval: {interval}s)")
    log.info(f"Target: {BASE_URL}")
    log.info(f"Output: {OUTPUT_DIR.resolve()}")
    log.info(f"Press Ctrl+C to stop")

    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n--- Cycle {cycle} ---")
        try:
            run_once(token)
        except Exception as e:
            log.error(f"Error in scrape cycle: {e}", exc_info=True)

        log.info(f"Next scrape in {interval} seconds...")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            break


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def apply_config(output: str, extra_domains: list[str]):
    """Apply CLI overrides to module-level config."""
    global OUTPUT_DIR, IMAGES_DIR, PAGES_DIR, HISTORY_DIR, SNAPSHOT_FILE, CHANGE_LOG
    OUTPUT_DIR = Path(output)
    IMAGES_DIR = OUTPUT_DIR / "images"
    PAGES_DIR = OUTPUT_DIR / "pages"
    HISTORY_DIR = OUTPUT_DIR / "history"
    SNAPSHOT_FILE = OUTPUT_DIR / "latest_snapshot.json"
    CHANGE_LOG = OUTPUT_DIR / "changelog.json"
    for domain in extra_domains:
        IMAGE_DOMAINS_ALLOWLIST.append(domain.lower())


def main():
    parser = argparse.ArgumentParser(
        description="Scrape https://new-t.github.io/ and save content + images",
    )
    parser.add_argument(
        "--token", "-t",
        required=True,
        help="Access token for the site",
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Seconds between scrape cycles (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single scrape then exit",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="scraped_data",
        help="Output directory (default: scraped_data)",
    )
    parser.add_argument(
        "--max-depth", "-d",
        type=int,
        default=5,
        help="Max link-following depth (default: 5)",
    )
    parser.add_argument(
        "--allow-domain",
        action="append",
        default=[],
        help="Additional domains to download images from (can be used multiple times)",
    )

    args = parser.parse_args()
    apply_config(args.output, args.allow_domain)

    if args.once:
        result = run_once(args.token)
        if result is None:
            sys.exit(1)
    else:
        run_loop(args.token, args.interval)


if __name__ == "__main__":
    main()
