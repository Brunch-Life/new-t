#!/usr/bin/env python3
"""
新T树洞 (New T Hole) scraper.
Continuously fetches all posts, comments, and images via the REST API.

Backend API: https://api.tholeapis.top/_api/v1/
Auth: User-Token header
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
from urllib.parse import urlparse, unquote

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = "https://api.tholeapis.top/_api/v1"
API_BASE_V2 = "https://api.tholeapis.top/_api/v2"
DEFAULT_INTERVAL = 300  # seconds between full scrape cycles
OUTPUT_DIR = Path("scraped_data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

REQUEST_TIMEOUT = 30
IMAGE_TIMEOUT = 60
MAX_PAGES = 10000
MAX_CHANGELOG_ENTRIES = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_filename(name: str, max_len: int = 200) -> str:
    safe = re.sub(r'[^\w.\-]', '_', name)
    if not safe or safe == '_' * len(safe):
        safe = hashlib.md5(name.encode()).hexdigest()[:16]
    if len(safe) > max_len:
        safe = safe[:max_len - 17] + "_" + hashlib.md5(name.encode()).hexdigest()[:16]
    return safe


def atomic_write_json(path: Path, data):
    """Write JSON atomically via temp file + rename."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def normalize_image_url(url: str) -> str:
    """Strip query params for dedup purposes."""
    parsed = urlparse(url)
    return parsed._replace(query="", fragment="").geturl()


def ensure_dirs(base: Path):
    for sub in ["posts", "images", "history"]:
        (base / sub).mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

class HoleClient:
    """Client for the 新T树洞 REST API."""

    def __init__(self, token: str, api_base: str = API_BASE):
        self.token = token
        self.api_base = api_base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.headers["User-Token"] = token

    def _get(self, endpoint: str, params: dict | None = None) -> dict | None:
        url = f"{self.api_base}/{endpoint.lstrip('/')}"
        try:
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 401:
                log.error(f"Token invalid or expired (401): {url}")
                return None
            if resp.status_code != 200:
                log.warning(f"GET {url} -> {resp.status_code}: {resp.text[:200]}")
                return None
            data = resp.json()
            if not isinstance(data, dict):
                log.warning(f"Unexpected JSON type from {url}: {type(data).__name__}")
                return None
            return data
        except requests.RequestException as e:
            log.error(f"Request error GET {url}: {e}")
            return None
        except ValueError as e:
            log.error(f"JSON decode error for {url}: {e}")
            return None

    def get_list(self, page: int = 1, order_mode: int = 0, room_id: int | None = None) -> dict | None:
        """Fetch paginated post list (25 per page)."""
        params = {"p": page, "order_mode": order_mode}
        if room_id is not None:
            params["room_id"] = room_id
        return self._get("getlist", params)

    def get_one(self, pid: int) -> dict | None:
        """Fetch a single post by ID."""
        return self._get("getone", {"pid": pid})

    def get_comments(self, pid: int) -> dict | None:
        """Fetch comments for a post."""
        return self._get("getcomment", {"pid": pid})

    def get_attention(self) -> dict | None:
        """Fetch user's followed posts."""
        return self._get("getattention")

    def search(self, keywords: str, page: int = 1, search_mode: int = 0) -> dict | None:
        """Search posts."""
        return self._get("search", {
            "keywords": keywords,
            "page": page,
            "search_mode": search_mode,
        })

    def get_multi(self, pids: list[int]) -> dict | None:
        """Fetch multiple posts by IDs."""
        return self._get("getmulti", {"pids": ",".join(str(p) for p in pids)})

    def get_system_msg(self) -> dict | None:
        """Fetch system messages."""
        return self._get("/system_msg")


# ---------------------------------------------------------------------------
# Image downloader
# ---------------------------------------------------------------------------

class ImageDownloader:
    """Download and cache images locally."""

    def __init__(self, images_dir: Path, token: str):
        self.images_dir = images_dir
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.headers["User-Token"] = token
        self.seen: set[str] = set()

    def reset(self):
        self.seen.clear()

    def download(self, url: str) -> str | None:
        """Download an image, return local path. Skips if already downloaded."""
        if not url or url.startswith("data:"):
            return None

        norm = normalize_image_url(url)
        if norm in self.seen:
            return None
        self.seen.add(norm)

        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace(":", "_") or "local"
            domain_dir = self.images_dir / safe_filename(domain)
            domain_dir.mkdir(parents=True, exist_ok=True)

            path_part = unquote(parsed.path).strip("/")
            if not path_part:
                path_part = sha256(url.encode())[:16]
            fname = safe_filename(path_part)

            local_path = domain_dir / fname

            # Also check with common image extensions
            image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico", ".avif"}
            if local_path.suffix.lower() not in image_exts:
                for ext in [".jpg", ".png", ".webp"]:
                    alt = local_path.with_suffix(ext)
                    if alt.exists() and alt.stat().st_size > 0:
                        return str(alt)

            if local_path.exists() and local_path.stat().st_size > 0:
                return str(local_path)

            resp = self.session.get(url, timeout=IMAGE_TIMEOUT, stream=True)
            if resp.status_code != 200:
                resp.close()
                log.warning(f"Image download failed ({resp.status_code}): {url}")
                return None

            content_type = resp.headers.get("Content-Type", "")
            ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) if content_type else None
            if ext and local_path.suffix.lower() not in image_exts:
                local_path = local_path.with_suffix(ext)

            size = 0
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    size += len(chunk)
            resp.close()

            if size < 50:
                local_path.unlink(missing_ok=True)
                log.warning(f"Image too small ({size}B): {url}")
                return None

            log.info(f"Saved image: {url} -> {local_path}")
            return str(local_path)

        except Exception as e:
            log.error(f"Error downloading image {url}: {e}")
            return None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class HoleScraper:
    """Full scraper that archives all posts, comments, and images."""

    def __init__(self, token: str, output_dir: Path, api_base: str = API_BASE):
        self.output = output_dir
        self.client = HoleClient(token, api_base)
        self.img_dl = ImageDownloader(output_dir / "images", token)
        self.posts_dir = output_dir / "posts"
        self.history_dir = output_dir / "history"
        ensure_dirs(output_dir)

        # State tracking
        self.changelog_file = output_dir / "changelog.json"
        self.state_file = output_dir / "scraper_state.json"

    def _load_state(self) -> dict:
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return {"last_max_pid": 0, "total_posts": 0, "total_images": 0}

    def _save_state(self, state: dict):
        atomic_write_json(self.state_file, state)

    def _extract_image_urls(self, post: dict) -> list[str]:
        """Extract all image URLs from a post or comment."""
        urls = []
        text = post.get("text", "")

        # Markdown image syntax: ![alt](url)
        for match in re.findall(r'!\[.*?\]\((.*?)\)', text):
            urls.append(match)

        # HTML img tags in text
        for match in re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', text):
            urls.append(match)

        # Direct image URL in text (common patterns)
        for match in re.findall(r'(https?://[^\s<>"]+\.(?:png|jpg|jpeg|gif|webp|avif|svg|bmp)(?:\?[^\s<>"]*)?)', text, re.IGNORECASE):
            urls.append(match)

        # Risu.io specific patterns
        for match in re.findall(r'(https?://[^\s<>"]*risu\.io[^\s<>"]*)', text, re.IGNORECASE):
            if match not in urls:
                urls.append(match)

        # Post-level image field (some backends store image URLs directly)
        if post.get("url"):
            urls.append(post["url"])
        if post.get("image"):
            urls.append(post["image"])
        if post.get("image_url"):
            urls.append(post["image_url"])

        return urls

    def _process_images(self, data: dict) -> int:
        """Download all images found in a post/comment. Return count of successes."""
        count = 0
        for url in self._extract_image_urls(data):
            if self.img_dl.download(url):
                count += 1
        return count

    def _save_post(self, post: dict, comments: list | None):
        """Save a single post with its comments."""
        pid = post.get("pid", 0)
        record = {
            "post": post,
            "comments": comments or [],
            "scraped_at": now_iso(),
        }
        path = self.posts_dir / f"post_{pid}.json"
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    def _iter_pages(self):
        """Yield posts one page at a time, without accumulating."""
        page = 1
        empty_pages = 0
        while page <= MAX_PAGES:
            result = self.client.get_list(page=page)
            if result is None:
                log.error(f"Failed to fetch page {page}, stopping pagination")
                break
            code = result.get("code", -1)
            if code != 0:
                log.warning(f"API returned code={code} on page {page}: {result.get('msg', '')}")
                break
            posts = result.get("data", [])
            if not posts:
                empty_pages += 1
                if empty_pages >= 3:
                    log.info(f"3 consecutive empty pages at page {page}, done.")
                    break
                page += 1
                continue
            empty_pages = 0
            count = result.get("count", "?")
            log.info(f"Page {page}: got {len(posts)} posts (server count: {count})")
            yield from posts
            page += 1
            time.sleep(0.3)

    def _fetch_comments(self, pid: int) -> list[dict] | None:
        result = self.client.get_comments(pid)
        if result is None:
            return None
        if result.get("code", -1) != 0:
            log.warning(f"Comments API error for pid={pid}: {result.get('msg', '')}")
            return None
        return result.get("data", [])

    def _process_single_post(self, post: dict) -> tuple[int, int]:
        """Fetch comments, download images, save to disk. Returns (n_comments, n_images)."""
        pid = post.get("pid", 0)
        n_images = self._process_images(post)

        comments = self._fetch_comments(pid)
        n_comments = 0
        if comments:
            n_comments = len(comments)
            for comment in comments:
                n_images += self._process_images(comment)

        self._save_post(post, comments)
        time.sleep(0.2)
        return n_comments, n_images

    def run_full_scrape(self) -> dict:
        """Stream-process all posts: fetch page by page, process and discard."""
        state = self._load_state()
        ts = now_iso()
        log.info("=" * 60)
        log.info(f"Full scrape starting at {ts}")
        log.info(f"Previous max PID: {state['last_max_pid']}")
        log.info("=" * 60)

        self.img_dl.reset()

        total_posts = 0
        total_comments = 0
        total_images = 0
        new_posts_count = 0
        max_pid = 0

        # Stream posts: process each one and immediately discard
        for post in self._iter_pages():
            pid = post.get("pid", 0)
            max_pid = max(max_pid, pid)
            if pid > state["last_max_pid"]:
                new_posts_count += 1

            total_posts += 1
            if total_posts % 50 == 0:
                log.info(f"Processing post #{total_posts} (pid={pid})...")

            n_comments, n_images = self._process_single_post(post)
            total_comments += n_comments
            total_images += n_images

        if total_posts == 0:
            log.error("No posts fetched.")
            return {"error": "No posts fetched", "timestamp": ts}

        # Save lightweight summary
        summary = {
            "scraped_at": ts,
            "total_posts": total_posts,
            "new_posts": new_posts_count,
            "total_comments": total_comments,
            "total_images_downloaded": total_images,
            "max_pid": max_pid,
        }

        ts_safe = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        history_file = self.history_dir / f"scrape_{ts_safe}.json"
        history_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        if new_posts_count > 0:
            changelog = []
            if self.changelog_file.exists():
                try:
                    changelog = json.loads(self.changelog_file.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    pass
            changelog.append({
                "type": "new_posts",
                "count": new_posts_count,
                "timestamp": ts,
                "max_pid": max_pid,
            })
            if len(changelog) > MAX_CHANGELOG_ENTRIES:
                changelog = changelog[-MAX_CHANGELOG_ENTRIES:]
            atomic_write_json(self.changelog_file, changelog)

        state["last_max_pid"] = max_pid
        state["total_posts"] = total_posts
        state["total_images"] = total_images
        state["last_scrape"] = ts
        self._save_state(state)

        log.info("=" * 60)
        log.info(f"Scrape complete!")
        log.info(f"  Posts: {total_posts}")
        log.info(f"  New posts: {new_posts_count}")
        log.info(f"  Comments: {total_comments}")
        log.info(f"  Images: {total_images}")
        log.info("=" * 60)

        return summary

    def run_incremental(self) -> dict:
        """Quick incremental scrape - only fetch recent pages until we hit known posts."""
        state = self._load_state()
        last_max = state["last_max_pid"]
        ts = now_iso()

        log.info(f"Incremental scrape (looking for posts after pid={last_max})")
        self.img_dl.reset()

        new_count = 0
        total_images = 0
        new_max = last_max
        done = False
        page = 1

        while not done:
            result = self.client.get_list(page=page)
            if result is None or result.get("code", -1) != 0:
                break
            posts = result.get("data", [])
            if not posts:
                break

            for post in posts:
                pid = post.get("pid", 0)
                if pid <= last_max:
                    done = True
                    break
                new_max = max(new_max, pid)
                n_comments, n_images = self._process_single_post(post)
                total_images += n_images
                new_count += 1

            page += 1
            time.sleep(0.3)

        if new_count == 0:
            log.info("No new posts found.")
            return {"new_posts": 0, "timestamp": ts}

        state["last_max_pid"] = new_max
        state["last_scrape"] = ts
        self._save_state(state)

        log.info(f"Incremental scrape done: {new_count} new posts, {total_images} images")
        return {"new_posts": new_count, "images": total_images, "timestamp": ts}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="新T树洞 scraper - fetch all posts, comments, and images via API",
    )
    parser.add_argument(
        "--token", "-t",
        required=True,
        help="User-Token for API authentication",
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
        help="Run a single full scrape then exit",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force a full scrape even in continuous mode (default: first=full, then incremental)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="scraped_data",
        help="Output directory (default: scraped_data)",
    )
    parser.add_argument(
        "--api-base",
        type=str,
        default=API_BASE,
        help=f"API base URL (default: {API_BASE})",
    )

    args = parser.parse_args()
    output = Path(args.output)

    scraper = HoleScraper(args.token, output, args.api_base)

    if args.once:
        result = scraper.run_full_scrape()
        if "error" in result:
            sys.exit(1)
        return

    # Continuous mode
    log.info(f"Starting continuous scraping (interval: {args.interval}s)")
    log.info(f"API: {args.api_base}")
    log.info(f"Output: {output.resolve()}")
    log.info("Press Ctrl+C to stop")

    cycle = 0
    try:
        while True:
            cycle += 1
            log.info(f"\n--- Cycle {cycle} ---")
            try:
                if cycle == 1 or args.full:
                    scraper.run_full_scrape()
                else:
                    scraper.run_incremental()
            except Exception as e:
                log.error(f"Error in scrape cycle: {e}", exc_info=True)

            log.info(f"Next scrape in {args.interval} seconds...")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Stopped by user.")


if __name__ == "__main__":
    main()
