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
from urllib.parse import urljoin, urlparse, unquote

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
    if len(safe) > max_len:
        safe = safe[:max_len - 17] + "_" + hashlib.md5(name.encode()).hexdigest()[:16]
    return safe


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
            return resp.json()
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
        self.downloaded: dict[str, str] = {}  # url -> local path

    def download(self, url: str) -> str | None:
        """Download an image, return local path. Skips if already downloaded."""
        if not url or url.startswith("data:"):
            return None

        if url in self.downloaded:
            return self.downloaded[url]

        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace(":", "_") or "local"
            domain_dir = self.images_dir / safe_filename(domain)
            domain_dir.mkdir(parents=True, exist_ok=True)

            # Build filename from URL path
            path_part = unquote(parsed.path).strip("/")
            if not path_part:
                path_part = sha256(url.encode())[:16]
            fname = safe_filename(path_part)

            local_path = domain_dir / fname

            # Skip if file exists and is non-empty
            if local_path.exists() and local_path.stat().st_size > 0:
                rel = str(local_path)
                self.downloaded[url] = rel
                return rel

            resp = self.session.get(url, timeout=IMAGE_TIMEOUT)
            if resp.status_code != 200:
                log.warning(f"Image download failed ({resp.status_code}): {url}")
                return None

            content = resp.content
            if not content or len(content) < 50:
                log.warning(f"Image too small or empty: {url}")
                return None

            # Fix extension based on content-type
            content_type = resp.headers.get("Content-Type", "")
            ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) if content_type else None
            image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico", ".avif"}
            if ext and local_path.suffix.lower() not in image_exts:
                local_path = local_path.with_suffix(ext)

            local_path.write_bytes(content)
            rel = str(local_path)
            self.downloaded[url] = rel
            log.info(f"Saved image: {url} -> {rel}")
            return rel

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
        self.all_posts_file = output_dir / "all_posts.json"
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
        self.state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

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

    def _process_images(self, data: dict) -> dict[str, str]:
        """Download all images found in a post/comment. Return url->local mapping."""
        image_map = {}
        for url in self._extract_image_urls(data):
            local = self.img_dl.download(url)
            if local:
                image_map[url] = local
            else:
                image_map[url] = f"[failed] {url}"
        return image_map

    def _save_post(self, post: dict, comments: list | None, image_map: dict):
        """Save a single post with its comments and image mapping."""
        pid = post.get("pid", 0)
        record = {
            "post": post,
            "comments": comments or [],
            "images_downloaded": image_map,
            "scraped_at": now_iso(),
        }
        path = self.posts_dir / f"post_{pid}.json"
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    def scrape_all_posts(self) -> list[dict]:
        """Fetch ALL posts by paginating through getlist."""
        all_posts = []
        page = 1
        empty_pages = 0

        log.info("Fetching all posts...")
        while True:
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
            all_posts.extend(posts)
            count = result.get("count", "?")
            log.info(f"Page {page}: got {len(posts)} posts (total so far: {len(all_posts)}, server count: {count})")

            page += 1
            time.sleep(0.3)  # polite rate limiting

        log.info(f"Fetched {len(all_posts)} posts total across {page - 1} pages")
        return all_posts

    def scrape_comments_for_post(self, pid: int) -> list[dict] | None:
        """Fetch all comments for a post."""
        result = self.client.get_comments(pid)
        if result is None:
            return None
        if result.get("code", -1) != 0:
            log.warning(f"Comments API error for pid={pid}: {result.get('msg', '')}")
            return None
        return result.get("data", [])

    def run_full_scrape(self) -> dict:
        """Run a complete scrape: all posts + comments + images."""
        state = self._load_state()
        ts = now_iso()
        log.info("=" * 60)
        log.info(f"Full scrape starting at {ts}")
        log.info(f"Previous max PID: {state['last_max_pid']}")
        log.info("=" * 60)

        # 1. Fetch all posts
        posts = self.scrape_all_posts()
        if not posts:
            log.error("No posts fetched.")
            return {"error": "No posts fetched", "timestamp": ts}

        new_max_pid = max(p.get("pid", 0) for p in posts)
        new_posts_count = sum(1 for p in posts if p.get("pid", 0) > state["last_max_pid"])
        log.info(f"New posts since last scrape: {new_posts_count}")
        log.info(f"Max PID: {new_max_pid}")

        # 2. For each post, fetch comments and download images
        total_comments = 0
        total_images = 0
        all_records = []

        for i, post in enumerate(posts):
            pid = post.get("pid", 0)
            if i % 50 == 0:
                log.info(f"Processing post {i+1}/{len(posts)} (pid={pid})...")

            # Download post images
            image_map = self._process_images(post)
            total_images += len([v for v in image_map.values() if not v.startswith("[failed]")])

            # Fetch comments
            comments = self.scrape_comments_for_post(pid)
            if comments:
                total_comments += len(comments)
                # Download comment images
                for comment in comments:
                    comment_images = self._process_images(comment)
                    image_map.update(comment_images)
                    total_images += len([v for v in comment_images.values() if not v.startswith("[failed]")])

            # Save individual post file
            self._save_post(post, comments, image_map)

            all_records.append({
                "pid": pid,
                "text_preview": post.get("text", "")[:100],
                "n_comments": len(comments) if comments else 0,
                "n_images": len(image_map),
                "create_time": post.get("create_time", ""),
            })

            time.sleep(0.2)  # rate limiting

        # 3. Save summary
        summary = {
            "scraped_at": ts,
            "total_posts": len(posts),
            "new_posts": new_posts_count,
            "total_comments": total_comments,
            "total_images_downloaded": total_images,
            "max_pid": new_max_pid,
            "posts_index": all_records,
        }

        # Save all posts JSON (full data)
        self.all_posts_file.write_text(
            json.dumps({"posts": posts, "scraped_at": ts}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Save timestamped snapshot
        ts_safe = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        history_file = self.history_dir / f"scrape_{ts_safe}.json"
        history_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        # Changelog
        changes = []
        if new_posts_count > 0:
            changes.append({
                "type": "new_posts",
                "count": new_posts_count,
                "timestamp": ts,
                "max_pid": new_max_pid,
            })
        if changes:
            changelog = []
            if self.changelog_file.exists():
                try:
                    changelog = json.loads(self.changelog_file.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    pass
            changelog.extend(changes)
            self.changelog_file.write_text(
                json.dumps(changelog, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        # Update state
        state["last_max_pid"] = new_max_pid
        state["total_posts"] = len(posts)
        state["total_images"] = total_images
        state["last_scrape"] = ts
        self._save_state(state)

        log.info("=" * 60)
        log.info(f"Scrape complete!")
        log.info(f"  Posts: {len(posts)}")
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

        new_posts = []
        page = 1
        found_old = False

        while not found_old:
            result = self.client.get_list(page=page)
            if result is None or result.get("code", -1) != 0:
                break

            posts = result.get("data", [])
            if not posts:
                break

            for post in posts:
                pid = post.get("pid", 0)
                if pid <= last_max:
                    found_old = True
                    break
                new_posts.append(post)

            page += 1
            time.sleep(0.3)

        if not new_posts:
            log.info("No new posts found.")
            return {"new_posts": 0, "timestamp": ts}

        log.info(f"Found {len(new_posts)} new posts")

        total_images = 0
        for post in new_posts:
            pid = post.get("pid", 0)
            image_map = self._process_images(post)
            total_images += len([v for v in image_map.values() if not v.startswith("[failed]")])

            comments = self.scrape_comments_for_post(pid)
            if comments:
                for comment in comments:
                    comment_images = self._process_images(comment)
                    image_map.update(comment_images)
                    total_images += len([v for v in comment_images.values() if not v.startswith("[failed]")])

            self._save_post(post, comments, image_map)
            time.sleep(0.2)

        new_max = max(p.get("pid", 0) for p in new_posts)
        state["last_max_pid"] = max(new_max, last_max)
        state["last_scrape"] = ts
        self._save_state(state)

        log.info(f"Incremental scrape done: {len(new_posts)} new posts, {total_images} images")
        return {"new_posts": len(new_posts), "images": total_images, "timestamp": ts}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def apply_config(output: str):
    global OUTPUT_DIR
    OUTPUT_DIR = Path(output)


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
    apply_config(args.output)
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
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            break


if __name__ == "__main__":
    main()
