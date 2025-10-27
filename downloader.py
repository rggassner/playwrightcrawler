#!/usr/bin/env python3
import os
import re
import random
import asyncio
from urllib.parse import urlsplit
from collections import defaultdict
import httpx
from elasticsearch import helpers
from playwrightcrawler import content_type_comic_regex, content_type_octetstream, DatabaseConnection
from config import CONTENT_INDEX


# --- CONFIGURATION ---
LINKS_INDEX = CONTENT_INDEX
OUTPUT_DIR = "downloaded_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- FILTER CONFIGURATION (regex-based) ---
INCLUDE_EXTENSIONS = [r"cbr", r"cbz"]
EXCLUDE_EXTENSIONS = []

INCLUDE_CONTENT_TYPES = content_type_comic_regex + content_type_octetstream
EXCLUDE_CONTENT_TYPES = [r"^text/plain", r"^text/html"]

INCLUDE_HOSTS = []
EXCLUDE_HOSTS = []

MAX_CONCURRENCY = 20
RETRIES = 3
BACKOFF_FACTOR = 2  # exponential backoff base


# --- FILTER HELPERS ---
def regexes_to_es_regexp(terms):
    if not terms:
        return None
    cleaned = [re.sub(r'^\^|\$$', '', t) for t in terms]
    return "(" + "|".join(cleaned) + ")"


# --- MAIN FUNCTION TO GET FILTERED URLS ---
def get_filtered_urls(db, size=None):
    urls_index = f"{LINKS_INDEX}-*"
    must_clauses, must_not_clauses = [], []

    # --- Extension filters ---
    include_ext = regexes_to_es_regexp(INCLUDE_EXTENSIONS)
    if include_ext:
        must_clauses.append({"regexp": {"file_extension.keyword": include_ext}})
    exclude_ext = regexes_to_es_regexp(EXCLUDE_EXTENSIONS)
    if exclude_ext:
        must_not_clauses.append({"regexp": {"file_extension.keyword": exclude_ext}})

    # --- Content-type filters ---
    include_ct = regexes_to_es_regexp(INCLUDE_CONTENT_TYPES)
    if include_ct:
        must_clauses.append({"regexp": {"content_type.keyword": include_ct}})
    exclude_ct = regexes_to_es_regexp(EXCLUDE_CONTENT_TYPES)
    if exclude_ct:
        must_not_clauses.append({"regexp": {"content_type.keyword": exclude_ct}})

    # --- Host filters ---
    include_host = regexes_to_es_regexp(INCLUDE_HOSTS)
    if include_host:
        must_clauses.append({"regexp": {"host.keyword": include_host}})
    exclude_host = regexes_to_es_regexp(EXCLUDE_HOSTS)
    if exclude_host:
        must_not_clauses.append({"regexp": {"host.keyword": exclude_host}})

    query = {
        "query": {"bool": {"must": must_clauses or [{"match_all": {}}], "must_not": must_not_clauses}},
        "_source": ["url"],
    }

    urls = []
    for doc in helpers.scan(db.es, index=urls_index, query=query):
        url = doc["_source"].get("url")
        if url:
            urls.append(url)

    random.shuffle(urls)
    if size:
        urls = urls[:size]
    return urls


# --- ASYNC DOWNLOAD FUNCTION WITH RETRY & RESUME ---
async def download_file(client, url, global_semaphore, host_locks, retries=RETRIES):
    host = urlsplit(url).hostname or "unknown"
    if host not in host_locks:
        host_locks[host] = asyncio.Lock()

    attempt = 0
    filename = os.path.basename(urlsplit(url).path) or "index.html"
    filepath = os.path.join(OUTPUT_DIR, filename)

    while attempt < retries:
        try:
            async with global_semaphore, host_locks[host]:
                headers = {}
                # Resume support: start from file size if exists
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    headers["Range"] = f"bytes={file_size}-"
                else:
                    file_size = 0

                async with client.stream("GET", url, timeout=30.0, follow_redirects=True, headers=headers) as response:
                    # Accept 206 Partial Content for resume
                    if response.status_code not in (200, 206):
                        response.raise_for_status()

                    import aiofiles
                    mode = "ab" if file_size > 0 else "wb"
                    async with aiofiles.open(filepath, mode) as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            await f.write(chunk)

            print(f"[+] Downloaded {url} ({'resumed' if file_size > 0 else 'new'})")
            return

        except Exception as e:
            attempt += 1
            wait = BACKOFF_FACTOR ** attempt
            print(f"[WARN] Attempt {attempt}/{retries} failed for {url}: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)

    print(f"[ERROR] Failed to download {url} after {retries} attempts")


# --- ASYNC DOWNLOAD MANAGER ---
async def download_urls_async(urls, concurrency=MAX_CONCURRENCY):
    global_semaphore = asyncio.Semaphore(concurrency)
    host_locks = defaultdict(asyncio.Lock)

    async with httpx.AsyncClient(http2=True, verify=False) as client:
        tasks = [download_file(client, url, global_semaphore, host_locks) for url in urls]
        await asyncio.gather(*tasks)


def main():
    """
    Entry point for the asynchronous file downloader.

    This function initializes the database connection, retrieves all URLs
    that match the defined filters (extensions, content types, and hosts),
    and launches asynchronous download tasks with a configurable concurrency limit.

    Behavior:
        - Connects to Elasticsearch via `DatabaseConnection`.
        - Uses `get_filtered_urls()` to collect eligible URLs.
        - Displays the number of filtered URLs found.
        - If any URLs are available, runs `download_urls_async()` with the specified
          concurrency level defined by `MAX_CONCURRENCY`.

    Notes:
        - The script gracefully handles large result sets using Elasticsearch’s
          scroll/scan helpers.
        - Each download is performed concurrently while respecting per-host
          politeness limits.
        - Intended as the main orchestrator for filtered bulk downloads.

    """    
    db = DatabaseConnection()
    urls = get_filtered_urls(db)
    print(f"Found {len(urls)} filtered URLs.")
    if urls:
        asyncio.run(download_urls_async(urls, concurrency=MAX_CONCURRENCY))


if __name__ == "__main__":
    main()

