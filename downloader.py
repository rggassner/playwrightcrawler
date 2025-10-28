#!venv/bin/python3
import os
import re
import random
import asyncio
from urllib.parse import urlsplit, quote
from collections import defaultdict
import httpx
import aiofiles
from elasticsearch import helpers
from playwrightcrawler import content_type_comic_regex, content_type_octetstream, DatabaseConnection
from config import CONTENT_INDEX


# --- CONFIGURATION ---
LINKS_INDEX = CONTENT_INDEX
OUTPUT_DIR = os.path.abspath("downloaded_files")
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


# --- SECURE PATH BUILDER ---
def safe_filepath_from_url(url):
    """
    Generate a safe, normalized, traversal-proof file path inside OUTPUT_DIR.
    Replicates host and URL structure, adding 'index.html' where needed.
    """
    parsed = urlsplit(url)
    host = parsed.hostname or "unknown"
    path = parsed.path or "/"
    query = parsed.query

    # Add index.html for directories
    if path.endswith("/"):
        path += "index.html"

    # Encode query string to avoid collisions and illegal chars
    if query:
        safe_query = quote(query, safe="")
        base, ext = os.path.splitext(path)
        path = f"{base}_{safe_query}{ext or '.html'}"

    # Remove any leading slashes to prevent absolute paths
    path = path.lstrip("/")

    # Replace unsafe characters
    safe_path = re.sub(r"[<>:\"|?*]", "_", path)

    # Combine into host directory
    full_path = os.path.join(OUTPUT_DIR, host, safe_path)

    # Normalize to prevent ../ traversal
    normalized = os.path.normpath(full_path)

    # Ensure path is inside OUTPUT_DIR
    if not os.path.commonpath([normalized, OUTPUT_DIR]) == OUTPUT_DIR:
        # If it escapes, flatten the path
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", f"{host}_{path}")
        normalized = os.path.join(OUTPUT_DIR, "unsafe", safe_name)

    # Create parent directories
    os.makedirs(os.path.dirname(normalized), exist_ok=True)

    return normalized


# --- ASYNC DOWNLOAD FUNCTION WITH RETRY & RESUME ---
async def download_file(client, url, global_semaphore, host_locks, retries=RETRIES):
    host = urlsplit(url).hostname or "unknown"
    if host not in host_locks:
        host_locks[host] = asyncio.Lock()

    attempt = 0
    filepath = safe_filepath_from_url(url)

    while attempt < retries:
        try:
            async with global_semaphore, host_locks[host]:
                headers = {}
                # Resume support
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    headers["Range"] = f"bytes={file_size}-"
                else:
                    file_size = 0

                async with client.stream("GET", url, timeout=30.0, follow_redirects=True, headers=headers) as response:
                    if response.status_code not in (200, 206):
                        response.raise_for_status()

                    mode = "ab" if file_size > 0 else "wb"
                    async with aiofiles.open(filepath, mode) as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            await f.write(chunk)

            print(f"[+] Downloaded {url} -> {filepath} ({'resumed' if file_size > 0 else 'new'})")
            return

        except Exception as e:
            attempt += 1
            wait = BACKOFF_FACTOR ** attempt
            print(f"[WARN] Attempt {attempt}/{retries} failed for {url}: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)

    print(f"[ERROR] Failed to download {url} after {retries} attempts")


async def download_urls_async(urls, concurrency=MAX_CONCURRENCY):
    """
    Asynchronously download multiple URLs with concurrency and per-host throttling.

    This function manages concurrent downloads of a list of URLs while respecting both 
    global and per-host concurrency limits. Each host has its own asyncio.Lock to 
    prevent simultaneous requests to the same domain, which helps reduce server load 
    and avoid detection or throttling. All downloads are handled through a single 
    shared `httpx.AsyncClient` instance with HTTP/2 enabled for efficiency.

    Args:
        urls (list[str]): A list of URLs to be downloaded.
        concurrency (int, optional): The maximum number of simultaneous downloads 
            allowed across all hosts. Defaults to `MAX_CONCURRENCY`.

    Behavior:
        - Uses a global asyncio.Semaphore to cap total concurrent downloads.
        - Assigns a dedicated asyncio.Lock per host to serialize requests to the same domain.
        - Creates download tasks for all URLs and runs them concurrently.
        - Ensures all tasks complete, even if individual downloads fail.

    Raises:
        httpx.RequestError: If there is a network-level issue during download.
        asyncio.CancelledError: If the coroutine is cancelled before completion.

    Notes:
        - Directory creation and file saving are handled inside `download_file()`.
        - This function is intended to be run within an asyncio event loop, 
          typically via `asyncio.run(download_urls_async(...))`.
    """
    global_semaphore = asyncio.Semaphore(concurrency)
    host_locks = defaultdict(asyncio.Lock)

    async with httpx.AsyncClient(http2=True, verify=False) as client:
        tasks = [download_file(client, url, global_semaphore, host_locks) for url in urls]
        await asyncio.gather(*tasks)


def main():
    """
    Entry point for the asynchronous file downloader.

    - Connects to Elasticsearch via `DatabaseConnection`
    - Retrieves filtered URLs based on regex inclusion/exclusion
    - Downloads each URL concurrently using asyncio + httpx
    - Reconstructs site directory trees safely under OUTPUT_DIR
    - Prevents directory traversal, absolute path writes, and collisions
    - Supports partial resume, exponential backoff retries, and per-host concurrency
    """
    db = DatabaseConnection()
    urls = get_filtered_urls(db)
    print(f"Found {len(urls)} filtered URLs.")
    if urls:
        asyncio.run(download_urls_async(urls, concurrency=MAX_CONCURRENCY))


if __name__ == "__main__":
    main()

