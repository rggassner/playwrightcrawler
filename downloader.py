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
from playwrightcrawler import content_type_octetstream, content_type_pdf_regex, DatabaseConnection
from config import CONTENT_INDEX


# --- CONFIGURATION ---
LINKS_INDEX = CONTENT_INDEX
OUTPUT_DIR = os.path.abspath("downloaded_files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- FILTER CONFIGURATION (regex-based) ---

#Download all comics
#from playwrightcrawler import content_type_comic_regex
#INCLUDE_EXTENSIONS = [r"cbr", r"cbz"]
#EXCLUDE_EXTENSIONS = []
#INCLUDE_CONTENT_TYPES = content_type_comic_regex + content_type_octetstream
#EXCLUDE_CONTENT_TYPES = [r"^text/plain", r"^text/html"]
#INCLUDE_HOSTS = []
#EXCLUDE_HOSTS = []


INCLUDE_EXTENSIONS = [r"pdf"]
EXCLUDE_EXTENSIONS = []

INCLUDE_CONTENT_TYPES = content_type_pdf_regex + content_type_octetstream
EXCLUDE_CONTENT_TYPES = [r"^text/plain", r"^text/html"]

INCLUDE_HOSTS = []
EXCLUDE_HOSTS = []

MAX_CONCURRENCY = 20
RETRIES = 3
BACKOFF_FACTOR = 2  # exponential backoff base


# --- FILTER HELPERS ---
def regexes_to_es_regexp(terms):
    """
    Converts a list of regex terms into a single Elasticsearch-compatible regexp pattern.
    
    Safely handles None, empty strings, or non-string elements. 
    Returns None if no valid patterns remain.
    """
    if not terms:
        return None

    cleaned = []
    for t in terms:
        if not t or not isinstance(t, str):
            continue
        # Remove ^ and $ anchors, since ES regex doesn't support them
        t = re.sub(r'^\^|\$$', '', t)
        t = t.strip()
        if t:
            cleaned.append(t)

    if not cleaned:
        return None

    # Join all patterns using alternation
    joined = "|".join(cleaned)

    # Truncate overly long regexes to stay under Elasticsearch's 1000-character limit
    if len(joined) > 900:
        joined = joined[:900] + ".*"  # fallback pattern to avoid error

    return joined


# --- MAIN FUNCTION TO GET FILTERED URLS ---
def get_filtered_urls(db, size=None, chunk_size=10):
    """
    Retrieve filtered URLs from Elasticsearch using regex-based inclusion and exclusion rules.
    Automatically splits long regex lists into smaller chunks to stay under the
    Elasticsearch max_regex_length limit (default 1000 chars).

    Args:
        db (DatabaseConnection): The database connection wrapper (must expose `es` attribute).
        size (int, optional): Limit number of URLs returned. If None, all are returned.
        chunk_size (int, optional): Maximum number of regex patterns per query chunk.

    Returns:
        list[str]: List of filtered URLs matching all the inclusion/exclusion criteria.
    """

    urls_index = f"{LINKS_INDEX}-*"
    urls = []

    def chunks(lst, n):
        """Yield successive n-sized chunks from a list."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Helper to generate query parts
    def build_query(include_ext, exclude_ext, include_ct, exclude_ct, include_host, exclude_host):
        must_clauses, must_not_clauses = [], []

        # --- Extension filters ---
        if include_ext:
            must_clauses.append({"regexp": {"file_extension.keyword": include_ext}})
        if exclude_ext:
            must_not_clauses.append({"regexp": {"file_extension.keyword": exclude_ext}})

        # --- Content-type filters ---
        if include_ct:
            must_clauses.append({"regexp": {"content_type.keyword": include_ct}})
        if exclude_ct:
            must_not_clauses.append({"regexp": {"content_type.keyword": exclude_ct}})

        # --- Host filters ---
        if include_host:
            must_clauses.append({"regexp": {"host.keyword": include_host}})
        if exclude_host:
            must_not_clauses.append({"regexp": {"host.keyword": exclude_host}})

        return {
            "query": {"bool": {"must": must_clauses or [{"match_all": {}}], "must_not": must_not_clauses}},
            "_source": ["url"],
        }

    # --- Chunk-based querying for large regex lists ---
    include_ct_chunks = list(chunks(INCLUDE_CONTENT_TYPES or [None], chunk_size))
    include_ext_chunks = list(chunks(INCLUDE_EXTENSIONS or [None], chunk_size))
    include_host_chunks = list(chunks(INCLUDE_HOSTS or [None], chunk_size))

    # Ensure at least one chunk exists for each (so we loop once even if empty)
    include_ct_chunks = include_ct_chunks or [[None]]
    include_ext_chunks = include_ext_chunks or [[None]]
    include_host_chunks = include_host_chunks or [[None]]

    for ct_chunk in include_ct_chunks:
        for ext_chunk in include_ext_chunks:
            for host_chunk in include_host_chunks:
                query = build_query(
                    regexes_to_es_regexp(ext_chunk),
                    regexes_to_es_regexp(EXCLUDE_EXTENSIONS),
                    regexes_to_es_regexp(ct_chunk),
                    regexes_to_es_regexp(EXCLUDE_CONTENT_TYPES),
                    regexes_to_es_regexp(host_chunk),
                    regexes_to_es_regexp(EXCLUDE_HOSTS),
                )

                try:
                    for doc in helpers.scan(db.es, index=urls_index, query=query):
                        url = doc["_source"].get("url")
                        if url:
                            urls.append(url)
                except Exception as e:
                    print(f"[WARN] Skipping chunk due to query error: {e}")

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


async def download_file(client, url, global_semaphore, host_locks, retries=RETRIES):
    """
    Asynchronously download a file with resume, retry, and skip-on-non-range support.

    This function handles the download of a single URL while enforcing both global 
    and per-host concurrency limits. It supports HTTP range-based resuming for 
    partially downloaded files and intelligently skips re-downloading files when 
    the server does not support range requests.

    Behavior:
        - If the file does not exist, it downloads from scratch.
        - If the file exists and the server supports `Range` requests (HTTP 206),
          the download resumes from the last saved byte.
        - If the file exists but the server responds with `HTTP 200 OK`, indicating 
          no range support, the download is skipped to avoid overwriting a complete file.
        - Uses exponential backoff retry logic for transient errors.
        - Each host is protected by a dedicated asyncio.Lock to prevent multiple
          concurrent requests to the same domain.

    Args:
        client (httpx.AsyncClient): Shared HTTP client with HTTP/2 and connection pooling.
        url (str): The target URL to download.
        global_semaphore (asyncio.Semaphore): Limits the total number of concurrent downloads.
        host_locks (dict[str, asyncio.Lock]): A mapping of hostname to per-host lock objects.
        retries (int, optional): Maximum number of retry attempts before giving up. Defaults to `RETRIES`.

    Returns:
        None
            Writes the downloaded (or resumed) file to disk under OUTPUT_DIR using a safe, 
            normalized path derived from the URL. Skipped and failed downloads are logged 
            to stdout with clear messages.

    Raises:
        httpx.RequestError: On unrecoverable network or connection issues.
        httpx.HTTPStatusError: If the server responds with an unexpected HTTP code.
        asyncio.CancelledError: If the coroutine is cancelled mid-download.

    Notes:
        - The function supports resume via the HTTP `Range` header.
        - Files are opened in append mode (`"ab"`) when resuming, and in write mode (`"wb"`) for fresh downloads.
        - The function respects server limits by serializing downloads per host.
        - Skipping avoids redundant data transfer when range is unsupported.
    """    
    host = urlsplit(url).hostname or "unknown"
    if host not in host_locks:
        host_locks[host] = asyncio.Lock()

    attempt = 0
    filepath = safe_filepath_from_url(url)

    while attempt < retries:
        try:
            async with global_semaphore, host_locks[host]:
                headers = {}
                file_size = 0

                # --- If file exists, try to resume ---
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    headers["Range"] = f"bytes={file_size}-"

                async with client.stream("GET", url, timeout=30.0, follow_redirects=True, headers=headers) as response:
                    # --- Handle HTTP status codes ---
                    if response.status_code == 206:
                        # ✅ Server supports range requests (resume)
                        mode = "ab"
                        print(f"[RESUME] Resuming {url} from {file_size} bytes...")
                    elif response.status_code == 200:
                        # ⚠️ Server does not support Range
                        if os.path.exists(filepath) and file_size > 0:
                            print(f"[SKIP] Server does not support Range, and file already exists: {filepath}")
                            return  # ✅ Skip instead of redownloading
                        mode = "wb"
                    else:
                        response.raise_for_status()

                    # --- Write file ---
                    async with aiofiles.open(filepath, mode) as f: # pylint: disable=possibly-used-before-assignment
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

