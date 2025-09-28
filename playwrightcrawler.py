#!venv/bin/python3
from urllib.parse import urljoin, urlsplit, unquote, urlparse, parse_qs
from config import *
import re
import os
import json
import chardet
import hashlib
import asyncio
import urllib3
import hashlib
import warnings
import absl.logging
import numpy as np
from pathlib import PurePosixPath
from playwright.async_api import async_playwright
from collections import Counter
from PIL import Image, UnidentifiedImageError
from io import BytesIO
from pprint import pprint
from fake_useragent import UserAgent
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from datetime import datetime, timezone
from elasticsearch import helpers, ConflictError
from elasticsearch import NotFoundError, RequestError
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions
from elasticsearch.exceptions import NotFoundError, RequestError
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime, timezone


absl.logging.set_verbosity('error')
warnings.filterwarnings("ignore", category=InsecureRequestWarning)
warnings.filterwarnings(
        "ignore",
        category=Warning,
        message=".*verify_certs=False is insecure.*")
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ua = UserAgent()

if CATEGORIZE_NSFW:
    import opennsfw2 as n2
    model = n2.make_open_nsfw_model()

soup_tag_blocklist = {"script", "style", "noscript", "iframe", "meta", "head", "title", "input"}

url_functions = []
content_type_functions = []
results = {"crawledcontent": {}, "crawledlinks": set()}


def create_directories():
    dirs = ["images", "images/sfw", "images/nsfw", "fonts", "videos"]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def get_host_levels(hostname):
    hostname = hostname.split(':')[0]  # Remove port if present
    parts = hostname.split('.')
    parts_reversed = list(parts)
    return {
        "host_levels": parts_reversed,
        "host_level_map": {
            f"host_level_{i+1}": level
            for i, level in enumerate(parts_reversed)
        }
    }

def get_directory_levels(url_path):
    # Split the URL path into parts and remove empty strings
    levels = [p for p in url_path.strip("/").split("/") if p]

    # Ensure the levels list is padded to MAX_DIR_LEVELS
    if len(levels) < MAX_DIR_LEVELS:
        levels = levels + [''] * (MAX_DIR_LEVELS - len(levels))  # Add empty levels at the end

    # Map the levels to their directory level numbers
    directory_level_map = {f"directory_level_{i+1}": levels[i] for i in range(len(levels))}

    return {
        "directory_levels": levels,
        "directory_level_map": directory_level_map
    }
        


def preprocess_crawler_data(data: dict) -> dict:
    """
    Sample data

    data = {
        'crawledcontent': {
            'https://imapsync.lamiral.info/examples/file.txt': {
                'content_type': 'text/plain',
                'isopendir': False,
                'parent_host': 'imapsync.lamiral.info',
                'source': 'content_type_plain_text_regex',
                'url': 'https://imapsync.lamiral.info/examples/file.txt',
                'visited': True,
                'words': ['script', 'test1', 'exclude', 'toto', 'tata']
            }
        },
        'crawledlinks': {
            'https://imapsync.lamiral.info/',
            'https://imapsync.lamiral.info/examples',
            'https://imapsync.lamiral.info/examples/file.txt'
        }
    }
    """
def preprocess_crawler_data(data: dict) -> dict:
    crawledcontent = data.get("crawledcontent", {})
    crawledlinks = data.get("crawledlinks", set())

    filtered_links = set()
    new_crawledcontent = {}

    for url in crawledlinks:
        host = urlsplit(url).hostname
        if host and not is_host_block_listed(host) and is_host_allow_listed(host) and not is_url_block_listed(url):
            if url not in crawledcontent:
                filtered_links.add(url)

    for url, doc in crawledcontent.items():
        host = urlsplit(url).hostname
        if host and not is_host_block_listed(host) and is_host_allow_listed(host) and not is_url_block_listed(url):
            new_doc = doc.copy()
            insert_only_fields = {}

            parsed = urlsplit(url)

            # --- Emails ---
            email = doc.get("emails") or doc.get("email")
            if email and isinstance(email, str):
                insert_only_fields["emails"] = [email]

            # --- Query info ---
            has_query = bool(parsed.query)
            query_variables = []
            query_values = []
            if has_query:
                parsed_qs = parse_qs(parsed.query)
                query_variables = list(parsed_qs.keys())
                query_values = [v for vals in parsed_qs.values() for v in vals]

            insert_only_fields["has_query"] = has_query
            if query_variables:
                insert_only_fields["query_variables"] = query_variables
            if query_values:
                insert_only_fields["query_values"] = query_values

            # --- Host levels ---
            host_parts = get_host_levels(host).get("host_levels", [])
            if len(host_parts) < MAX_HOST_LEVELS:
                host_parts = [''] * (MAX_HOST_LEVELS - len(host_parts)) + host_parts
            insert_only_fields["host_levels"] = host_parts
            for i, part in enumerate(reversed(host_parts[-MAX_HOST_LEVELS:])):
                insert_only_fields[f"host_level_{i+1}"] = part

            # --- Directory levels ---
            dir_parts = get_directory_levels(parsed.path).get("directory_levels", [])
            if len(dir_parts) < MAX_DIR_LEVELS:
                dir_parts = [''] * (MAX_DIR_LEVELS - len(dir_parts)) + dir_parts
            insert_only_fields["directory_levels"] = dir_parts
            for i, part in enumerate(dir_parts[:MAX_DIR_LEVELS]):
                insert_only_fields[f"directory_level_{i+1}"] = part

            # --- File extension ---
            _, file_extension = os.path.splitext(unquote(parsed.path))
            if file_extension:
                insert_only_fields['file_extension'] = file_extension.lower().lstrip('.')

            # Merge extra fields
            new_doc.update(insert_only_fields)
            new_crawledcontent[url] = new_doc

    return {
        "crawledcontent": new_crawledcontent,
        "crawledlinks": filtered_links
    }



class DatabaseConnection:

    def __init__(self):
        es_config = {
            "hosts": [f"https://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"],
            "basic_auth": (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
            "verify_certs": ELASTICSEARCH_VERIFY_CERTS,
            "request_timeout": ELASTICSEARCH_TIMEOUT,
            "retry_on_timeout": ELASTICSEARCH_RETRY,
            "max_retries": ELASTICSEARCH_RETRIES,
            "http_compress": ELASTICSEARCH_HTTP_COMPRESS
        }
        if ELASTICSEARCH_CA_CERT_PATH:
            es_config["ca_certs"] = ELASTICSEARCH_CA_CERT_PATH
        self.es = Elasticsearch(**es_config)
        self.con = self.es

    def close(self):
        self.es.close()

    def commit(self):
        pass

    def search(self, *args, **kwargs):
        return self.es.search(*args, **kwargs)

    def scroll(self, *args, **kwargs):
        return self.es.scroll(*args, **kwargs)


    def _get_index_name(self, base: str) -> str:
        """Return index name with year-month suffix (timezone-aware)."""
        suffix = datetime.now(timezone.utc).strftime("%Y-%m")
        return f"{base}-{suffix}"

    def save_batch(self, data: dict):
        """
        Save crawled content + links into Elasticsearch using streaming_bulk.
        Handles errors per-document instead of crashing.
        """
        if not self.con:
            raise ValueError("db connection is required")

        urls_index = self._get_index_name(LINKS_INDEX)
        content_index = self._get_index_name(CONTENT_INDEX)

        actions = []

        # --- crawledcontent ---
        for url, doc in data.get("crawledcontent", {}).items():
            doc["created_at"] = datetime.now(timezone.utc)
            doc["updated_at"] = datetime.now(timezone.utc)
            actions.append({
                "_op_type": "index",
                "_index": content_index,
                "_id": url_to_id(url),  # <-- hashed ID
                "_source": doc
            })

        # --- crawledlinks ---
        for link in data.get("crawledlinks", set()):
            doc = {
                "url": link,
                "visited": False,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            actions.append({
                "_op_type": "index",
                "_index": urls_index,
                "_id": url_to_id(link),  # <-- hashed ID
                "_source": doc
            })

        # --- Insert using streaming_bulk with per-item error logging ---
        failed_count = 0
        for ok, item in helpers.streaming_bulk(
            self.es.options(request_timeout=240),
            actions,
            raise_on_error=False,
            raise_on_exception=False
        ):
            if not ok:
                failed_count += 1
                print("[BULK FAILED] Document:", item)

        print(f"[BULK] Inserted {len(actions) - failed_count} docs successfully, {failed_count} failed")

def url_to_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def get_index_name(base: str) -> str:
    suffix = datetime.now(timezone.utc).strftime("%Y-%m")
    return f"{base}-{suffix}"

def db_create_monthly_indexes(db=None):
    if db is None or db.es is None:  # <- changed from db.con
        raise ValueError("db connection is required")
    urls_index = get_index_name(LINKS_INDEX)
    content_index = get_index_name(CONTENT_INDEX)

    return urls_index, content_index

content_type_html_regex = [
        r"^text/html$",
        r"^application/html$",
        r"^text/html,text/html",
        r"^text/fragment\+html$",
        r"^text/html, charset=.*",
        r"^text/x-html-fragment$",
        r"^application/xhtml\+xml$",
        r"^text/html,charset=UTF-8$",
        r"^text/vnd\.reddit\.partial\+html$",
    ]

content_type_plain_text_regex = [
        r"^\.js$",
        r"^text$",
        r"^json$",
        r"^text/\*$",
        r"^text/js$",
        r"^text/xml$",
        r"^text/srt$",
        r"^text/rtf$",
        r"^text/csv$",
        r"^text/vtt$",
        r"^app/json$",
        r"^text/x-c$",
        r"^text/text$",
        r"^text/x-sh$",
        r"^text/json$",
        r"^text/yaml$",
        r"^text/x-go$",
        r"^text/x-js$",
        r"^text/ascii$",
        r"^plain/text$",
        r"^text/x-csh$",
        r"^text/x-log$",
        r"^text/vcard$",
        r"^text/x-tex$",
        r"^text/plain$",
        r"^text/x-wiki$",
        r"^text/x-diff$",
        r"^text/x-perl$",
        r"^text/x-chdr$",
        r"^text/x-json$",
        r"^text/x-csrc$",
        r"^text/turtle$",
        r"^text/webloc$",
        r"^text/x-vcard$",
        r"^text/calendar$",
        r"^text/x-ndjson$",
        r"^text/x-bibtex$",
        r"^text/uri-list$",
        r"^text/markdown$",
        r"^text/x-python$",
        r"^text/directory$",
        r"^text/x-amzn-ion$",
        r"^text/javsacript$",
        r"^text/ecmascript$",
        r"^application/json$",
        r"^text/x-vcalendar$",
        r"^model/gltf\+json$",
        r"^text/x-component$",
        r"^application/text$",
        r"^text/x-html-parts$",
        r"^application/jsonp$",
        r"^text/x-javascript$",
        r"^text/event-stream$",
        r"^text/vnd\.graphviz$",
        r"^application/json-p$",
        r"^application/ld\+json$",
        r"^application/x-ndjson$",
        r"^application/hr\+json$",
        r"^application/ion\+json$",
        r"^application/hal\+json$",
        r"^text/txtcharset=utf-8$",
        r"^application/geo\+json$",
        r"^application/feed\+json$",
        r"^applicaiton/jasvascript$",
        r"^application/v3\.25\+json$",
        r"^application/json,charset=",
        r"^application/v3\.24\+json$",
        r"^application/schema\+json$",
        r"^application/stream\+json$",
        r"^application/problem\+json$",
        r"^text/0\.4/hammer\.min\.js$",
        r"^application/expanded\+json$",
        r"^text/x-handlebars-template$",
        r"^application/vnd\.api\+json$",
        r"^application/x-thrift\+json$",
        r"^application/json\+protobuf$",
        r"^application/manifest\+json$",
        r"^application/importmap\+json$",
        r"^application/x-amz-json-1\.1$",
        r"^text/vnd\.turbo-stream\.html$",
        r"^text/vnd\.trolltech\.linguist$",
        r"^application/jsoncharset=UTF-8$",
        r"^text/x-comma-separated-values$",
        r"^application/linkset\+json$",
        r"^application/x-ipynb\+json$",
        r"^application/jwk-set\+json$",
        r"^application/activity\+json$",
        r"^application/vnd\.geo\+json$",
        r"^application/x-amz-json-1\.0$",
        r"^application/vnd\.s\.v1\+json$",
        r"^application/vnd\.siren\+json$",
        r"^Content-Type:application/json$",
        r"^:application/application/json$",
        r"^application/vnd\.bestbuy\+json$",
        r"^application/vnd\.1cbn\.v1+json$",
        r"^application/vnd\.1cbn\.v1\+json$",
        r"^application/sparql-results\+json$",
        r"^application/vnd\.imgur\.v1\+json$",
        r"^application/vnd\.adobe\.dex\+json$",
        r"^application/json,application/json$",
        r"^application/vnd\.solid-v1\.0\+json$",
        r"^application/graphql-response\+json$",
        r"^application/speculationrules\+json$",
        r"^application/vnd\.vimeo\.user\+json$",
        r"^application/vnd\.wg\.cds_api\+json$",
        r"^application/vnd\.urbanairship\+json$",
        r"^application/vnd\.vimeo\.album\+json$",
        r"^application/vnd\.vimeo\.video\+json$",
        r"^application/amazonui-streaming-json$",
        r"^application/vnd\.vimeo\.error\+json$",
        r"^application/vnd\.oai\.openapi\+json$",
        r"^application/vnd\.com\.amazon\.api\+json$",
        r"^application/vnd\.treasuredata\.v1\+json$",
        r"^application/vnd\.github-octolytics\+json$",
        r"^application/vnd\.mangahigh\.api-v1\+json$",
        r"^application/vnd\.maxmind\.com-city\+json$",
        r"^application/vnd\.initializr\.v2\.2\+json$",
        r"^application/vnd\.radio-canada\.neuro\+json$",
        r"^application/vnd\.vimeo\.profilevideo\+json$",
        r"^application/vnd\.oracle\.adf\.version\+json$",
        r"^application/vnd\.maxmind\.com-country\+json$",
        r"^application/vnd\.treasuredata\.v1\.js\+json$",
        r"^application/vnd\.disney\.error\.v1\.0\+json$",
        r"^application/vnd\.vimeo\.currency\.json\+json$",
        r"^application/vnd\.vimeo\.video\.texttrack\+json$",
        r"^application/vnd\.contentful\.delivery\.v1\+json$",
        r"^application/vnd\.maxmind\.com-insights\+json$",
        r"^application/vnd\.adobe\.error-response\+json$",
        r"^application/vnd\.vimeo\.profilesection\+json$",
        r"^application/vnd\.spring-boot\.actuator\.v3\+json$",
        r"^application/vnd\.vimeo\.marketplace\.skill\+json$",
        r"^application/vnd\.disney\.field\.error\.v1\.0\+json$",
        r"^application/vnd\.oracle\.adf\.resourcecollection\+json$",
        r"^application/vnd\.vmware\.horizon\.manager\.branding\+json$",
        r"^application/vnd\.vimeo\.live\.interaction_room_status\+json$",
        r"^application/vnd\.abc\.terminus\.content\+json$",
        r"^application/vnd\.maxmind\.com-error\+json$",
        r"^application/vnd\.inveniordm\.v1\+json$",
        r"^application/vnd\.vimeo\.credit\+json$",
        r"^application/vnd\.vimeo\.comment\+json$",
        r"^application/vnd\.vimeo\.location\+json$",
        r"^application/json\+containerv1-server$",
        r"^application/json-amazonui-streaming$",
    ]

content_type_image_regex = [
        r"^png$",
        r"^webp$",
        r"^jpeg$",
        r"^webpx$",
        r"^.jpeg$",
        r"^image/$",
        r"^image$",
        r"^img/jpeg$",
        r"^image/\*$",
        r"^image/any$",
        r"^image/bmp$",
        r"^image/gif$",
        r"^image/ico$",
        r"^image/jp2$",
        r"^image/jpg$",
        r"^image/pbf$",
        r"^image/png$",
        r"^image/svg$",
        r"^(null)/ico$",
        r"^image/heic$",
        r"^image/fits$",
        r"^image/apng$",
        r"^image/avif$",
        r"^image/jpeg$",
        r"^image/tiff$",
        r"^image/webp$",
        r"^image/x-ico$",
        r"^image/pjpeg$",
        r"^image/x-png$",
        r"^image/x-eps$",
        r"^\(null\)/ico$",
        r"^image/dicomp$",
        r"^image/x-icon$",
        r"^image/\{png\}$",
        r"^data:image/png$",
        r"^image/vnd\.dwg$",
        r"^image/svg\+xml$",
        r"^image/x-ms-bmp$",
        r"^image/vnd\.djvu$",
        r"^image/x-xbitmap$",
        r"^image/x-photoshop$",
        r"^image/x-coreldraw$",
        r"^image/x-cmu-raster$",
        r"^image/vnd\.wap\.wbmp$",
        r"^image/x\.fb\.keyframes$",
        r"^image/vnd\.microsoft\.icon$",
        r"^image/vnd\.adobe\.photoshop$",
        r"^application/jpg$",
    ]

content_type_midi_regex = [
        r"^audio/midi$",
        r"^audio/sp-midi$",
    ]

content_type_audio_regex = [
        r"^audio/xm$",
        r"^audio/ogg$",
        r"^audio/mp3$",
        r"^audio/mp4$",
        r"^audio/wav$",
        r"^audio/aac$",
        r"^audio/m4a$",
        r"^audio/s3m$",
        r"^audio/wave$",
        r"^audio/MP2T$",
        r"^audio/webm$",
        r"^audio/flac$",
        r"^audio/mpeg$",
        r"^audio/opus$",
        r"^audio/x-m4a$",
        r"^audio/x-m4p$",
        r"^audio/x-rpm$",
        r"^audio/x-s3m$",
        r"^audio/x-wav$",
        r"^audio/mpeg3$",
        r"^audio/x-aiff$",
        r"^audio/x-flac$",
        r"^audio/unknown$",
        r"^audio/mpegurl$",
        r"^audio/x-scpls$",
        r"^audio/x-ms-wma$",
        r"^audio/prs\.sid$",
        r"^audio/mp4a-latm$",
        r"^application/mp3$",
        r"^audio/x-mpegurl$",
        r"^application/mp4$",
        r"^audio/x-oggvorbis$",
        r"^audio/x-pn-realaudio$",
        r"^application/octetstream$",
        r"^application/octet-stream$",
        r"^application/x-octet-stream$",
        r"^audio/x-pn-realaudio-plugin$",
        r"^application/vnd\.rn-realmedia$",
    ]

content_type_video_regex = [
        r"^video/mp4$",
        r"^video/ogg$",
        r"^video/f4v$",
        r"^video/3gpp$",
        r"^video/m2ts$",
        r"^video/webm$",
        r"^video/MP2T$",
        r"^video/mpeg$",
        r"^video/x-m4v$",
        r"^video/x-flv$",
        r"^video/x-ms-wm$",
        r"^video/x-ms-wmv$",
        r"^video/x-ms-asf$",
        r"^application/ogg$",
        r"^application/wmv$",
        r"^application/avi$",
        r"^application/mp4$",
        r"^video/x-msvideo$",
        r"^video/quicktime$",
        r"^application/mp4$",
        r"^video/x-matroska$",
        r"^video/iso.segment$",
        r"^application/x-mpegurl$",
        r"^video/vnd\.objectvideo$",
        r"^application/octetstream$",
        r"^application/vnd\.ms-asf$",
        r"^application/octet-stream$",
        r"^video/vnd\.dlna\.mpeg-tts$",
        r"^application/x-octet-stream$",
        r"^application/x-shockwave-flash$",
        r"^application/vnd\.apple\.mpegurl$",
        r"^application/vnd\.adobe\.flash\.movie$",
        r"^application/mp4,audio/mp4,video/mp4,video/vnd\.objectvideo$",
        ]

content_type_pdf_regex = [
        r"^adobe/pdf$",
        r"^application/pdf$",
        r"^application/\.pdf$",
        r"^application/x-pdf$",
        r"^application/pdfcontent-length:",
    ]

content_type_doc_regex = [
        r"^application/doc$",
        r"^application/xls$",
        r"^application/xlsx$",
        r"^application/docx$",
        r"^application/msword$",
        r"^application/msexcel$",
        r"^application/ms-excel$",
        r"^application/x-msexcel$",
        r"^application/vnd\.visio$",
        r"^application/vnd\.ms-excel$",
        r"^application/vnd\.ms-visio\.drawing$",
        r"^application/vnd\.ms-word\.document\.12$",
        r"^application/vnd\.ms-excel\.openxmlformat$",
        r"^application/vnd\.oasis\.opendocument\.text$",
        r"^application/vnd\.ms-excel\.sheet\.macroenabled\.12$",
        r"^application/vnd\.ms-powerpoint\.slideshow\.macroEnabled\.12$",
        r"^application/vnd\.openxmlformats-officedocument\.spreadsheetml\.sheet$",
        r"^application/vnd\.openxmlformats-officedocument\.presentationml\.slideshow",
        r"^application/vnd\.openxmlformats-officedocument\.wordprocessingml\.document$",
        r"^application/vnd\.openxmlformats-officedocument\.wordprocessingml\.template$",
        r"^application/vnd\.openxmlformats-officedocument\.presentationml\.presentation$",
        ]

content_type_database_regex = [
        r"^application/sql$",
        r"^application/msaccess$",
        r"^application/x-msaccess$",
        ]

content_type_font_regex = [
        r"^woff$",
        r"^woff2$",
        r"^font/eot$",
        r"^font/ttf$",
        r"^font/otf$",
        r"^file/woff$",
        r"^font/sfnt$",
        r"^image/otf$",
        r"^font/woff$",
        r"^x-font/ttf$",
        r"^font/woff2$",
        r"^fonts/woff2$",
        r"^font/x-woff$",
        r"^x-font/woff$",
        r"^font/x-woff2$",
        r"^font/truetype$",
        r"^font/opentype$",
        r"^font/font-woff$",
        r"^\(null\)/woff2$",
        r"^font/font-woff2$",
        r"^application/ttf$",
        r"^application/font$",
        r"^application/woff$",
        r"^application/x-font$",
        r"^application/x-woff$",
        r"^application/x-woff2$",
        r"^application/font-otf$",
        r"^application/font-ttf$",
        r"^application/font-sfnt$",
        r"^application/font-woff$",
        r"^application/x-font-ttf$",
        r"^application/x-font-otf$",
        r"^application/font/woff2$",
        r"^application/font-woff2$",
        r"^application/x-font-woff$",
        r"^application/x-font-woff2$",
        r"^application/x-font-truetype$",
        r"^application/x-font-opentype$",
        r"^value=application/x-font-woff2$",
        r"^application/vnd\.ms-fontobject$",
        r"^application/font-woff2,font/woff2$",
        r"^font/woff2\|application/octet-stream\|font/x-woff2$",
        ]

content_type_torrent_regex = [
        r"^application/x-bittorrent$",
        r"^application/octetstream$",
        r"^application/octet-stream$",
        r"^application/x-octet-stream$",
        ]

content_type_compressed_regex = [
        r"^multipart/x-zip$",
        r"^application/zip$",
        r"^application/rar$",
        r"^application/gzip$",
        r"^application/x-bzip$",
        r"^application/x-xz$",
        r"^application/\.rar$",
        r"^application/\.zip$",
        r"^application/x-zip$",
        r"^application/x-rar$",
        r"^application/x-tar$",
        r"^application/x-lzma$",
        r"^application/x-gzip$",
        r"^application/x-bzip2$",
        r"^application/vnd\.rar$",
        r"^application/x-tar-gz$",
        r"^application/x-compress$",
        r"^application/octetstream$",
        r"^application/octet-stream$",
        r"^application/x-octet-stream$",
        r"^application/x-7z-compressed$",
        r"^application/x-rar-compressed$",
        r"^application/x-zip-compressed$",
        r"^application/x-gtar-compressed$",
        r"^application/vnd\.ms-cab-compressed$",
        r"^application/x-zip-compressedcontent-length:",
        r"^application/vnd\.adobe\.air-application-installer-package\+zip$",
    ]

content_type_all_others_regex = [
        r"^$",
        r"^-$",
        r"^js$",
        r"^\*$",
        r"^None$",
        r"^null$",
        r"^file$",
        r"^\*/\*$",
        r"^binary$",
        r"^unknown$",
        r"^\(null\)$",
        r"^\(none\)$",
        r"^text/css$",
        r"^redirect$",
        r"^model/usd$",
        r"^model/stl$",
        r"^model/obj$",
        r"^model/step$",
        r"^test/plain$",
        r"^text/octet$",
        r"^text/x-scss$",
        r"^application$",
        r"^Content-Type$",
        r"^octet/stream$",
        r"^cms/redirect$",
        r"^message/news$",
        r"^text/x-matlab$",
        r"^inode/x-empty$",
        r"^text/x-invalid$",
        r"^application/js$",
        r"^application/\*$",
        r"^model/vnd\.mts$",
        r"^text/x-haskell$",
        r"^message/rfc822$",
        r"^application/jsv$",
        r"^unknown/unknown$",
        r"^multipart/mixed$",
        r"^application/cgi$",
        r"^text/javascript$",
        r"^application/xml$",
        r"^application/x-j$",
        r"^application/jwt$",
        r"^application/rtf$",
        r"^application/csv$",
        r"^application/acad$",
        r"^application/x-po$",
        r"^application/mbox$",
        r"^application/epub$",
        r"^application/node$",
        r"^application/smil$",
        r"^application/wasm$",
        r"^application/x-js$",
        r"^application/mobi$",
        r"^application/save$",
        r"^application/null$",
        r"^application/zlib$",
        r"^application/x-sh$",
        r"^application/empty$",
        r"^application/x-cbr$",
        r"^text/plaincharset:",
        r"^chemical/x-cerius$",
        r"^application/x-rpm$",
        r"^application/x-twb$",
        r"^application/x-xcf$",
        r"^application/x-msi$",
        r"^application/x-xar$",
        r"^application/proto$",
        r"^model/gltf-binary$",
        r"^application/x-shar$",
        r"^application/x-ruby$",
        r"^application/x-frpc$",
        r"^application/x-tgif$",
        r"^application/x-perl$",
        r"^application/binary$",
        r"^application/turtle$",
        r"^application/x-doom$",
        r"^application/x-troff$",
        r"^text/remix-deferred$",
        r"^binary/octet-stream$",
        r"^application/express$",
        r"^multipart/form-data$",
        r"^application/x-trash$",
        r"^application/unknown$",
        r"^application/xml-dtd$",
        r"^application/x-empty$",
        r"^application/x-blorb$",
        r"^application/java-vm$",
        r"^application/msgpack$",
        r"^application/rfc\+xml$",
        r"^application/x-netcdf$",
        r"^application/gml\+xml$",
        r"^chemical/x-molconn-Z$",
        r"^application/x-nozomi$",
        r"^application/x-adrift$",
        r"^application/x-binary$",
        r"^application/rdf\+xml$",
        r"^application/download$",
        r"^application/rss\+xml$",
        r"^application/x-msword$",
        r"^application/pgp-keys$",
        r"^application/x-subrip$",
        r"^application/x-bibtex$",
        r"^application/pkix-crl$",
        r"^httpd/unix-directory$",
        r"^application/x-stuffit$",
        r"^application/calques3d$",
        r"^application/n-triples$",
        r"^application/vnd\.smaf$",
        r"^application/ttml\+xml$",
        r"^application/xslt\+xml$",
        r"^application/dash\+xml$",
        r"^application/x-dosexec$",
        r"^application/epub\+zip$",
        r"^application/atom\+xml$",
        r"^application/pkix-cert$",
        r"^application/smil\+xml$",
        r"^text/javascript=UTF-8$",
        r"^application/x-zmachine$",
        r"^application/typescript$",
        r"^application/x-director$",
        r"^application/postscript$",
        r"^application/x-rss\+xml$",
        r"^application/ecmascript$",
        r"^application/x-protobuf$",
        r"^application/pkcs7-mime$",
        r"^application/javascript$",
        r"^application/oct-stream$",
        r"^application/x-httpd-cgi$",
        r"^application/dns-message$",
        r"^application/vnd\.ms-wpl$",
        r"^application/x-asciicast$",
        r"^applications/javascript$",
        r"^javascriptcharset=UTF-8$",
        r"^chemical/x-galactic-spc$",
        r"^application/vnd\.yt-ump$",
        r"^application/octetstream$",
        r"^application/x-xpinstall$",
        r"^application/x-httpd-php$",
        r"^application/x-directory$",
        r"^application/x-troff-man$",
        r"^application/mac-binhex40$",
        r"^application/encrypted-v2$",
        r"^application/java-archive$",
        r"^application/x-javascript$",
        r"^application/x-msdownload$",
        r"^application/octet-stream$",
        r"^application/vnd\.ms-word$",
        r"^application/x-executable$",
        r"^application/marcxml\+xml$",
        r"^javascript charset=UTF-8$",
        r"^multipart/x-mixed-replace$",
        r"^application/pgp-encrypted$",
        r"^application/x-base64-frpc$",
        r"^application/pgp-signature$",
        r"^application/x-ms-manifest$",
        r"^application/x-mobi8-ebook$",
        r"^application/grpc-web-text$",
        r"^application/force-download$",
        r"^application/vnd\.visionary$",
        r"^application/x-java-archive$",
        r"^application/x-octet-stream$",
        r"^application/x-x509-ca-cert$",
        r"^x-application/octet-stream$",
        r"^application/mac-compactpro$",
        r"^application/x-endnote-refer$",
        r"^application/vnd\.olpc-sugar$",
        r"^text/x-unknown-content-type$",
        r"^application/grpc-web\+proto$",
        r"^application/x-msdos-program$",
        r"^application/x-iso9660-image$",
        r"^application/x-csp-hyperevent$",
        r"^application/x-ms-application$",
        r"^application/vnd\.ms-opentype$",
        r"^application/x-debian-package$",
        r"^application/x-httpd-ea-php54$",
        r"^application/vnd\.ms-htmlhelp$",
        r"^application/x-shared-scripts$",
        r"^application/x-java-jnlp-file$",
        r"^application/x-httpd-ea-php71$",
        r"^application/rls-services\+xml$",
        r"^application/vnd\.ogc\.wms_xml$",
        r"^application/x-apple-diskimage$",
        r"^application/privatetempstorage$",
        r"^application/x-chrome-extension$",
        r"^application/x-mobipocket-ebook$",
        r"^application/vnd\.ms-powerpoint$",
        r"^application/sparql-results\+xml$",
        r"^application/vnd\.openxmlformats$",
        r"^application/apple\.vnd\.mpegurl$",
        r"^application/vnd\.ms-officetheme$",
        r"^application/vnd\.wv\.csp\+wbxml$",
        r"^application/x-ms-dos-executable$",
        r"^application/vnd\.geogebra\.file$",
        r"^application/grpc-web-text\+proto$",
        r"^application/vnd\.lotus-screencam$",
        r"^application/x-pkcs7-certificates$",
        r"^application/x-www-form-urlencoded$",
        r"^application/vnd\.google-earth\.kmz$",
        r"^application/x-typekit-augmentation$",
        r"^application/x-unknown-content-type$",
        r"^application/octet-stream,text/html$",
        r"^application/octet-stream,text/plain$",
        r"^application/x-research-info-systems$",
        r"^application/vnd\.mapbox-vector-tile$",
        r"^application/octet-stream,atext/plain$",
        r"^application/vnd\.cas\.services\+yaml$",
        r"^application/x-redhat-package-manager$",
        r"^application/vnd\.groove-tool-template$",
        r"^application/octet-streamCharset=UTF-8$",
        r"^application/vnd\.apple\.installer\+xml$",
        r"^application/opensearchdescription\+xml$",
        r"^application/vnd\.google-earth\.kml\+xml$",
        r"^text/javascript/application/x-javascript$",
        r"^application/vnd\.android\.package-archive$",
        r"^application/javascript,application/javascript$",
        r"^application/javascriptapplication/x-javascript$",
        r"^application/javascript,application/x-javascript$",
        r"^application/vnd\.oasis\.opendocument\.spreadsheet$",
        r"^application/vnd\.google\.octet-stream-compressible$",
        r"^application/vnd\.oasis\.opendocument\.presentation$",
        r"^application/vnd\.openxmlformats-officedocument\.spre$",
        r"^application/vnd\.oasis\.opendocument\.formula-template$",
    ]


def function_for_url(regexp_list):
    def get_url_function(f):
        for regexp in regexp_list:
            url_functions.append((re.compile(regexp, flags=re.I | re.U), f))
        return f
    return get_url_function


@function_for_url(
    [
        r"^(\/|\.\.\/|\.\/)",
        r"^[0-9\-\./\?=_\&\s%@<>\(\);\+!,\w\$\'–’—”“a°§£Ã¬´c�í¦a]+$",
        r"^[0-9\-\./\?=_\&\s%@<>\(\);\+!,\w\$\'–’—”“a°§£Ã¬´c]*[\?\/][0-9\-\./\?=_\&\s%@<>\(\);\+!,\w\$\'–’—”“a°§£Ã¬:\"¶c´™*]+$",
    ]
)
def relative_url(args):
    out_url = urljoin(args['parent_url'], args['url'])
    parent_host = urlsplit(args['parent_url']).hostname
    return [{
        "url": out_url,
        "visited": False,
        "source": "relative_url",
        "parent_host": parent_host,
        "host": urlsplit(out_url).hostname,
    }]

@function_for_url([r"^https*://", r"^ftp://"])
def full_url(args):
    parent_host = urlsplit(args['parent_url']).hostname
    return [{
        "url": args['url'],
        "visited": False,
        "source": "full_url",
        "parent_host": parent_host,
        "host": urlsplit(args['url']).hostname,
    }]

@function_for_url([r"^(mailto:|maillto:|maito:|mail:|malito:|mailton:|\"mailto:|emailto:|maltio:|mainto:|E\-mail:|mailtfo:|mailtp:|mailtop:|mailo:|mail to:|Email para:|email :|email:|E-mail: |mail-to:|maitlo:|mail.to:)"])
def email_url(args):
    address_search = re.search(
        r"^(mailto:|maillto:|maito:|mail:|malito:|mailton:|\"mailto:|emailto:|maltio:|mainto:|E\-mail:|mailtfo:|mailtp:|mailtop:|mailo:|mail to:|Email para:|email :|email:|E-mail: |mail-to:|maitlo:|mail.to:)(.*)",
        args['url'],
        flags=re.I | re.U,
    )
    if address_search:
        address = address_search.group(2)
        if re.match(r"^([A-Za-z0-9]+[._-])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+$", address):
            parent_host = urlsplit(args['parent_url']).hostname
            return [{
                "url": args['parent_url']+'|'+address,
                "emails": [address],
                "visited": True,
                "source": "email_url",
                "parent_host": parent_host,
                "host": parent_host,
                "isopendir": False
            }]
    return []

def get_words_from_soup(soup) -> list[str]:
    text_parts = [
        t for t in soup.find_all(string=True)
        if t.parent.name not in soup_tag_blocklist
    ]
    combined_text = " ".join(text_parts)
    return extract_top_words_from_text(combined_text)

def sanitize_url(
        url,
        debug=True,
        skip_log_tags=['FINAL_NORMALIZE',
                       'STRIP_WHITESPACE',
                       'NORMALIZE_PATH_SLASHES']):
    if skip_log_tags is None:
        skip_log_tags = set()

    def log_change(reason, before, after):
        if before != after and reason not in skip_log_tags and debug:
            print(f"\033[91m[{reason}] URL sanitized \
                  from -{before}- to -{after}-\033[00m")

    def clean_hostname_with_userinfo(netloc, scheme):
        """
        Cleans netloc, preserving valid username:password@host:port
        patterns. Removes invalid characters, strips default ports, and
        validates port range.
        """
        userinfo = ''
        host_port = netloc

        if '@' in netloc:
            userinfo, host_port = netloc.split('@', 1)
            # Clean userinfo (basic, do not over-sanitize)
            userinfo = ''.join(c for c in userinfo if c.isprintable())

        if ':' in host_port:
            host, port = host_port.rsplit(':', 1)
            host = ''.join(c for c in host if c.isalnum() or c in '-.')
            if port.isdigit():
                port_num = int(port)
                if (scheme == 'http' and port == '80') or \
                        (scheme == 'https' and port == '443'):
                    port = ''
                elif 1 <= port_num <= 65535:
                    pass  # valid
                else:
                    port = ''
            else:
                port = ''
        else:
            host = ''.join(c for c in host_port if c.isalnum() or c in '-.')
            port = ''

        result = host
        if port:
            result += f':{port}'
        if userinfo:
            result = f'{userinfo}@{result}'
        return result

    def safe_normalize_path_slashes(path):
        # Split on any embedded full http(s) URL and keep them intact
        segments = re.split(r'(/https?://)', path)
        result = []
        for i in range(0, len(segments), 2):
            part = segments[i]
            part = re.sub(r'/{2,}', '/', part)
            result.append(part)
            if i + 1 < len(segments):
                # re-append the "/https://" or "/http://"
                result.append(segments[i + 1])
        return ''.join(result)

    pre_sanitize = url
    if not url or not isinstance(url, str):
        return ""

    url = url.strip()
    log_change("STRIP_WHITESPACE", pre_sanitize, url)
    pre_sanitize = url
    special_quote_pairs = [
        (r'^"(.*)"$', r'\1'),
        (r"^'(.*)'$", r'\1'),
        (r'^\u201C(.*)\u201D$', r'\1'),
        (r'^\u2018(.*)\u2019$', r'\1'),
        (r'^"(.*)″$', r'\1'),
    ]
    for pattern, replacement in special_quote_pairs:
        cleaned = re.sub(pattern, replacement, url)
        log_change("SPECIAL_QUOTE_CLEAN", url, cleaned)
        url = cleaned

    scheme_fixes = [
        (r'^ps://', 'https://'), (r'^ttps://', 'https://'),
        (r'^htpps://', 'https://'), (r'^httpp://', 'https://'),
        (r'^http:s//', 'https://'), (r'^hthttps://', 'https://'),
        (r'^httsp://', 'https://'), (r'^htts://', 'https://'),
        (r'^htttps://', 'https://'), (r'^https:https://', 'https://'),
        (r'^https https://', 'https://'), (r'^httpshttps://', 'https://'),
        (r'^https://https://', 'https://'), (r'^"https://', 'https://'),
        (r'^httpd://', 'https://'), (r'^htps://', 'https://'),
        (r'^https: //', 'https://'), (r'^https : //', 'https://'),
        (r'^http2://', 'https://'), (r'^https%3A//', 'https://'),
        (r'^%20https://', 'https://'), (r'^htto://', 'http://'),
        (r'^htt://', 'http://'), (r'^htp://http//', 'http://'),
        (r'^htp://', 'http://'), (r'^hhttp://', 'http://'),
        (r'^http:/http://', 'http://'), (r'^http:www', 'http://www'),
        (r'^htttp://', 'http://'), (r'^ttp://', 'http://'),
        (r'^%20http://', 'http://'), (r'^%22mailto:', 'mailto:'),
        (r'^httpqs://', 'https://www.'), (r'^://', 'https://')
    ]
    for pattern, replacement in scheme_fixes:
        fixed = re.sub(pattern, replacement, url)
        log_change("FIX_SCHEME", url, fixed)
        url = fixed

    cleaned = re.sub(r'^[a-zA-Z."(´]https://', 'https://', url)
    log_change("PREFIX_CLEAN_HTTPS", url, cleaned)
    url = cleaned
    cleaned = re.sub(r'^[a-zA-Z."(´]http://', 'http://', url)
    log_change("PREFIX_CLEAN_HTTP", url, cleaned)
    url = cleaned

    url = re.sub(r'^(https?:)/+', r'\1//', url)
    log_change("FIX_SCHEME_SLASHES", pre_sanitize, url)
    try:
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        netloc = clean_hostname_with_userinfo(parsed.netloc, scheme)

        if not netloc and parsed.path.startswith('/') and scheme:
            parts = parsed.path.lstrip('/').split('/', 1)
            if parts and '.' in parts[0]:
                netloc = clean_hostname_with_userinfo(parts[0], scheme)
                path = '/' + (parts[1] if len(parts) > 1 else '')
                rebuilt = urlunsplit(
                        (scheme,
                         netloc,
                         path,
                         parsed.query,
                         parsed.fragment))
                log_change("FIX_NETLOC_IN_PATH", url, rebuilt)
                url = rebuilt
        else:
            path = re.sub(r'/{2,}', '/', parsed.path)
            rebuilt = urlunsplit(
                    (scheme,
                     netloc,
                     path,
                     parsed.query,
                     parsed.fragment))
            log_change("NORMALIZE_PATH_SLASHES", url, rebuilt)
            url = rebuilt
    except Exception:
        fallback = re.sub(r'(https?://[^/]+)/{2,}', r'\1/', url)
        log_change("FALLBACK_SLASH_FIX", url, fallback)
        url = fallback

    try:
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        if ':' in netloc:
            host, port = netloc.split(':', 1)
            if (
                    (scheme == 'http' and port == '80') or
                    (scheme == 'https' and port == '443')
               ):
                netloc = host

        path = safe_normalize_path_slashes(parsed.path)
        normalized = urlunsplit((scheme, netloc, path, parsed.query, ''))
        log_change("FINAL_NORMALIZE", url, normalized)
        return normalized.strip()
    except Exception:
        return url.strip()


def function_for_content_type(regexp_list):
    def get_content_type_function(f):
        for regexp in regexp_list:
            content_type_functions.append((re.compile(regexp, flags=re.I | re.U), f))
        return f
    return get_content_type_function

async def get_links_page(page, base_url: str) -> list[str]:
    links = set()
    try:
        hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
        links.update(hrefs)
        hrefs = await page.eval_on_selector_all("link[href]", "els => els.map(e => e.href)")
        links.update(hrefs)
        srcs = await page.eval_on_selector_all("script[src]", "els => els.map(e => e.src)")
        links.update(srcs)
        srcs = await page.eval_on_selector_all("img[src]", "els => els.map(e => e.src)")
        links.update(srcs)
    except Exception as e:
        print(f"Error extracting links from {base_url}: {e}")
    return list(links)

def get_words(text: bytes | str) -> list[str]:
    if not text:
        return []
    if isinstance(text, bytes):
        try:
            text = text.decode('utf-8', errors='replace')
        except Exception:
            return []
    return extract_top_words_from_text(text)


async def get_words_from_page(page) -> list[str]:
    # JS snippet that walks the DOM and collects text nodes
    js = """
    () => {
        const blocklist = new Set(["script", "style", "noscript", "iframe"]);
        function getTextNodes(node) {
            let texts = [];
            if (node.nodeType === Node.TEXT_NODE) {
                const text = node.textContent.trim();
                if (text.length > 0) {
                    texts.push(text);
                }
            } else if (node.nodeType === Node.ELEMENT_NODE && !blocklist.has(node.tagName.toLowerCase())) {
                for (const child of node.childNodes) {
                    texts = texts.concat(getTextNodes(child));
                }
            }
            return texts;
        }
        return getTextNodes(document.body);
    }
    """
    text_parts = await page.evaluate(js)
    combined_text = " ".join(text_parts)
    return extract_top_words_from_text(combined_text)

@function_for_content_type(content_type_all_others_regex)
async def content_type_ignore(args):
    return { args['url'] : 
            {
        "url": args['url'],
        "content_type": args['content_type'],
        "visited": True,
        "source": 'content_type_all_others_regex',
        "parent_host": args['parent_host'] }
            }

@function_for_content_type(content_type_plain_text_regex)
async def content_type_plain_text(args):
    words = ''
    if EXTRACT_WORDS:
        words = get_words(args['content'])
    return { args['url'] : 
            {
        "url": args['url'],
        "content_type": args['content_type'],
        "isopendir": False,
        "visited": True,
        "words": words,
        "source": 'content_type_plain_text_regex',
        "parent_host": args['parent_host'] }
    }

@function_for_content_type(content_type_font_regex)
async def content_type_fonts(args):

    if DOWNLOAD_FONTS:
        raw_content = args.get('raw_content')
        if not raw_content:
            results["crawledlinks"].add(args['url'])
            print(f"################# {args['url']}")
            return {}

        url = args['url']
        base_filename = os.path.basename(urlparse(url).path)
        try:
            decoded_name = unquote(base_filename)
        except Exception:
            decoded_name = base_filename
        # Separate extension (e.g., ".pdf")
        name_part, ext = os.path.splitext(decoded_name)
        # Sanitize both parts
        name_part = re.sub(r"[^\w\-.]", "_", name_part)
        ext = re.sub(r"[^\w\-.]", "_", ext)
        # Create URL hash prefix (always fixed length)
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        # Max length for entire filename (255) minus hash + dash + extension + safety margin
        max_name_length = MAX_FILENAME_LENGTH - len(url_hash) - 1 - len(ext)
        if len(name_part) > max_name_length:
            name_part = name_part[:max_name_length - 3] + "..."
        safe_filename = f"{url_hash}-{name_part}{ext}"
        filepath = os.path.join(FONTS_FOLDER, safe_filename)
        with open(filepath, "wb") as f:
            f.write(args['raw_content'])
        return  { args['url']:
                {   
                    "url":args['url'],
                    "content_type":args['content_type'],
                    "source":"content_type_fonts_download",
                    "isopendir":False,
                    "visited":True,
                    "parent_host":args['parent_host'],
                    "filename":safe_filename}
                }
    return  { args['url']:
            {   
                "url":args['url'],
                "content_type":args['content_type'],
                "source":"content_type_fonts",
                "isopendir":False,
                "visited":True,
                "parent_host":args['parent_host']}
            }


@function_for_content_type(content_type_video_regex)
async def content_type_videos(args):
    if DOWNLOAD_VIDEOS:
        url = args['url']
        base_filename = os.path.basename(urlparse(url).path)
        try:
            decoded_name = unquote(base_filename)
        except Exception:
            decoded_name = base_filename
        # Separate extension (e.g., ".pdf")
        name_part, ext = os.path.splitext(decoded_name)
        # Sanitize both parts
        name_part = re.sub(r"[^\w\-.]", "_", name_part)
        ext = re.sub(r"[^\w\-.]", "_", ext)
        # Create URL hash prefix (always fixed length)
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        # Max length for entire filename (255) minus hash + dash + extension + safety margin
        max_name_length = MAX_FILENAME_LENGTH - len(url_hash) - 1 - len(ext)
        if len(name_part) > max_name_length:
            name_part = name_part[:max_name_length - 3] + "..."
        safe_filename = f"{url_hash}-{name_part}{ext}"
        filepath = os.path.join(VIDEOS_FOLDER, safe_filename)
        with open(filepath, "wb") as f:
            f.write(args['raw_content'])
        return  { args['url']:
                {   
                    "url":args['url'],
                    "content_type":args['content_type'],
                    "source":"content_type_video_regex_download",
                    "isopendir":False,
                    "visited":True,
                    "parent_host":args['parent_host'],
                    "filename":safe_filename}
                }
    return  { args['url']:
            {   
                "url":args['url'],
                "content_type":args['content_type'],
                "source":"content_type_video_regex",
                "isopendir":False,
                "visited":True,
                "parent_host":args['parent_host']}
            }


@function_for_content_type(content_type_html_regex)
async def content_type_download(args):
    try:
        content = args['content']
        soup = BeautifulSoup(content, "html.parser")
    except Exception as e:
        print(f"Soup parsing error for {args['url']}: {e}")
        return False
    words = ''
    min_webcontent = ''
    raw_webcontent = ''
    if EXTRACT_WORDS:
        words = get_words_from_soup(soup)
    if EXTRACT_RAW_WEBCONTENT:
        raw_webcontent = str(soup)[:MAX_WEBCONTENT_SIZE]
    if EXTRACT_MIN_WEBCONTENT:
        min_webcontent = get_min_webcontent(soup)[:MAX_WEBCONTENT_SIZE]
    isopendir, pat  = is_open_directory(str(soup), args['url'])
    return { args['url'] : 
            {
        "url": args['url'],
        "content_type": args['content_type'],
        "isopendir": isopendir,
        "opendir_pattern": pat,
        "visited": True,
        "words": words,
        "min_webcontent": min_webcontent,
        "raw_webcontent": raw_webcontent,
        "source": 'content_type_html_regex',
        "parent_host": args['parent_host'] }
    }

def get_min_webcontent(soup) -> str:
    text_parts = [
        t.strip()  # remove leading/trailing whitespace including \n
        for t in soup.find_all(string=True)
        if t.parent.name not in soup_tag_blocklist
    ]
    # Filter out any empty strings after stripping
    text_parts = [t for t in text_parts if t]

    # Join with a single space
    combined_text = " ".join(text_parts)
    return combined_text


@function_for_content_type(content_type_image_regex)
async def content_type_images(args):
    global model
    npixels = 0
    if CATEGORIZE_NSFW or DOWNLOAD_ALL_IMAGES:
        try:
            img = Image.open(BytesIO(args['raw_content']))
            width, height = img.size
            npixels = width * height
            nsfw_probability = 0
            if img.mode == "CMYK":
                img = img.convert("RGB")
            # Check if it's a palette-based image with transparency
            if img.mode == "P" and "transparency" in img.info:
                # Convert to RGBA to handle transparency properly
                img = img.convert("RGBA")
            filename = hashlib.sha512(img.tobytes()).hexdigest() + ".png"
            if DOWNLOAD_ALL_IMAGES:
                img.save(IMAGES_FOLDER+'/' + filename, "PNG")
            if CATEGORIZE_NSFW and npixels > MIN_NSFW_RES:
                image = n2.preprocess_image(img, n2.Preprocessing.YAHOO)
                inputs = np.expand_dims(image, axis=0)
                predictions = model.predict(inputs, verbose=0)
                sfw_probability, nsfw_probability = predictions[0]
                if nsfw_probability > NSFW_MIN_PROBABILITY:
                    print('porn {} {}'.format(nsfw_probability, args['url']))
                    if DOWNLOAD_NSFW:
                        img.save(NSFW_FOLDER + '/' + filename, "PNG")
                else:
                    if DOWNLOAD_SFW:
                        img.save(SFW_FOLDER + '/' + filename, "PNG")
                return  { args['url']:
                        {   
                            "url":args['url'],
                            "content_type":args['content_type'],
                            "source":"content_type_images_nsfw_categorization",
                            "isopendir":False,
                            "visited":True,
                            "parent_host":args['parent_host'],
                            "isnsfw":float(nsfw_probability),
                            "filename":filename,
                            "resolution":npixels }
                        }
            else:
                return  { args['url']:
                        {   
                            "url":args['url'],
                            "content_type":args['content_type'],
                            "source":"content_type_images_download",
                            "isopendir":False,
                            "visited":True,
                            "parent_host":args['parent_host'],
                            "filename":filename,
                            "resolution":npixels }
                        }
        except UnidentifiedImageError as e:
            results["crawledlinks"].add(args['url'])
        except Image.DecompressionBombError as e:
            return {args['url']:
                    {
                    "url":args['url'],
                    "content_type":args['content_type'],
                    "source":"content_type_images_decompression_bomb_error",
                    "isopendir":False,
                    "visited":True,
                    "parent_host":args['parent_host'],
                    "resolution":npixels }
                    }
        except OSError:
            return {args['url']:
                    {
                    "url":args['url'],
                    "content_type":args['content_type'],
                    "source":"content_type_images_oserror",
                    "isopendir":False,
                    "visited":True,
                    "parent_host":args['parent_host'],
                    "resolution":npixels }
                    }

    return { args['url']:
            {
                "url":args['url'],
                "content_type":args['content_type'],
                "source":"content_type_images_no_download",
                "isopendir":False,
                "visited":True,
                "parent_host":args['parent_host']}
            }

def get_directory_tree(url):
    host = '://'.join(urlsplit(url)[:2])
    dtree = []
    parts = PurePosixPath(unquote(urlparse(url).path)).parts
    for iter in range(1, len(parts[0:])):
        dtree.append(str(host + '/' + '/'.join(parts[1:-iter])))
    return dtree

def is_url_block_listed(url):
    for regex in URL_REGEX_BLOCK_LIST:
        if re.search(regex, url, flags=re.I | re.U):
            return True
    return False

def is_host_allow_listed(url):
    for regex in HOST_REGEX_ALLOW_LIST:
        if re.search(regex, url, flags=re.I | re.U):
            return True
    return False

def is_host_block_listed(url):
    for regex in HOST_REGEX_BLOCK_LIST:
        if re.search(regex, url, flags=re.I | re.U):
            return True
    return False

def sanitize_content_type(content_type):
    content_type = content_type.strip()
    content_type = re.sub(r'^"(.*)"$', r"\1", content_type)  # remove surrounding quotes
    content_type = re.sub(r'^content-type:\s*', '', content_type, flags=re.I)  # remove prefix
    content_type = re.sub(r'^(.*?);.*$', r"\1", content_type)  # keep only type/subtype
    content_type = re.sub(r'\s+', '', content_type)  # remove spaces
    return content_type

soup_tag_blocklist = {
    "script", "style", "noscript", "iframe", "meta", "head", "title", "input"
}

async def get_min_webcontent_page(page) -> str:
    js = f"""
    () => {{
        const blocklist = new Set({list(soup_tag_blocklist)});
        function getTextNodes(node) {{
            let texts = [];
            if (node.nodeType === Node.TEXT_NODE) {{
                const text = node.textContent.trim();
                if (text.length > 0) texts.push(text);
            }} else if (node.nodeType === Node.ELEMENT_NODE && !blocklist.has(node.tagName.toLowerCase())) {{
                for (const child of node.childNodes) {{
                    texts = texts.concat(getTextNodes(child));
                }}
            }}
            return texts;
        }}
        return getTextNodes(document.body);
    }}
    """
    text_parts = await page.evaluate(js)
    combined_text = " ".join(text_parts)
    return combined_text[:MAX_WEBCONTENT_SIZE]

def is_open_directory(content, content_url):
    host = urlsplit(content_url)[1]
    hostnp = host.split(':')[0]

    patterns = [
        r'<title>Index of /',                                # Apache-style
        r'<h1>Index of /',                                   # Apache-style H1
        r'\[To Parent Directory\]</A>',                      # IIS-style
        r'<title>' + re.escape(host) + r' - /</title>',      # Lighttpd-style
        r'_sort=\'name\';SortDirsAndFilesName\(\)',          # h5ai
        r'<body[^>]*class="[^"]*dufs[^"]*"',                 # DUFS body
        r'<footer[^>]*>Generated by dufs',                   # DUFS footer
        r'<script[^>]*src="[^"]*dufs[^"]*"',                 # DUFS JS
        r'<div class="breadcrumbs">Folder Path</div>',
        r'<th><a href="\?C=N;O=D">Name</a></th><th><a href="\?C=M;O=A">Last modified</a></th><th><a href="\?C=S;O=A">Size</a></th><th><a href="\?C=D;O=A">Description</a></th>',
        r'<table class="sortable">\s*<thead>\s*<tr>\s*<th>Name\s*</th>\s*<th>Size\s*</th>\s*<th>Uploaded\s*</th>\s*<th>\s*</th>\s*</tr>',
        r'<title>Directory Listing</title>',
        r'<h1>Listing of /',
        r'Powered by <a class="autoindex_a" href="http://autoindex.sourceforge.net/">AutoIndex PHP Script</a>',
        r'<a href="\?C=N;O=D">\s*Name\s*</a>\s*<a href="\?C=M;O=A">\s*Last modified\s*</a>\s*<a href="\?C=S;O=A">\s*Size\s*</a>\s*<a href="\?C=D;O=A">\s*Description\s*</a>',
        r'<a href="\?C=N&amp;O=A">\s*File Name\s*</a>\s*&nbsp;\s*<a href="\?C=N&amp;O=D">\s*&nbsp;&darr;&nbsp;\s*</a></th>\s*<th style="width:20%">\s*<a href="\?C=S&amp;O=A">\s*File Size\s*</a>\s*&nbsp;\s*<a href="\?C=S&amp;O=D">\s*&nbsp;&darr;&nbsp;\s*</a>',
        r'<a href="\?C=N&amp;O=A">\s*File Name\s*</a>\s*(?:&nbsp;|\u00a0)\s*<a href="\?C=N&amp;O=D">\s*(?:&nbsp;|\u00a0)?(?:&darr;|\u2193)(?:&nbsp;|\u00a0)?\s*</a>[\s\S]*?<a href="\?C=S&amp;O=A">\s*File Size\s*</a>\s*(?:&nbsp;|\u00a0)\s*<a href="\?C=S&amp;O=D">\s*(?:&nbsp;|\u00a0)?(?:&darr;|\u2193)(?:&nbsp;|\u00a0)?\s*</a>',
        r'<meta\s+name="generator"\s+content="AList V\d+"\s*/?>',
        r'<meta\scontent="AList V\d+"\sname="generator"/?>',
        r'<div\s+id=["\']idx["\']>\s*<!--\s*do not remove\s*-->',
        r'<tr[^>]*class=["\']indexhead["\'][^>]*>.*Name.*Last modified.*Size.*Description',
        r'<pre>(?:\s*\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+(?:AM|PM)?\s+\d+\s+<a href="[^"]+">[^<]+</a>\s*<br>\s*){2,}</pre>',
        r'<html><head><title>' + hostnp + r' - /[^<]*</title></head><body><h1>' + hostnp + r' - /[^<]*</h1>',
        r'<meta\s+name=["\']description["\']\s+content=["\']Yet another directory listing, powered by Directory Lister\.["\']\s*/?>',
        r'<meta\scontent="Yet\sanother\sdirectory\slisting,\spowered\sby\sDirectory\sLister\."\sname="description"/>',
        r'<title>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\s*-\s*/</title>',
        r'<title>Index of .*?</title>',
        r'<h1>Index of .*?</h1>',
        r'<h1>文件索引.*?</h1>',
        r'Directory listing for .*',
        r'<ListBucketResult\s+xmlns=[\'\"].*?[\'\"]>',
        r'<tr\s+class=["\']indexhead["\']>\s*<th\s+class=["\']indexcolicon["\']>\s*<img\s+src=["\']/icons/blank\.gif["\']\s+alt=["\']\[ICO\]["\']\s*/?>\s*</th>\s*<th\s+class=["\']indexcolname["\']>\s*<a\s+href=["\']\?C=N;O=A["\']>\s*Name\s*</a>\s*</th>\s*<th\s+class=["\']indexcollastmod["\']>\s*<a\s+href=["\']\?C=M;O=A["\']>\s*Last\s+modified\s*</a>\s*</th>\s*<th\s+class=["\']indexcolsize["\']>\s*<a\s+href=["\']\?C=S;O=A["\']>\s*Size\s*</a>\s*</th>\s*</tr>',
        r'\.calibreRangeWrapper',
        r'<body\sstyle="font-size:medium">[a-z]*\sFolder\s*\t*<a\shref="/list\?dir=1">',
        r'<img\s+[^>]*alt="\[PARENTDIR\]"[^>]*>',
        r'<img\s+[^>]*alt="\[DIR\]"[^>]*>',
        r'\.\.\/">Parent Directory<\/a>',
        r'\.\.\/">Parent directory\/<\/a>',
        r'<a href="\.\./">\.\./</a>',
        r'https:\/\/github\.com\/DirectoryLister\/DirectoryLister',
        r'<h1>Directory \/',
        r'powered by h5ai',
        r'<h1>Directory: \/',
        r'<hr>Directory Listing Script &copy;',
        r'<a href="\.\.\/">Parent directory\/<\/a>',
        r'<a href="\?C=N&O=A">Name<\/a>',
        r'<a href="\?C=N;O=A">Name</a>',
        r'<a href="\?C=N;O=D">Name</a>',
        r'<a href="\?C=N&O=D">Name&nbsp; &#8679;<\/a>',
        r'<a href="\?C=M;O=A">Last modified</a>',
        r'<a href="\.\.\/\?C=N&amp;O=D">Parent directory\/<\/a>',
        r'<td align="center" class="powered">Powered by <a href="https://www.pcloud.com/">pCloud</a></td>',
        r'<a href="\?C=N;O=D">Name</a>',
        r'<h2>Directory listing of /</h2>',
        r'<a href="\?srt=size"><b>Размер</b></a>',
        r'<title>Directory listing of http',
        r'<input type="search" id="search" value="" class="form-control search" placeholder="Nom du fichier">',
        r'<td><a href="\?dir=\.">Parent Directory<\/a>',
        r'<a href="https://github\.com/DirectoryLister/DirectoryLister"',
    ]

    for pat in patterns:
        if re.search(pat, content, re.IGNORECASE):
            print(f'### Is open directory - {content_url} - matched pattern: {pat}')
            return True, pat
    return False, ""

def extract_top_words_from_text(text: str) -> list[str]:
    if WORDS_REMOVE_SPECIAL_CHARS:
        text = re.sub(r'[^\w\s]', ' ', text, flags=re.UNICODE)
    if WORDS_TO_LOWER:
        text = text.lower()

    words = [word for word in text.split() if
             WORDS_MIN_LEN < len(word) <= WORDS_MAX_LEN]
    most_common = Counter(words).most_common(WORDS_MAX_WORDS)
    return [word for word, _ in most_common]

#def get_links(soup, content_url):
#    tags = soup("a")
#    bulk_data = []
#    for tag in tags:
#        url = tag.get("href")
#        if not isinstance(url, str):
#            continue
#        url = sanitize_url(url)
#        # Collect URLs from all handlers
#        for regex, function in url_functions:
#            if regex.search(url):
#                results = function({'url': url, 'parent_url': content_url})
#                if results:
#                    bulk_data.extend(results)
#                break  # Only first matching handler
#
#    # Remove duplicates before insert
#    seen = set()
#    unique_bulk_data = []
#
#    for item in bulk_data:
#        url = item.get("url")
#        if not url:
#            continue  # skip items without a URL
#        if url not in seen:
#            unique_bulk_data.append(item)
#            seen.add(url)
#
#    return seen


async def auto_scroll(page, max_attempts: int = SCROLL_ATTEMPTS, delay: float = SCROLL_DELAY):
    """Scroll down until bottom or until max_attempts reached."""
    last_height = await page.evaluate("document.body.scrollHeight")
    attempts = 0

    while attempts < max_attempts:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(delay)
        new_height = await page.evaluate("document.body.scrollHeight")

        if new_height == last_height:
            break
        last_height = new_height
        attempts += 1


async def safe_content(page, retries: int = 5, delay: float = 1.0) -> str:
    """Try to get page.content(), retrying if the page is busy."""
    for i in range(retries):
        try:
            return await page.content()
        except Exception as e:
            print(f"page.content() failed (attempt {i+1}): {e}")
            await asyncio.sleep(delay)
    return ""


def is_html_content(content_type: str) -> bool:
    return any(
        re.match(pattern, content_type, re.IGNORECASE)
        for pattern in content_type_html_regex
    )


# --- main function ---

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

async def get_page_async(url: str, playwright):
    browser = await playwright.chromium.launch(headless=True)
    user_agent = ua.random
    context = await browser.new_context(user_agent=user_agent)
    page = await context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT_MS)
    parent_host = urlsplit(url)[1]
    page_data = {
        "crawledcontent": {},
        "crawledlinks": set()
    }

    async def crawl(scroll: bool = False):
        try:
            response = await page.goto(url, wait_until="domcontentloaded")
            if not response:
                print(f"Failed to load {url}")
                return None

            ctype = response.headers.get("content-type", "")
            if ctype:
                ctype = sanitize_content_type(ctype)

            if is_html_content(ctype) and scroll:
                await auto_scroll(page)
            if is_html_content(ctype):
                await page.wait_for_load_state("networkidle")

            return ctype
        except PlaywrightTimeoutError:
            print(f"Timeout fetching {url}, scroll={scroll}")
            return None
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

    content_type = await crawl(scroll=False)
    if content_type is None:
        print(f"Retrying {url} with scrolling...")
        content_type = await crawl(scroll=True)

    content_type = content_type or ""

    # Safe DOM snapshot and links
    html_text = await safe_content(page)
    links = await get_links_page(page, url)
    page_data["crawledlinks"].update(links)

    # Extract words / raw / min content
    words = await get_words_from_page(page) if EXTRACT_WORDS else ''
    raw_webcontent = str(html_text)[:MAX_WEBCONTENT_SIZE] if EXTRACT_RAW_WEBCONTENT else ''
    min_webcontent = await get_min_webcontent_page(page) if EXTRACT_MIN_WEBCONTENT else ''
    isopendir, pat = is_open_directory(str(html_text), url)


    async def handle_response(response):
        try:
            if page.is_closed():
                return

            status = response.status
            rurl = response.url
            host = urlsplit(rurl)[1]

            body_bytes = None
            content = ""
            encoding = "utf-8"

            # Always grab content-type first
            ctype = response.headers.get("content-type")

            try:
                body_bytes = await response.body()   # always grab raw bytes once
            except Exception as e:
                print(f"Could not fetch body for {rurl}: {e}")

            # Decode only if textual content
            if body_bytes and ctype and any(t in ctype for t in ["text", "json", "xml"]):
                encoding = chardet.detect(body_bytes)["encoding"] or "utf-8"
                content = body_bytes.decode(encoding, errors="replace")

            if ctype:
                ctype = sanitize_content_type(ctype)

            if (
                not is_host_block_listed(host)
                and is_host_allow_listed(host)
                and not is_url_block_listed(rurl)
                and ctype
            ):
                found = False
                for regex, function in content_type_functions:
                    if regex.search(ctype):
                        found = True
                        try:
                            urlresult = await function({
                                'url': rurl,
                                'content': content,
                                'content_type': ctype,
                                'raw_content': body_bytes,
                                'parent_host': parent_host
                            })
                            page_data["crawledcontent"].update(urlresult)
                        except Exception as e:
                            print(f"Handler failed for {rurl}: {e}")
                if not found:
                    print(f"UNKNOWN type -{rurl}- -{ctype}-")

        except Exception as e:
            print(f"Error handling response: {e}")


    handler = lambda response: asyncio.create_task(handle_response(response))
    page.on("response", handler)

    try:
        await page.goto(url, wait_until="domcontentloaded")
    except Exception as e:
        print(f"Error while fetching {url}: {e}")
    finally:
        page.remove_listener("response", handler)
        await browser.close()
    if is_html_content(content_type):
        page_data["crawledcontent"].update({
            url: {
                "url": url,
                "content_type": content_type,
                "isopendir": isopendir,
                "opendir_pattern": pat,
                "visited": True,
                "words": words,
                "min_webcontent": min_webcontent,
                "raw_webcontent": raw_webcontent,
                "source": 'get_page_async',
                "parent_host": parent_host
            }
        })
    results["crawledcontent"].update(page_data["crawledcontent"])
    results["crawledlinks"].update(page_data["crawledlinks"])
    await browser.close()



async def get_page(url, playwright):
    db = DatabaseConnection()
    await get_page_async(url, playwright)
    presults=preprocess_crawler_data(results)
    pprint(presults)
    db.save_batch(presults)
    results.clear()


async def main():
    async with async_playwright() as playwright:
        url = INITIAL_URL
        await get_page(url, playwright)

if __name__ == "__main__":
    db = DatabaseConnection()
    urls_index, content_index = db_create_monthly_indexes(db)
    create_directories()
    asyncio.run(main())

