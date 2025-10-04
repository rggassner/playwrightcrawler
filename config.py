
# -------------------------------------------
# Elasticsearch configuration
# -------------------------------------------

# The hostname or IP address of the Elasticsearch server
ELASTICSEARCH_HOST = '192.168.15.71'

# The port Elasticsearch is listening on (typically 9200 for HTTP/HTTPS)
ELASTICSEARCH_PORT = 9200

# Username for basic authentication
ELASTICSEARCH_USER = 'elastic'

# Password for basic authentication
ELASTICSEARCH_PASSWORD = 'yourpassword'

# Optional path to a CA certificate file for verifying the server's TLS cert
# Set to None to skip custom CA verification (not recommended in production)
ELASTICSEARCH_CA_CERT_PATH = None

# Timeout in seconds for each request to Elasticsearch
# Useful when dealing with long-running queries or slow networks
ELASTICSEARCH_TIMEOUT = 300

# Whether to retry the request if it times out
# Helps improve resilience in the face of network hiccups or brief server issues
ELASTICSEARCH_RETRY = True

# Total number of retry attempts if a request fails or times out
# Applies when ELASTICSEARCH_RETRY is True
ELASTICSEARCH_RETRIES = 5

# Whether to enable HTTP compression for request/response bodies
# Can reduce bandwidth usage, but adds CPU cost — usually safe to enable
ELASTICSEARCH_HTTP_COMPRESS = False

# Whether to verify the server’s SSL certificate
# Should be True in production; set to False only in dev or when using self-signed certs
ELASTICSEARCH_VERIFY_CERTS = False

# Name of the index where crawled data will be stored
CONTENT_INDEX = 'crawler-content'

#Name of the index where links are going to be stored
LINKS_INDEX = 'crawler-links'


# --------------------
# --- What to keep ---
# --------------------

# Enable / Disable word extraction
EXTRACT_WORDS = True

# Remove special chars from words
WORDS_REMOVE_SPECIAL_CHARS = True

# Convert words to lowercase
WORDS_TO_LOWER = True

# Minimum word length
WORDS_MIN_LEN = 3

# Files won't be longer than MAX_FILENAME_LENGTH in disk. If it happens
# name will be trunkated, but original extensions are kept.
MAX_FILENAME_LENGTH = 255

# Categorize images as NSFW
CATEGORIZE_NSFW = False

# Minimum value to categorize as NSFW
NSFW_MIN_PROBABILITY = .78

# Minimum number of pixels an image should have in order to be evaluated
MIN_NSFW_RES = 128 * 128

DOWNLOAD_NSFW = False
NSFW_FOLDER = 'images/nsfw'

DOWNLOAD_SFW = False
SFW_FOLDER = 'images/sfw'

DOWNLOAD_ALL_IMAGES = False
IMAGES_FOLDER = 'images'

DOWNLOAD_FONTS = False
FONTS_FOLDER = 'fonts'

DOWNLOAD_VIDEOS = False
VIDEOS_FOLDER = 'videos'

DOWNLOAD_MIDIS = True
MIDIS_FOLDER = 'midis'

DOWNLOAD_AUDIOS = False
AUDIOS_FOLDER = 'audios'

DOWNLOAD_PDFS = False
PDFS_FOLDER = 'pdfs'

DOWNLOAD_DOCS = False
DOCS_FOLDER = 'docs'

DOWNLOAD_DATABASES = False
DATABASES_FOLDER = 'databases'

DOWNLOAD_TORRENTS = False
TORRENTS_FOLDER = 'torrents'

DOWNLOAD_COMPRESSEDS = False
COMPRESSEDS_FOLDER = 'compresseds'

#When decomposing directories and host name domains, how many levels should be stored
MAX_DIR_LEVELS = 7
MAX_HOST_LEVELS = 7


# WORDS_MAX_LEN * WORDS_MAX_WORDS should be under 1 million
# for a default elastic search env
WORDS_MAX_LEN = 40
WORDS_MAX_WORDS = 24000

# If we should or not save full html to the database
EXTRACT_RAW_WEBCONTENT = False
# If we should or not save rendered text page to the database
EXTRACT_MIN_WEBCONTENT = True

# Should be under 1 million for a default elastic search env
MAX_WEBCONTENT_SIZE = 900000

# Used only in the first run
INITIAL_URL = 'https://crawler-test.com/'

# This will include all directories from tree
# might sound aggressive for some websites
HUNT_OPEN_DIRECTORIES = True

# Folder where you place url files to be crawled
INPUT_FOLDER = 'input_url_files' 
# Url from file batch size
MAX_URLS_FROM_FILE = 100

ITERATIONS = 10000
RANDOM_SITES_QUEUE = 10000

# Weighs used to pick methods
METHOD_WEIGHTS = {
    "fewest_urls":  1,
    "oldest":       1,
    "host_prefix":  10,
    "random":       10
}

# -----------------------------
# --- Playwright parameters ---
# -----------------------------

# Page timeout in ms
PAGE_TIMEOUT_MS = 60000

#How many scrolls try before giving up to reach end of page
SCROLL_ATTEMPTS = 5

#Delay between scrolls
SCROLL_DELAY = 1.0


# --------------------
# --- Fast crawler ---
# --------------------

# Delay between fast buckets. Used to decrease the elastic search access.
FAST_DELAY = 2

# When working with only one worker and if you want to avoid WAFs
FAST_RANDOM_MIN_WAIT = 0
FAST_RANDOM_MAX_WAIT = 0

MAX_FAST_WORKERS = 16

USE_OCTET_STREAM = True

STRICT_EXTENSION_QUERY = True

# --------------------------
# --- Blocks and removes ---
# --------------------------

# This option only makes sense to be activated when you have an external
# script packing data to database, since all crawler data is already
# filtered while urls are entering.
REMOVE_INVALID_URLS = True
    
# If urls that are blocked based on host should be removed from the database.
REMOVE_BLOCKED_HOSTS = True
    
# If urls that are blocked based on path should be deleted from the database.
REMOVE_BLOCKED_URLS = True

# Do not crawl these domains.
HOST_REGEX_BLOCK_LIST = [
    r'(^|\.)gstatic\.com$',
]

# Only crawl domains that match this regex
HOST_REGEX_ALLOW_LIST = [r'.*']

# Do not crawl urls that match any of these regexes
URL_REGEX_BLOCK_LIST = [
    '/noticias/modules/noticias/modules/',
    '/images/images/images/images/',
    '/plugins/owlcarousel/plugins/',
]


