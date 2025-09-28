
# Word extraction
EXTRACT_WORDS = True
WORDS_REMOVE_SPECIAL_CHARS = True
WORDS_TO_LOWER = True
WORDS_MIN_LEN = 3

# Files won't be longer than MAX_FILENAME_LENGTH in disk. If it happens
# name will be trunkated, but original extensions are kept.
MAX_FILENAME_LENGTH = 255

# NonSafeForWork parameters
CATEGORIZE_NSFW = False
NSFW_MIN_PROBABILITY = .78
# Minimum number of pixels an image should have in order to be evaluated
MIN_NSFW_RES = 128 * 128
DOWNLOAD_NSFW = False
NSFW_FOLDER = 'images/nsfw'
DOWNLOAD_SFW = False
SFW_FOLDER = 'images/sfw'
DOWNLOAD_ALL_IMAGES = True
IMAGES_FOLDER = 'images'

DOWNLOAD_FONTS = True
FONTS_FOLDER = 'fonts'

DOWNLOAD_VIDEOS = True
VIDEOS_FOLDER = 'videos'

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

# Do not crawl these domains.
HOST_REGEX_BLOCK_LIST = [
    r'localhost:4443$',
    r'(^|\.)spotify.com$',
    r'(^|\.)google$',
]


# Only crawl domains that match this regex
HOST_REGEX_ALLOW_LIST = [r'.*']

# Do not crawl urls that match any of these regexes
URL_REGEX_BLOCK_LIST = [
    '/noticias/modules/noticias/modules/',
    '/images/images/images/images/',
    '/plugins/owlcarousel/plugins/',
]

# -------------------------------------------
# Elasticsearch connection configuration
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


# In order to avoid multiple workers on the same url
ELASTICSEARCH_RANDOM_BUCKETS = 20

# Name of the indexes where data will be stored
CONTENT_INDEX = 'crawler-content'
LINKS_INDEX = 'crawler-links'

# Page timeout in ms
PAGE_TIMEOUT_MS = 60000

#How many scrolls try before giving up to reach end of page
SCROLL_ATTEMPTS = 5

#Delay between scrolls
SCROLL_DELAY = 1.0


URLS_MAPPING = {
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "visited": {"type": "boolean"},
            "content_type": {"type": "keyword"},
            "source": {"type": "keyword"},
            "host": {"type": "keyword"},
            "parent_host": {"type": "keyword"},
            "created_at": {"type": "date"},
            "updated_at": {"type": "date"},
            "emails": {"type": "keyword"},
            "random_bucket": {"type": "integer"},
        }
    }
}

CONTENT_MAPPING = {
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "host": {"type": "keyword"},
            "content_type": {"type": "keyword"},
            "words": {"type": "keyword"},
            "raw_webcontent": {"type": "text"},
            "min_webcontent": {"type": "text"},
            "created_at": {"type": "date"},
            "updated_at": {"type": "date"}
        }
    }
}



