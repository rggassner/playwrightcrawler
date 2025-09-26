# Word extraction
EXTRACT_WORDS = True
WORDS_REMOVE_SPECIAL_CHARS = True
WORDS_TO_LOWER = True
WORDS_MIN_LEN = 3

# NonSafeForWork parameters
CATEGORIZE_NSFW = True
NSFW_MIN_PROBABILITY = .78
# Minimum number of pixels an image should have in order to be evaluated
MIN_NSFW_RES = 128 * 128
DOWNLOAD_NSFW = True
NSFW_FOLDER = 'images/nsfw'
DOWNLOAD_SFW = True
SFW_FOLDER = 'images/sfw'


DOWNLOAD_ALL_IMAGES = True
IMAGES_FOLDER = 'images'

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
# INITIAL_URL = 'https://crawler-test.com/'
INITIAL_URL = 'https://www.sito.org/'

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
