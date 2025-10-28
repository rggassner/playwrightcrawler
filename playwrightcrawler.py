#!venv/bin/python3
import sys
import os
import re
import asyncio
import argparse
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urljoin, urlsplit, unquote, urlparse, parse_qs, urlunsplit
from pathlib import PurePosixPath
from collections import Counter
import random
import hashlib
import warnings
import fcntl

import httpx
import urllib3
import chardet
import numpy as np
import psutil
from fake_useragent import UserAgent
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError
from PIL import Image, UnidentifiedImageError
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from playwright.async_api import async_playwright, Error as PlaywrightError
import absl.logging
from urllib3.exceptions import InsecureRequestWarning

from config import *



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



content_type_octetstream = [
        r"^text/octet$",
        r"^octet/stream$",
        r"^application/stream$",
        r"^binary/octet-stream$",
        r"^application/download$",
        r"^application/octetstream$",
        r"^application/octet-stream$",
        r"^application/x-octet-stream$",
        r"^x-application/octet-stream$",
        r"^application/force-download$",
        r"^application/octet-stream,text/html$",
        r"^application/octet-stream,atext/plain$",
        r"^application/octet-streamCharset=UTF-8$",
        r"^application/octet-stream,text/plain$",
        r"^application/vnd\.google\.octet-stream-compressible$",
    ]

content_type_html_regex = [
        r"^text/html$",
        r"^application/html$",
        r"^application/x-php$",
        r"^text/html,text/html",
        r"^text/htmltext/html$",
        r"^text/fragment\+html$",
        r"^text/html, charset=.*",
        r"^text/x-html-fragment$",
        r"^application/xhtml\+xml$",
        r"^text/html,charset=UTF-8$",
        r"^text/html,charset=iso-8859-1$",
        r"^text/vnd\.reddit\.partial\+html$",
        r"^text/htmltext/html;charset=utf-8$",
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

#for ct in `wget -O - https://www.iana.org/assignments/media-types/image.csv |\
#grep -vP "Template" |cut -d, -f 2`; do echo "        r\"^$ct\$\","; done | sort |\
#uniq |awk '{ print length, $0 }' | sort -n | awk '{ $1=""; sub(/^ /, ""); print "        "$1}'
content_type_image_regex = [
        r"^jpg$",
        r"^png$",
        r"^jpeg$",
        r"^webp$",
        r"^image$",
        r"^PNG32$",
        r"^.jpeg$",
        r"^webpx$",
        r"^image/$",
        r"^img/png$",
        r"^JPG_MIME$",
        r"^image/\*$",
        r"^img/jpeg$",
        r"^image/any$",
        r"^image/bmp$",
        r"^video/png$",
        r"^image/cgm$",
        r"^image/dpx$",
        r"^image/emf$",
        r"^image/gif$",
        r"^image/ico$",
        r"^image/ief$",
        r"^image/j2c$",
        r"^image/jls$",
        r"^image/jp2$",
        r"^image/jpg$",
        r"^image/jph$",
        r"^image/jpm$",
        r"^image/jpx$",
        r"^image/jxl$",
        r"^image/jxr$",
        r"^image/jxs$",
        r"^image/ktx$",
        r"^image/pbf$",
        r"^image/png$",
        r"^image/svg$",
        r"^image/t38$",
        r"^image/wmf$",
        r"^image/jpeg$",
        r"^image/jpqg$",
        r"^image/jphc$",
        r"^image/jxrA$",
        r"^image/aces$",
        r"^image/apng$",
        r"^image/avci$",
        r"^image/avcs$",
        r"^image/avif$",
        r"^iamge/avif$",
        r"^image/fits$",
        r"^image/heic$",
        r"^image/heif$",
        r"^image/hsj2$",
        r"^image/jaii$",
        r"^image/jais$",
        r"^image/jpeg$",
        r"^image/jxrS$",
        r"^image/jxsc$",
        r"^image/jxsi$",
        r"^image/jxss$",
        r"^image/ktx2$",
        r"^image/tiff$",
        r"^image/webp$",
        r"^(null)/ico$",
        r"^image/xicon$",
        r"^image/g3fax$",
        r"^image/hej2k$",
        r"^image/pjpeg$",
        r"^image/x-emf$",
        r"^image/x-eps$",
        r"^image/x-ico$",
        r"^image/x-png$",
        r"^image/x-wmf$",
        r"^image/dicomp$",
        r"^image/naplps$",
        r"^image/x-icon$",
        r"^\(null\)/ico$",
        r"^image/example$",
        r"^image/\{png\}$",
        r"^image/prs.pti$",
        r"^image/svg+xml$",
        r"^image/tiff-fx$",
        r"^image/vnd.dwg$",
        r"^image/vnd.dxf$",
        r"^image/vnd.fpx$",
        r"^image/vnd.fst$",
        r"^image/vnd.mix$",
        r"^image/vnd.svf$",
        r"^data:image/png$",
        r"^image/prs.btif$",
        r"^image/svg\+xml$",
        r"^image/vnd.clip$",
        r"^image/vnd.djvu$",
        r"^image/vnd\.dwg$",
        r"^image/vnd.xiff$",
        r"^image/x-ms-bmp$",
        r"^application/jpg$",
        r"^image/dicom-rle$",
        r"^image/vnd\.djvu$",
        r"^image/x-xbitmap$",
        r"^image/pwg-raster$",
        r"^image/vnd.ms-modi$",
        r"^image/vnd.net-fpx$",
        r"^image/vnd.pco.b16$",
        r"^image/x-coreldraw$",
        r"^image/x-photoshop$",
        r"^image/extendedwebp$",
        r"^image/vnd.cns.inf2$",
        r"^image/vnd.radiance$",
        r"^image/vnd.wap.wbmp$",
        r"^image/x-cmu-raster$",
        r"^image/x-win-bitmap$",
        r"^image/heic-sequence$",
        r"^image/heif-sequence$",
        r"^image/png,image/jpeg$",
        r"^image/vnd.sealed.png$",
        r"^image/vnd\.wap\.wbmp$",
        r"^image/vnd.zbrush.pcx$",
        r"^image/vnd.tencent.tap$",
        r"^text/plain,image/avif$",
        r"^image/vnd.dece.graphic$",
        r"^image/vnd.dvb.subtitle$",
        r"^image/vnd.fastbidsheet$",
        r"^image/vnd.mozilla.apng$",
        r"^image/x\.fb\.keyframes$",
        r"^image/vnd.microsoft.icon$",
        r"^image/vnd.adobe.photoshop$",
        r"^image/vnd.blockfact.facti$",
        r"^image/vnd\.microsoft\.icon$",
        r"^image/vnd\.adobe\.photoshop$",
        r"^image/vnd.globalgraphics.pgb$",
        r"^binary/octet-stream,image/webp$",
        r"^image/vnd.fujixerox.edmics-mmr$",
        r"^image/vnd.fujixerox.edmics-rlc$",
        r"^image/vnd.valve.source.texture$",
        r"^image/vnd.airzip.accelerator.azv$",
        r"^image/vnd.sealedmedia.softseal.gif$",
        r"^image/vnd.sealedmedia.softseal.jpg$",
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
        r"^application/ogg$",
        r"^audio/x-mpegurl$",
        r"^application/mp4$",
        r"^audio/x-oggvorbis$",
        r"^audio/x-pn-realaudio$",
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
        r"^application/vnd\.ms-asf$",
        r"^video/vnd\.dlna\.mpeg-tts$",
        r"^application/x-shockwave-flash$",
        r"^application/vnd\.apple\.mpegurl$",
        r"^application/vnd\.adobe\.flash\.movie$",
        r"^application/mp4,audio/mp4,video/mp4,video/vnd\.objectvideo$",
        ]

content_type_pdf_regex = [
        r"^adobe/pdf$",
        r"^image/pdf$",
        r"^application/pdf$",
        r"^,application/pdf$",
        r"^application/\.pdf$",
        r"^application/x-pdf$",
        r"^application/pdfcontent-length:",
        r"^application/x-www-form-urlencoded,",
        r"^application/pdf,application/pdf$",
        r"^binary/octet-stream,application/pdf$",
    ]


content_type_comic_regex = [
        r"^application/x-cbr$",
        r"^application/x-cbz$",
        r"^application/vnd\.comicbook\+zip$",
        r"^application/vnd\.comicbook-rar$",
        ]


content_type_doc_regex = [
        r"^application/doc$",
        r"^application/xls$",
        r"^application/xlsx$",
        r"^application/docx$",
        r"^application/x-cbr$",
        r"^application/x-cbz$",
        r"^application/msword$",
        r"^application/msexcel$",
        r"^application/ms-excel$",
        r"^application/x-msword$",
        r"^application/x-msexcel$",
        r"^application/vnd\.visio$",
        r"^application/vnd\.ms-word$",
        r"^application/vnd\.ms-excel$",
        r"^application/vnd\.freelog\.comic$",
        r"^application/vnd\.ms-officetheme$",
        r"^application/vnd\.ms-visio\.drawing$",
        r"^application/vnd\.ms-word\.document\.12$",
        r"^application/vnd\.ms-excel\.openxmlformat$",
        r"^application/vnd\.oasis\.opendocument\.text$",
        r"^application/vnd\.oasis\.opendocument\.spreadsheet$",
        r"^application/vnd\.oasis\.opendocument\.presentation$",
        r"^application/vnd\.ms-excel\.sheet\.macroenabled\.12$",
        r"^application/vnd\.openxmlformats-officedocument\.spre$",
        r"^application/vnd\.oasis\.opendocument\.formula-template$",
        r"^application/vnd\.ms-powerpoint\.slideshow\.macroEnabled\.12$",
        r"^application/vnd\.openxmlformats-officedocument\.spreadsheetml$",
        r"^application/vnd\.openxmlformats-officedocument\.wordprocessingml$",
        r"^application/vnd\.openxmlformats-officedocument\.spreadsheetml\.sheet$",
        r"^application/vnd\.openxmlformats-officedocument\.presentationml\.slideshow",
        r"^application/vnd\.openxmlformats-officedocument\.wordprocessingml\.document$",
        r"^application/vnd\.openxmlformats-officedocument\.wordprocessingml\.template$",
        r"^application/vnd\.openxmlformats-officedocument\.presentationml\.presentation$",
        ]

content_type_database_regex = [
        r"^application/sql$",
        r"^application/x-sql$",
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
        r"^text/woff$",
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
        r"^font/collection$",
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
        ]

content_type_torrent_regex = [
        r"^application/x-bittorrent$",
        ]

content_type_compressed_regex = [
        r"^zip$",
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
        r"^application/gzipped-tar$",
        r"^application/zip-compressed$",
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
        r"^javascript$",
        r"^text/x-scss$",
        r"^application$",
        r"^Content-Type$",
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
        r"^application/exe$",
        r"^application/xml$",
        r"^application/aux$",
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
        r"^text/x-javascript$",
        r"^application/empty$",
        r"^text/plaincharset:",
        r"^chemical/x-cerius$",
        r"^text/css,text/css$",
        r"^application/x-rpm$",
        r"^application/x-twb$",
        r"^application/x-xcf$",
        r"^application/x-msi$",
        r"^application/plain$",
        r"^application/x-xar$",
        r"^application/proto$",
        r"^model/gltf-binary$",
        r"^text/htmltext/css$",
        r"^application/x-plt$",
        r"^text/html,text/css$",
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
        r"^application/express$",
        r"^multipart/form-data$",
        r"^application/x-trash$",
        r"^application/unknown$",
        r"^application/xml-dtd$",
        r"^application/x-empty$",
        r"^application/x-blorb$",
        r"^application/java-vm$",
        r"^application/msgpack$",
        r"^application/rsd\+xml$",
        r"^application/rfc\+xml$",
        r"^application/x-netcdf$",
        r"^application/gml\+xml$",
        r"^chemical/x-molconn-Z$",
        r"^application/x-nozomi$",
        r"^application/x-adrift$",
        r"^application/x-binary$",
        r"^application/rdf\+xml$",
        r"^application/rss\+xml$",
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
        r"^application/csp-report$",
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
        r"^application/x-xpinstall$",
        r"^application/x-httpd-php$",
        r"^application/link-format$",
        r"^application/x-directory$",
        r"^application/x-troff-man$",
        r"^application/mac-binhex40$",
        r"^application/encrypted-v2$",
        r"^application/java-archive$",
        r"^application/x-javascript$",
        r"^application/x-msdownload$",
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
        r"^application/vnd\.visionary$",
        r"^application/x-java-archive$",
        r"^application/x-x509-ca-cert$",
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
        r"^application/vnd\.sas\.api\+json$",
        r"^application/vnd\.openxmlformats$",
        r"^application/vnd\.apple\.keynote$",
        r"^application/apple\.vnd\.mpegurl$",
        r"^application/vnd\.wv\.csp\+wbxml$",
        r"^application/x-ms-dos-executable$",
        r"^application/vnd\.geogebra\.file$",
        r"^text/html,application/javascript$",
        r"^application/grpc-web-text\+proto$",
        r"^application/vnd\.lotus-screencam$",
        r"^application/x-pkcs7-certificates$",
        r"^application/x-www-form-urlencoded$",
        r"^application/vnd\.google-earth\.kmz$",
        r"^application/x-typekit-augmentation$",
        r"^application/x-unknown-content-type$",
        r"^application/x-research-info-systems$",
        r"^application/vnd\.mapbox-vector-tile$",
        r"^application/vnd\.cas\.services\+yaml$",
        r"^application/x-redhat-package-manager$",
        r"^application/vnd\.adobe\.skybox\+json$",
        r"^application/vnd\.groove-tool-template$",
        r"^application/vnd\.apple\.installer\+xml$",
        r"^application/opensearchdescription\+xml$",
        r"^application/vnd\.google-earth\.kml\+xml$",
        r"^text/javascript/application/x-javascript$",
        r"^application/vnd\.android\.package-archive$",
        r"^application/javascript,application/javascript$",
        r"^application/javascriptapplication/x-javascript$",
        r"^application/javascript,application/x-javascript$",
        r"^application/vnd\.oracle\.adf\.resourceitem\+json$",
    ]

"""Extend regex lists with octet-stream types if enabled."""
if USE_OCTET_STREAM:
    categories = [
        content_type_database_regex,
        content_type_image_regex,
        content_type_midi_regex,
        content_type_audio_regex,
        content_type_video_regex,
        content_type_pdf_regex,
        content_type_doc_regex,
        content_type_font_regex,
        content_type_torrent_regex,
        content_type_comic_regex,
        content_type_compressed_regex,
    ]
    for category in categories:
        for pattern in content_type_octetstream:
            if pattern not in category:
                category.append(pattern)



# IMPORTANT: When adding a new extension and its corresponding content-type group here,
# make sure to also update the `needs_download` logic in the `fast_extension_crawler` function
# so the new type is either downloaded or skipped as expected.
#
# Additionally, ensure a corresponding handler function is decorated with
# @function_for_content_type(<your_new_regex>) and implemented properly
# to process and optionally store the file.
EXTENSION_MAP = {
        ".aac": content_type_audio_regex,
        ".aif": content_type_audio_regex,
        ".flac": content_type_audio_regex,
        ".m4a": content_type_audio_regex,
        ".mp3": content_type_audio_regex,
        ".ogg": content_type_audio_regex,
        ".rm": content_type_audio_regex,
        ".s3m": content_type_audio_regex,
        ".wav": content_type_audio_regex,
        ".xm": content_type_audio_regex,
        ".webm": content_type_audio_regex,
        ".Z": content_type_compressed_regex,
        ".lz": content_type_compressed_regex,
        ".7z": content_type_compressed_regex,
        ".gz": content_type_compressed_regex,
        ".zip": content_type_compressed_regex,
        ".bz2": content_type_compressed_regex,
        ".lzma": content_type_compressed_regex,
        ".cab": content_type_compressed_regex,
        ".rar": content_type_compressed_regex,
        ".sql": content_type_database_regex,
        ".mdb": content_type_database_regex,
        ".cbr": content_type_comic_regex,
        ".cbz": content_type_comic_regex,
        ".doc": content_type_doc_regex,
        ".docx": content_type_doc_regex,
        ".vsd": content_type_doc_regex,
        ".xls": content_type_doc_regex,
        ".xlsx": content_type_doc_regex,
        ".otf": content_type_doc_regex,
        ".ttf": content_type_font_regex,
        ".otf": content_type_font_regex,
        ".pfb": content_type_font_regex,
        ".eot": content_type_font_regex,
        ".ttc": content_type_font_regex,
        ".TTF": content_type_font_regex,
        ".woff": content_type_font_regex,
        ".woff2": content_type_font_regex,
        ".aces": content_type_image_regex,
        ".apng": content_type_image_regex,
        ".avci": content_type_image_regex,
        ".avcs": content_type_image_regex,
        ".avif": content_type_image_regex,
        ".bmp": content_type_image_regex,
        ".cgm": content_type_image_regex,
        ".cur": content_type_image_regex,
        ".dpx": content_type_image_regex,
        ".emf": content_type_image_regex,
        ".example": content_type_image_regex,
        ".fits": content_type_image_regex,
        ".g3fax": content_type_image_regex,
        ".gif": content_type_image_regex,
        ".heic": content_type_image_regex,
        ".HEIC": content_type_image_regex,
        ".heif": content_type_image_regex,
        ".hej2k": content_type_image_regex,
        ".ico": content_type_image_regex,
        ".ief": content_type_image_regex,
        ".j2c": content_type_image_regex,
        ".jaii": content_type_image_regex,
        ".jais": content_type_image_regex,
        ".jls": content_type_image_regex,
        ".jp2": content_type_image_regex,
        ".jpeg": content_type_image_regex,
        ".jpg": content_type_image_regex,
        ".JPG": content_type_image_regex,
        ".jphc": content_type_image_regex,
        ".jph": content_type_image_regex,
        ".jpm": content_type_image_regex,
        ".jpx": content_type_image_regex,
        ".jxl": content_type_image_regex,
        ".jxrA": content_type_image_regex,
        ".jxr": content_type_image_regex,
        ".jxrS": content_type_image_regex,
        ".jxsc": content_type_image_regex,
        ".jxs": content_type_image_regex,
        ".jxsi": content_type_image_regex,
        ".jxss": content_type_image_regex,
        ".ktx2": content_type_image_regex,
        ".ktx": content_type_image_regex,
        ".naplps": content_type_image_regex,
        ".pbf": content_type_image_regex,
        ".png": content_type_image_regex,
        ".PNG": content_type_image_regex,
        ".pnj": content_type_image_regex,
        ".psd": content_type_image_regex,
        ".svg": content_type_image_regex,
        ".t38": content_type_image_regex,
        ".tiff": content_type_image_regex,
        ".webp": content_type_image_regex,
        ".wmf": content_type_image_regex,
        ".mid": content_type_midi_regex,
        ".Mid": content_type_midi_regex,
        ".midi": content_type_midi_regex,
        ".pdf": content_type_pdf_regex,
        ".torrent": content_type_torrent_regex,
        ".3gp": content_type_video_regex,
        ".asf": content_type_video_regex,
        ".flv": content_type_video_regex,
        ".m3u8": content_type_video_regex,
        ".m4s": content_type_video_regex,
        ".mkv": content_type_video_regex,
        ".mov": content_type_video_regex,
        ".MOV": content_type_video_regex,
        ".mp4": content_type_video_regex,
        ".mpg": content_type_video_regex,
        ".mpeg": content_type_video_regex,
        ".ogv": content_type_video_regex,
        ".swf": content_type_video_regex,
        ".webm": content_type_video_regex,
        ".wm": content_type_video_regex,
        ".wmv": content_type_video_regex,
    }


def db_create_monthly_indexes(db=None):
    if db is None or db.es is None:  # <- changed from db.con
        raise ValueError("db connection is required")
    urls_index = get_index_name(LINKS_INDEX)
    content_index = get_index_name(CONTENT_INDEX)
    return True
    #return urls_index, content_index

def get_urls_by_random_host_prefix(db, size=RANDOM_SITES_QUEUE):
    """
    Pick a random character, collect one random URL per host
    whose hostname starts with that character.
    """
    urls_index = f"{LINKS_INDEX}-*"
    host_to_url = {}

    # Step 1: pick a random starting char (a-z, 0-9)
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    chosen_char = random.choice(chars)
    print(f"[CHAR PICK] Filtering hosts starting with '{chosen_char}'")

    # Step 2: scan ES
    query = {"query": {"match_all": {}}}
    for doc in helpers.scan(db.es, index=urls_index, query=query):
        url = doc["_source"].get("url")
        if not url:
            continue
        host = urlsplit(url).hostname
        if not host:
            continue

        # Filter hosts by chosen char
        if not host.lower().startswith(chosen_char):
            continue

        # Reservoir sampling to keep one URL per host
        if host not in host_to_url:
            host_to_url[host] = url
        else:
            if random.random() < 0.5:
                host_to_url[host] = url

    # Step 3: shuffle and cut
    all_urls = list(host_to_url.values())
    random.shuffle(all_urls)

    if len(all_urls) > size:
        all_urls = all_urls[:size]

    return all_urls


def has_repeated_segments(url: str, max_pattern: int = 5, min_repeats: int = 3) -> bool:
    """
    Detects cyclic or recursive directory structures in a URL path
    by finding repeated patterns of up to `max_pattern` segments.

    Example detections:
        /fonts/fonts/fonts/
        /assets/video/assets/video/assets/video/
        /a/b/c/a/b/c/a/b/c/
        /assets/video/assets/video/  (only 2 repeats, below threshold)

    Args:
        url (str): Full URL to analyze.
        max_pattern (int): Maximum length of the repeating directory pattern (default 5).
        min_repeats (int): Minimum consecutive repetitions required to trigger detection (default 3).

    Returns:
        bool: True if a repeating path pattern occurs `min_repeats` or more times.
    """
    path = urlparse(url).path.strip('/')
    if not path:
        return False

    segments = path.split('/')

    for pattern_len in range(1, min(max_pattern, len(segments) // min_repeats) + 1):
        # Convert to tuple for hashable pattern comparison
        for i in range(len(segments) - pattern_len * min_repeats + 1):
            pattern = segments[i:i + pattern_len]

            # Count how many times it repeats consecutively
            repeat_count = 1
            j = i + pattern_len
            while j + pattern_len <= len(segments) and segments[j:j + pattern_len] == pattern:
                repeat_count += 1
                j += pattern_len

            if repeat_count >= min_repeats:
                return True
    return False


def get_random_host_domains(db, size=RANDOM_SITES_QUEUE):
    urls_index = f"{LINKS_INDEX}-*"
    host_to_url = {}

    # Scan the entire index, but keep only one URL per host
    query = {"query": {"match_all": {}}}
    for doc in helpers.scan(db.es, index=urls_index, query=query):
        url = doc['_source'].get('url')
        if not url:
            continue
        host = urlsplit(url).hostname
        if not host:
            continue

        # Reservoir sampling: randomly keep one URL per host
        if host not in host_to_url:
            host_to_url[host] = url
        else:
            if random.random() < 0.5:
                host_to_url[host] = url

    all_urls = list(host_to_url.values())
    random.shuffle(all_urls)  # <--- ensure random order every time

    if len(all_urls) > size:
        all_urls = all_urls[:size]  # already shuffled, so this is a random cut

    return all_urls

def get_urls_from_least_populated_hosts(db, size=RANDOM_SITES_QUEUE):
    es = db.es
    urls_index = f"{LINKS_INDEX}-*"

    # Step 1: Aggregate by host with min_doc_count = 1
    aggs_query = {
        "size": 0,
        "aggs": {
            "by_host": {
                "terms": {
                    "field": "host.keyword",
                    "size": size * 5,  # fetch more buckets to sample from
                    "order": {"_count": "asc"}  # fewest URLs first
                }
            }
        }
    }

    agg_result = es.search(index=urls_index, body=aggs_query)
    buckets = agg_result["aggregations"]["by_host"]["buckets"]

    host_to_url = {}

    # Step 2: For each low-populated host, get one URL
    for bucket in buckets:
        host = bucket["key"]

        query = {
            "size": 1,
            "query": {"term": {"host.keyword": host}},
            "_source": ["url"]
        }
        hit = es.search(index=urls_index, body=query)["hits"]["hits"]
        if hit:
            url = hit[0]["_source"]["url"]
            host_to_url[host] = url

        if len(host_to_url) >= size:
            break

    # Step 3: Shuffle result to avoid bias
    all_urls = list(host_to_url.values())
    random.shuffle(all_urls)

    return all_urls

def get_oldest_host_domains(db, size=RANDOM_SITES_QUEUE):
    urls_index = f"{LINKS_INDEX}-*"
    host_to_url = {}

    # Sort ascending by _id (or replace with "@timestamp" if you have it)
    query = {
        "size": 10000,  # batch size
        "query": {"match_all": {}},
        "sort": [
          {"created_at": {"order": "asc"}}
        ],
        "_source": ["url"]
    }

    es = db.es
    response = es.search(index=urls_index, body=query)

    for hit in response["hits"]["hits"]:
        url = hit["_source"].get("url")
        if not url:
            continue
        host = urlsplit(url).hostname
        if not host:
            continue

        # Keep the *first* URL we encounter per host (since sorted oldest first)
        if host not in host_to_url:
            host_to_url[host] = url

        # Break early if enough collected
        if len(host_to_url) >= size:
            break

    all_urls = list(host_to_url.values())
    return all_urls


def url_to_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def get_index_name(base: str) -> str:
    suffix = datetime.now(timezone.utc).strftime("%Y-%m")
    return f"{base}-{suffix}"


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
        Skips links without host.
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
                "_id": url_to_id(url),
                "_source": doc
            })

        # --- crawledlinks ---
        for url, host in data.get("crawledlinks", {}).items():
            if not host:
                continue  # skip links with no host
            doc = {
                "url": url,
                "host": host,
                "visited": False,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            actions.append({
                "_op_type": "index",
                "_index": urls_index,
                "_id": url_to_id(url),
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

        #print(f"[BULK] Inserted {len(actions) - failed_count} docs successfully, {failed_count} failed")




def create_directories():
    dirs = [IMAGES_FOLDER, NSFW_FOLDER, SFW_FOLDER , FONTS_FOLDER, VIDEOS_FOLDER,  MIDIS_FOLDER , AUDIOS_FOLDER, PDFS_FOLDER ,DOCS_FOLDER , DATABASES_FOLDER, TORRENTS_FOLDER,COMPRESSEDS_FOLDER , COMICS_FOLDER, INPUT_FOLDER]
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

def is_embedded_url(url: str) -> bool:
    return url.startswith(("data:", "blob:", "about:", "javascript:"))

def preprocess_crawler_data(data: dict) -> dict:

    crawledcontent = data.get("crawledcontent", {})
    crawledlinks = data.get("crawledlinks", set())

    filtered_links = {}
    new_crawledcontent = {}

    if HUNT_OPEN_DIRECTORIES:
        for url, doc in crawledcontent.items():
            if is_embedded_url(url) or len(url) > MAX_URL_LENGTH:
                continue  # skip embedded base64 images        
            crawledlinks.update(set(get_directory_tree(url)))
        for url in crawledlinks.copy():
            if is_embedded_url(url) or len(url) > MAX_URL_LENGTH:
                continue  # skip embedded base64 images        
            crawledlinks.update(set(get_directory_tree(url)))

    for url in crawledlinks:
        if is_embedded_url(url) or len(url) > MAX_URL_LENGTH:
            continue  # skip embedded base64 images        
        try:
            url = sanitize_url(url)
            parts = urlsplit(url)
            host = parts.hostname
            if not host:
                continue  
            
            # Strip fragment (#whatever)
            normalized_url = urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
            if(
                    not is_host_block_listed(host) 
                    and is_host_allow_listed(host) 
                    and not is_url_block_listed(normalized_url)
                    and not has_repeated_segments(normalized_url)
            ):
                if normalized_url not in crawledcontent:
                    filtered_links[normalized_url] = host 

        except Exception as e:
            print(f"[PREPROCESS_CRAWLER_DATA NORMALIZATION] Failed to normalize {url}: {e}")
            continue    

    for url, doc in crawledcontent.items():
        if is_embedded_url(url) or len(url) > MAX_URL_LENGTH:
            continue  # skip embedded base64 images        
        host = urlsplit(url).hostname
        if not host:
            continue  # skip if content URL has no host
        if(
                not is_host_block_listed(host) 
                and is_host_allow_listed(host) 
                and not is_url_block_listed(url)
                and not has_repeated_segments(url)
        ):
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

            # --- Host name ---

            insert_only_fields["host"] = host

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
        "crawledlinks": filtered_links  # now a dict url -> host
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

    #def log_change(reason, before, after):
    #    if before != after and reason not in skip_log_tags and debug:
    #        print(f"\033[91m[{reason}] URL sanitized \
    #              from -{before}- to -{after}-\033[00m")

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
    #log_change("STRIP_WHITESPACE", pre_sanitize, url)
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
        #log_change("SPECIAL_QUOTE_CLEAN", url, cleaned)
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
        #log_change("FIX_SCHEME", url, fixed)
        url = fixed

    cleaned = re.sub(r'^[a-zA-Z."(´]https://', 'https://', url)
    #log_change("PREFIX_CLEAN_HTTPS", url, cleaned)
    url = cleaned
    cleaned = re.sub(r'^[a-zA-Z."(´]http://', 'http://', url)
    #log_change("PREFIX_CLEAN_HTTP", url, cleaned)
    url = cleaned

    url = re.sub(r'^(https?:)/+', r'\1//', url)
    #log_change("FIX_SCHEME_SLASHES", pre_sanitize, url)
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
                #log_change("FIX_NETLOC_IN_PATH", url, rebuilt)
                url = rebuilt
        else:
            path = re.sub(r'/{2,}', '/', parsed.path)
            rebuilt = urlunsplit(
                    (scheme,
                     netloc,
                     path,
                     parsed.query,
                     parsed.fragment))
            #log_change("NORMALIZE_PATH_SLASHES", url, rebuilt)
            url = rebuilt
    except Exception:
        fallback = re.sub(r'(https?://[^/]+)/{2,}', r'\1/', url)
        #log_change("FALLBACK_SLASH_FIX", url, fallback)
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
        #log_change("FINAL_NORMALIZE", url, normalized)
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

    async def safe_extract(selector: str, attr: str, tag_name: str):
        try:
            values = await page.evaluate(f"""
                () => Array.from(document.querySelectorAll('{selector}'))
                          .map(e => e['{attr}'])
            """)
            # Filter only strings
            return [v for v in values if isinstance(v, str)]
        except Exception as e:
            #print(f"[WARN] Could not extract <{tag_name}> from {base_url}: {e}")
            return []

    # Extract all sources safely
    links.update(await safe_extract("a[href]", "href", "a"))
    links.update(await safe_extract("link[href]", "href", "link"))
    links.update(await safe_extract("script[src]", "src", "script"))
    links.update(await safe_extract("img[src]", "src", "img"))

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
    # JS snippet that safely walks the DOM and collects text nodes
    js = """
    () => { 
        try {
            const body = document.body;
            if (!body) return [];
            const blocklist = new Set(["script", "style", "noscript", "iframe"]);
            
            function getTextNodes(node) {
                let texts = [];
                if (node.nodeType === Node.TEXT_NODE) {
                    const text = node.textContent.trim();
                    if (text.length > 0) texts.push(text);
                } else if (node.nodeType === Node.ELEMENT_NODE && !blocklist.has(node.tagName.toLowerCase())) {
                    for (const child of node.childNodes) {
                        texts = texts.concat(getTextNodes(child));
                    }
                }
                return texts;
            }
            return getTextNodes(body);
        } catch (err) {
            return [];
        }
    }
    """
    try:
        text_parts = await page.evaluate(js)
        if not isinstance(text_parts, list):
            text_parts = []
        else:
            # Keep only strings, ignore anything else
            text_parts = [str(x) for x in text_parts if isinstance(x, str)]
    except Exception as e:
        # print(f"[WARN] get_words_from_page failed: {e}")
        text_parts = []

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


async def handle_content_type(
    args,
    download_flag,
    regex,
    folder,
    default_name,
    type_label,
):
    """
    Generic handler for saving downloaded files of various content types.
    Reduces boilerplate between PDFs, DOCs, DBs, etc.
    """

    url = args.get("url")
    content_type = args.get("content_type")
    parent_host = args.get("parent_host")
    raw_content = args.get("raw_content")

    if not download_flag:
        # Skipping download by configuration
        return {
            url: {
                "url": url,
                "content_type": content_type,
                "source": f"content_type_{type_label}",
                "isopendir": False,
                "visited": True,
                "parent_host": parent_host,
            }
        }

    if not raw_content or not isinstance(raw_content, (bytes, bytearray)):
        results["crawledlinks"].add(url)
        return {
            url: {
                "url": url,
                "content_type": content_type,
                "source": f"content_type_{type_label}_empty",
                "isopendir": False,
                "visited": True,
                "parent_host": parent_host,
            }
        }

    try:
        base_filename = os.path.basename(urlparse(url).path) or default_name
        try:
            decoded_name = unquote(base_filename)
        except Exception:
            decoded_name = base_filename

        name_part, ext = os.path.splitext(decoded_name)

        if EXTENSION_MAP.get(ext) is not regex:
            if not is_octetstream(content_type):
                print(
                    f"\033[94m[SKIP {type_label.upper()}] "
                    f"Extension '{ext}' not mapped to {type_label} regex ({content_type}). URL={url}\033[0m"
                )
            return {}

        # Sanitize
        name_part = re.sub(r"[^\w\-.]", "_", name_part) or default_name
        ext = re.sub(r"[^\w\-.]", "_", ext)

        # Hash prefix
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        max_name_length = MAX_FILENAME_LENGTH - len(url_hash) - 1 - len(ext)
        if len(name_part) > max_name_length:
            name_part = name_part[: max_name_length - 3] + "..."

        safe_filename = f"{url_hash}-{name_part}{ext}"
        filepath = os.path.join(folder, safe_filename)

        # Write safely
        with open(filepath, "wb") as f:
            f.write(raw_content)

        return {
            url: {
                "url": url,
                "content_type": content_type,
                "source": f"content_type_{type_label}_download",
                "isopendir": False,
                "visited": True,
                "parent_host": parent_host,
                "filename": safe_filename,
            }
        }

    except Exception as e:
        print(f"\033[91m[ERROR {type_label.upper()}] {url}: {e}\033[0m")
        return {
            url: {
                "url": url,
                "content_type": content_type,
                "source": f"content_type_{type_label}_failed",
                "isopendir": False,
                "visited": True,
                "parent_host": parent_host,
            }
        }

@function_for_content_type(content_type_font_regex)
async def content_type_fonts(args):
    return await handle_content_type(
        args,
        DOWNLOAD_FONTS,
        content_type_font_regex,
        FONTS_FOLDER,
        "font",
        "font",
    )


@function_for_content_type(content_type_video_regex)
async def content_type_videos(args):
    return await handle_content_type(
        args,
        DOWNLOAD_VIDEOS,
        content_type_video_regex,
        VIDEOS_FOLDER,
        "video",
        "video",
    )

@function_for_content_type(content_type_audio_regex)
async def content_type_audios(args):
    return await handle_content_type(
        args,
        DOWNLOAD_AUDIOS,
        content_type_audio_regex,
        AUDIOS_FOLDER,
        "audio",
        "audio",
    )

@function_for_content_type(content_type_pdf_regex)
async def content_type_pdfs(args):
    return await handle_content_type(
        args,
        DOWNLOAD_PDFS,
        content_type_pdf_regex,
        PDFS_FOLDER,
        "pdf",
        "pdf",
    )

@function_for_content_type(content_type_doc_regex)
async def content_type_docs(args):
    return await handle_content_type(
        args,
        DOWNLOAD_DOCS,
        content_type_doc_regex,
        DOCS_FOLDER,
        "doc",
        "doc",
    )

@function_for_content_type(content_type_database_regex)
async def content_type_databases(args):
    return await handle_content_type(
        args,
        DOWNLOAD_DATABASES,
        content_type_database_regex,
        DATABASES_FOLDER,
        "database",
        "database",
    )


@function_for_content_type(content_type_torrent_regex)
async def content_type_torrents(args):
    return await handle_content_type(
        args,
        DOWNLOAD_TORRENTS,
        content_type_torrent_regex,
        TORRENTS_FOLDER,
        "torrent",
        "torrent",
    )


@function_for_content_type(content_type_comic_regex)
async def content_type_comics(args):
    return await handle_content_type(
        args,
        DOWNLOAD_COMICS,
        content_type_comic_regex,
        COMICS_FOLDER,
        "comic",
        "comic",
    )


@function_for_content_type(content_type_compressed_regex)
async def content_type_compresseds(args):
    return await handle_content_type(
        args,
        DOWNLOAD_COMPRESSEDS,
        content_type_compressed_regex,
        COMPRESSEDS_FOLDER,
        "compressed",
        "compressed",
    )


@function_for_content_type(content_type_midi_regex)
async def content_type_midis(args):
    return await handle_content_type(
        args,
        DOWNLOAD_MIDIS,
        content_type_midi_regex,
        MIDIS_FOLDER,
        "midi",
        "midi",
    )


def is_octetstream(content_type: str) -> bool:
    for pattern in content_type_octetstream:
        if re.match(pattern, content_type, re.IGNORECASE):
            return True
    return False


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

def remove_blocked_hosts_from_es_db(db):
    compiled_blocklist = [re.compile(pattern) for pattern in HOST_REGEX_BLOCK_LIST]
    def is_blocked(host):
        return any(regex.search(host) for regex in compiled_blocklist)
    def delete_from_index(index_pattern: str, label: str) -> int:
        print(f"Deleting blocked hosts from {label}")
        deleted = 0
        query = {"query": {"match_all": {}}}
        for doc in helpers.scan(db.es, index=index_pattern, query=query):
            url = doc["_source"].get("url")
            if not url:
                continue
            host = urlsplit(url).hostname or ""
            if is_blocked(host):
                db.es.delete(index=doc["_index"], id=doc["_id"])
                print(f"Deleted: {url} (from {doc['_index']})")
                deleted += 1
        return deleted
    total_deleted = 0
    total_deleted += delete_from_index(f"{LINKS_INDEX}-*", LINKS_INDEX)
    total_deleted += delete_from_index(f"{CONTENT_INDEX}-*", CONTENT_INDEX)
    print(f"\nDone. Total deleted: {total_deleted}")

def remove_empty_content_type_from_es_db(db):
    print(f"Starting safe bulk delete of empty content_type docs from {CONTENT_INDEX}")

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"term": {"content_type.keyword": ""}},
                                {"bool": {"must_not": {"exists": {"field": "content_type.keyword"}}}}
                            ]
                        }
                    },
                    {"term": {"visited": False}}
                ]
            }
        }
    }

    try:
        # Keep it foreground: wait_for_completion=True
        response = db.es.delete_by_query(
            index=f"{CONTENT_INDEX}-*",
            body=query,
            refresh=True,
            wait_for_completion=True,  # block until it's done
            slices="auto",             # parallelize internally
            conflicts="proceed",
            timeout="2m"               # extend if your deletes are heavy
        )

        deleted = response.get("deleted", 0)
        print(f"Foreground delete finished. Deleted: {deleted}")
        return deleted

    except Exception as e:
        print(f"Error in bulk delete: {e}")
        return 0


def remove_blocked_urls_from_es_db(db):
    # Compile path-based regex block list
    compiled_url_blocklist = [re.compile(pattern) for pattern in URL_REGEX_BLOCK_LIST]

    def is_blocked_path(path: str) -> bool:
        return any(regex.search(path) for regex in compiled_url_blocklist)

    def process_index(index_pattern: str, label: str) -> int:
        deleted = 0
        query = {"query": {"match_all": {}}}

        print(f"Deleting blocked URLs from {label}")

        for doc in helpers.scan(db.es, index=index_pattern, query=query):
            url = doc["_source"].get("url")
            if not url:
                continue
            path = urlsplit(url).path or ""
            if is_blocked_path(path):
                db.es.delete(index=doc["_index"], id=doc["_id"])
                print(f"Deleted by path: {url}")
                deleted += 1

        return deleted

    total_deleted = 0
    total_deleted += process_index(f"{LINKS_INDEX}-*", LINKS_INDEX)   # <-- added ()
    total_deleted += process_index(f"{CONTENT_INDEX}-*", CONTENT_INDEX)  # <-- added ()

    print(f"\nDone. Total deleted by path: {total_deleted}")

def remove_repeated_segments_urls_from_es_db(db):
    # Compile path-based regex block list
    def process_index(index_pattern: str, label: str) -> int:
        deleted = 0
        query = {"query": {"match_all": {}}}

        print(f"Deleting blocked URLs from {label}")

        for doc in helpers.scan(db.es, index=index_pattern, query=query):
            url = doc["_source"].get("url")
            if not url:
                continue
            if has_repeated_segments(url):
                db.es.delete(index=doc["_index"], id=doc["_id"])
                print(f"Deleted by repeated segments: {url}")
                deleted += 1

        return deleted

    total_deleted = 0
    total_deleted += process_index(f"{LINKS_INDEX}-*", LINKS_INDEX)   # <-- added ()
    total_deleted += process_index(f"{CONTENT_INDEX}-*", CONTENT_INDEX)  # <-- added ()

    print(f"\nDone. Total deleted by path: {total_deleted}")

async def process_input_url_files(db):
    if not os.path.isdir(INPUT_FOLDER):
        return

    while True:
        files = [f for f in os.listdir(INPUT_FOLDER) if os.path.isfile(os.path.join(INPUT_FOLDER, f))]
        if not files:
            print("No more input files to process.")
            break

        file_to_process = os.path.join(INPUT_FOLDER, random.choice(files))
        print(f"Processing input file: {file_to_process}")

        try:
            # Try normal UTF-8 read
            with open(file_to_process, "r", encoding="utf-8") as f:
                lines = f.readlines()

        except UnicodeDecodeError as e:
            print(f"Unicode error in {file_to_process}: {e}")
            print("Retrying with line-by-line decode (bad chars replaced).")

            lines = []
            with open(file_to_process, "rb") as f:  # binary read
                for i, raw_line in enumerate(f, 1):
                    try:
                        line = raw_line.decode("utf-8")
                    except UnicodeDecodeError as e:
                        print(f"Problem in line {i}: {e} -> replacing bad chars")
                        line = raw_line.decode("utf-8", errors="replace")
                    lines.append(line)

        if not lines:
            print(f"File is empty, deleting: {file_to_process}")
            os.remove(file_to_process)
            continue

        urls_to_process = lines[:MAX_URLS_FROM_FILE]
        remaining = lines[MAX_URLS_FROM_FILE:]

        for url in urls_to_process:
            url = url.strip()
            if not url:
                continue
            try:
                print('    [FILE] {}'.format(url))
                async with async_playwright() as playwright:
                    await get_page(url, playwright, db)
            except Exception as e:
                print(f"Error crawling {url}: {e}")

        # Rewrite file with remaining lines
        if remaining:
            with open(file_to_process, "w", encoding="utf-8") as f:
                f.writelines(remaining)
        else:
            os.remove(file_to_process)
            print(f"File fully processed and removed: {file_to_process}")


def remove_invalid_urls(db):
    def process_index(index_pattern: str, label: str) -> int:
        deleted = 0
        query = {"query": {"match_all": {}}}

        print(f"Deleting invalid URLs from {label}")

        for doc in helpers.scan(db.es, index=index_pattern, query=query, size=1000, scroll="5m" ):
            url = doc["_source"].get("url")
            if not url:
                continue

            parsed = urlparse(url)
            pre_url = url
            url = sanitize_url(url)

            # Remove if URL changed after sanitization
            if pre_url != url:
                print(f"Deleted sanitized URL: -{pre_url}- inserting -{url}-")
                results["crawledlinks"].add(url)
                db.es.delete(index=doc["_index"], id=doc["_id"])
                deleted += 1
                continue

            # Remove if completely missing a scheme (e.g., "www.example.com")
            if not parsed.scheme:
                print(f"Deleted URL with no scheme: -{url}-")
                db.es.delete(index=doc["_index"], id=doc["_id"])
                deleted += 1

        return deleted

    total_deleted = 0
    total_deleted += process_index(f"{LINKS_INDEX}-*", LINKS_INDEX)
    total_deleted += process_index(f"{CONTENT_INDEX}-*", CONTENT_INDEX)

    print(f"\nDone. Total invalid URLs deleted: {total_deleted}")

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
    try:
        host = '://'.join(urlsplit(url)[:2])
        dtree = []
        parts = PurePosixPath(unquote(urlparse(url).path)).parts
        for iter in range(1, len(parts[0:])):
            dtree.append(str(host + '/' + '/'.join(parts[1:-iter])))
        return dtree
    except Exception as e:
        #print(f"[WARN] Skipping invalid URL in get_directory_tree(): {url} — {e}")
        return []

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
    try:
        # Try waiting a little, but don't block forever
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception as e:
            pass
            #print(f"[WARN] Load state not reached for {page.url}: {e}")

        js = f"""
        () => {{
            const blocklist = new Set({list(soup_tag_blocklist)});
            function getTextNodes(node) {{
                if (!node) return [];
                let texts = [];
                if (node.nodeType === Node.TEXT_NODE) {{
                    const text = node.textContent.trim();
                    if (text.length > 0) texts.push(text);
                }} else if (node.nodeType === Node.ELEMENT_NODE && !blocklist.has(node.tagName?.toLowerCase?.())) {{
                    for (const child of (node.childNodes || [])) {{
                        texts = texts.concat(getTextNodes(child));
                    }}
                }}
                return texts;
            }}
            return getTextNodes(document.body) || [];
        }}
        """

        text_parts = await page.evaluate(js)
        combined_text = " ".join(text_parts)
        return combined_text[:MAX_WEBCONTENT_SIZE]

    except Exception as e:
        #print(f"[WARN] Failed extracting minimal webcontent from {page.url}: {e}")
        return ""


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

def get_instance_number():
    global lock_file
    try:
        os.makedirs("/tmp/instance_flags", exist_ok=True)
        for i in range(1, 100):
            lock_path = f"/tmp/instance_flags/instance_{i}.lock"
            lock_file = open(lock_path, "w")  # keep open!
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return i
            except BlockingIOError:
                lock_file.close()
                continue
    except Exception as e:
        print(f"Error determining instance number: {e}")
    return 999


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
            msg = str(e)
            if "Unable to retrieve content because the page is navigating" in msg:
                pass
            else:
                print(f"page.content() failed (attempt {i+1}): {e}")
                await asyncio.sleep(delay)
    return ""

def get_random_unvisited_domains(db, size=RANDOM_SITES_QUEUE):
    """
    Selects a random set of unvisited domains from the database using weighted strategies.

    This function dynamically chooses one domain selection method based on configurable
    probability weights defined in `METHOD_WEIGHTS`. Each method represents a different
    strategy for selecting host domains (e.g., least populated, oldest, random, or by prefix).
    If no custom weights are provided, all methods are assigned equal probability.

    The selected method is executed to retrieve up to `size` unvisited domains from the
    Elasticsearch index (or other data source provided by `db`). The goal is to maximize
    crawling diversity and minimize bias toward over-crawled hosts.

    Selection methods:
        - **fewest_urls**: Prefer hosts with the fewest URLs stored.
        - **oldest**: Prefer domains with the oldest last-visited timestamps.
        - **host_prefix**: Pick domains by random host prefix grouping.
        - **random**: Select completely random unvisited domains.

    Args:
        db: Database or Elasticsearch client instance used for querying.
        size (int, optional): Number of domains to retrieve. Defaults to `RANDOM_SITES_QUEUE`.

    Returns:
        list: A list of domain URLs selected according to the chosen method. 
              Returns an empty list if no valid methods are configured or if an error occurs.

    Exceptions:
        Catches and logs both `RequestError` (from Elasticsearch) and generic exceptions,
        returning an empty list in both cases.

    """    
    # Default weights if none provided
    if METHOD_WEIGHTS is None:
        method_weights = {
            "fewest_urls":  1,
            "oldest":       1,
            "host_prefix":  1,
            "random":       1
        }
    else:
        method_weights = METHOD_WEIGHTS

    # Filter out methods with zero weight
    active_methods = {name: weight for name, weight in method_weights.items() if weight > 0}

    # If no methods have weights > 0, return empty list
    if not active_methods:
        print("No active methods configured (all weights are 0)")
        return []

    # Normalize weights to sum to 1.0
    total_weight = sum(active_methods.values())
    normalized_weights = {name: weight/total_weight for name, weight in active_methods.items()}

    # Set up method mapping
    method_functions = {
        "fewest_urls": lambda: get_urls_from_least_populated_hosts(db, size=size),
        "oldest": lambda: get_oldest_host_domains(db, size=size),
        "host_prefix": lambda: get_urls_by_random_host_prefix(db, size=size),
        "random": lambda: get_random_host_domains(db, size=size)
    }

    try:
        # Choose method based on normalized weights
        methods = list(normalized_weights.keys())
        weights = list(normalized_weights.values())
        chosen_method = random.choices(methods, weights=weights, k=1)[0]
        print(f'Selected method: \033[32m{chosen_method}\033[0m')
        return method_functions[chosen_method]()

    except RequestError as e:
        print("Elasticsearch request error:", e)
        return []
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"Unhandled error in get_random_unvisited_domains: {e}")
        return []

def deduplicate_links_vs_content_es(
    db,
    links_index_pattern=f"{LINKS_INDEX}-*",
    content_index_pattern=f"{CONTENT_INDEX}-*",
    batch_size=5000
):
    """
    Deletes all docs from links_index_pattern that already exist in content_index_pattern
    by comparing the _id field (url_hash).
    Handles large indices with scroll and batches.
    """
    es = db.es
    print(f"Fetching _ids from {content_index_pattern} to deduplicate against {links_index_pattern}...")

    deleted_total = 0

    # --- Initialize scroll search ---
    scroll = es.search(
        index=content_index_pattern,
        scroll="2m",
        body={
            "size": batch_size,
            "query": {"match_all": {}},
            "_source": False
        }
    )

    scroll_id = scroll["_scroll_id"]
    hits = scroll["hits"]["hits"]

    while hits:
        # Extract IDs
        ids = [h["_id"] for h in hits]

        if ids:
            response = es.delete_by_query(
                index=links_index_pattern,
                body={"query": {"ids": {"values": ids}}},
                slices="auto",
                conflicts="proceed",
                refresh=True,
                wait_for_completion=True,
            )
            deleted_total += response.get("deleted", 0)

        # Fetch next scroll batch
        scroll = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = scroll["_scroll_id"]
        hits = scroll["hits"]["hits"]

    # Clear scroll
    try:
        es.clear_scroll(scroll_id=scroll_id)
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"[deduplicate_links_vs_content_es] {e}")

    print(f"Deduplication done. Deleted {deleted_total} docs from {links_index_pattern}.")
    return deleted_total

# pylint: disable=too-many-branches,too-many-locals
async def run_fast_extension_pass(db, max_workers=MAX_FAST_WORKERS): 
    """
    Perform a fast crawling pass on URLs with specific file extensions.

    This function queries the Elasticsearch database for URLs that match 
    configured file extensions (from `EXTENSION_MAP`) and performs asynchronous, 
    high-speed crawling on them using Playwright. It is designed for efficiently 
    collecting non-HTML content (e.g., PDFs, images, videos, etc.) while avoiding 
    redundancy and excessive load.

    Workflow:
        1. Deduplicates links already present in the content index.
        2. Randomizes the order of file extensions to distribute crawling effort.
        3. For each extension:
           - Queries Elasticsearch for matching URLs (regex or wildcard, depending on configuration).
           - Iterates through results using Elasticsearch's scroll API.
           - Selects at most one unique URL per host to minimize host bias.
           - Uses asyncio semaphores to run multiple crawlers concurrently (up to `max_workers`).
           - Processes crawler results (content and links) and stores them back in the database.
        4. Cleans up the Elasticsearch scroll context after processing.

    Args:
        db: 
            Database wrapper that provides Elasticsearch access and a `save_batch()` method 
            for saving results.
        max_workers (int, optional): 
            Maximum number of concurrent async tasks (default: `MAX_FAST_WORKERS`).

    Notes:
        - The crawling strategy aims to quickly gather downloadable content 
          (e.g., `.pdf`, `.zip`, `.mp4`) using lightweight Playwright requests.
        - Error handling is broad to prevent interruptions from isolated failures.
        - Uses randomization and host deduplication to spread requests evenly across domains.

    Raises:
        Exception: Any unexpected error during crawling or Elasticsearch operations 
                   is caught and logged, but not re-raised.

    """    
    print("Housekeeping links that are already in content.")
    deduplicate_links_vs_content_es(db)

    shuffled_extensions = list(EXTENSION_MAP.items())
    random.shuffle(shuffled_extensions)

    async with async_playwright() as playwright: 
        for extension, content_type_patterns in shuffled_extensions: # pylint: disable=too-many-nested-blocks
            await asyncio.sleep(FAST_DELAY)
            print(f"[FAST CRAWLER] Extension: {extension}")
            xtension = extension[1:]

            if STRICT_EXTENSION_QUERY:
                query = {
                    "bool": {
                        "must": [
                            {
                                "regexp": {
                                    "url.keyword": f".*\\/\\/.*\\/.*\\.{xtension}"
                                }
                            }
                        ]
                    }
                }
            else:
                query = {
                    "bool": {
                        "must": [
                            {"wildcard": {"url": f"*{extension}"}}
                        ]
                    }
                }

            try:
                scroll_size = 5000
                scroll = db.es.search(
                    index=f"{LINKS_INDEX}-*",
                    query=query,
                    size=scroll_size,
                    scroll="2m"
                )
                scroll_id = scroll["_scroll_id"]
                hits = scroll["hits"]["hits"]

                while hits:
                    urls = [hit["_source"]["url"] for hit in hits if "url" in hit["_source"]]
                    random.shuffle(urls)

                    # Track one URL per host
                    host_seen = set()
                    unique_urls = []
                    for url in urls:
                        host = urlparse(url).hostname
                        if host and host not in host_seen:
                            unique_urls.append(url)
                            host_seen.add(host)

                    results = {"crawledcontent": {}, "crawledlinks": set()} # pylint: disable=redefined-outer-name
                    semaphore = asyncio.Semaphore(max_workers)

                    async def sem_task(url, ctype_patterns=content_type_patterns, sem=semaphore):
                        async with sem:
                            return await fast_extension_crawler(url, ctype_patterns, db, playwright)



                    tasks = [sem_task(url) for url in unique_urls]
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)


                    for result in batch_results:
                        if isinstance(result, Exception):
                            pass
                        elif result:
                            presults = preprocess_crawler_data(result)
                            if isinstance(presults, list):
                                for item in presults:
                                    if "crawledcontent" in item:
                                        results["crawledcontent"].update(item["crawledcontent"])
                                    if "crawledlinks" in item:
                                        results["crawledlinks"].update(item["crawledlinks"])

                    if results["crawledcontent"] or results["crawledlinks"]:
                        db.save_batch([results])

                    scroll = db.es.scroll(scroll_id=scroll_id, scroll="2m")
                    scroll_id = scroll["_scroll_id"]
                    hits = scroll["hits"]["hits"]

                db.es.clear_scroll(scroll_id=scroll_id)

            except Exception as e: # pylint: disable=broad-exception-caught
                print(f"[run_fast_extension_pass] {url} {e}")


# pylint: disable=too-many-branches,too-many-locals
async def fast_extension_crawler(url, content_type_patterns, db, playwright):
    """
    Quickly determines how to handle a given URL based on its Content-Type header,
    downloading files of allowed types and delegating others to the standard crawler.

    This asynchronous function performs a lightweight HEAD request to determine
    a URL’s content type before deciding whether to:
    - Download and process the content immediately (for matching file types).
    - Delegate processing to `get_page()` when the content type is HTML-like or unsupported.
    - Skip URLs from blocklisted hosts or URLs not in the allow list.

    The function maps each recognized content type regex to a specialized handler
    (e.g., `content_type_images`, `content_type_docs`) and uses associated download flags
    (e.g., `DOWNLOAD_PDFS`, `DOWNLOAD_IMAGES`) to control what types of files to fetch.

    Args:
        url (str): The target URL to inspect or download.
        content_type_patterns (list[re.Pattern]): List of regex patterns defining
            acceptable Content-Type values to process quickly.
        db: Database handle or connection used by the main crawler for persistence.
        playwright: Playwright browser context or controller, used by fallback functions.

    Behavior:
        - Performs a HEAD request with randomized User-Agent.
        - Falls back to `get_page()` if the request fails, response is invalid,
          or content type is missing or mismatched.
        - Optionally downloads matching content types according to configured flags.
        - Updates the global `results["crawledcontent"]` dictionary with parsed results.
        - Randomly sleeps at the end to add delay variability between requests.

    Notes:
        - Broad exception handling is intentionally used to avoid breaking
          async loops during large-scale crawling.
        - Unknown content types trigger a visible warning message.
        - The function is intended for high-performance "fast path" crawling
          of non-HTML resources.

    """    
    headers = {"User-Agent": UserAgent().random}

    async def fallback():
        await get_page(url, playwright, db)
        return

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=10.0),
            verify=False,
            headers=headers,
            follow_redirects=True,
        ) as client:
            head_resp = await client.head(url)
    except Exception as e: # pylint: disable=broad-exception-caught
        if DEBUG_PW:
            print(f"[fast_extension_crawler fallback] {e}")
        return await fallback()

    if not (200 <= head_resp.status_code < 300):
        return await fallback()

    print(f"-{url}-")

    content_type = head_resp.headers.get("Content-Type", "")
    if not content_type:
        return await fallback()

    content_type = content_type.lower().split(";")[0].strip()

    if not any(re.match(p, content_type) for p in content_type_patterns):
        if content_type not in {
            "text/html",
            "text/plain",
            "application/json",
            "text/javascript",
        }:
            print(
                f"\033[92m[FAST CRAWLER] Mismatch content type for {url}, got: -{content_type}-\033[0m"
            )
        return await fallback()

    host = urlparse(url).hostname or ""
    if (
        is_host_block_listed(host)
        or not is_host_allow_listed(host)
        or is_url_block_listed(url)
        or has_repeated_segments(url)
    ):
        return

    # --- Lookup download flags by function name ---
    download_flags = {
        "content_type_audios": DOWNLOAD_AUDIOS,
        "content_type_compresseds": DOWNLOAD_COMPRESSEDS,
        "content_type_databases": DOWNLOAD_DATABASES,
        "content_type_docs": DOWNLOAD_DOCS,
        "content_type_fonts": DOWNLOAD_FONTS,
        "content_type_images": DOWNLOAD_SFW or DOWNLOAD_NSFW or DOWNLOAD_ALL_IMAGES,
        "content_type_midis": DOWNLOAD_MIDIS,
        "content_type_pdfs": DOWNLOAD_PDFS,
        "content_type_torrents": DOWNLOAD_TORRENTS,
        "content_type_comics": DOWNLOAD_COMICS,
    }

    try:
        for regex, function in content_type_functions:
            if not regex.search(content_type):
                continue

            flag = download_flags.get(function.__name__, False)
            content = None

            if flag:
                try:
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(30.0, connect=10.0),
                        verify=False,
                        headers=headers,
                        follow_redirects=True,
                    ) as client:
                        get_resp = await client.get(url)
                        content = get_resp.content
                except Exception as e: # pylint: disable=broad-exception-caught
                    if DEBUG_PW:
                        print(f"[fast_extension_crawler] {e}")
                    return

            doc = await function(
                {
                    "url": url,
                    "content": content,
                    "raw_content": content,
                    "content_type": content_type,
                    "parent_host": host,
                }
            )

            if doc:
                results["crawledcontent"].update(doc)
            break
        else:
            # Executed only if no break happened (no match found)
            print(f"\033[91m[FAST CRAWLER] UNKNOWN type -{url}- -{content_type}-\033[0m")

    except Exception as e: # pylint: disable=broad-exception-caught
        if DEBUG_PW: 
            print(f"[fast_extension_crawler] {e}")

    await asyncio.sleep(random.uniform(FAST_RANDOM_MIN_WAIT, FAST_RANDOM_MAX_WAIT))


def is_html_content(content_type: str) -> bool:
    """
    Check whether a given Content-Type value corresponds to HTML content.

    This function compares the provided MIME type string (e.g., "text/html", 
    "application/xhtml+xml") against a list of regular expression patterns 
    defined in `content_type_html_regex`. The comparison is case-insensitive.

    Args:
        content_type (str): The Content-Type header value to check.

    Returns:
        bool: True if the content type matches any HTML-related pattern, 
              False otherwise.
    """    
    return any(
        re.match(pattern, content_type, re.IGNORECASE)
        for pattern in content_type_html_regex
    )


async def get_page_async(url: str, playwright): # pylint: disable=too-many-statements,too-many-locals
    """
    Asynchronously crawls a web page using Playwright, extracts content, and processes linked resources.

    This function performs a full asynchronous crawl of a given URL with multiple fallback and parsing stages.
    It leverages Playwright for dynamic content rendering and integrates auxiliary async functions for
    content-type analysis, word and link extraction, and open directory detection.

    Workflow overview:
        1. **Browser setup** — Launches a headless Chromium instance with a random user agent.
        2. **Page load (crawl)** — Attempts to load the target URL (first without scrolling, then with scrolling)
           and determines the normalized `Content-Type`.
        3. **HTTPX fallback** — If Playwright fails to detect the content type, performs a lightweight
           async HTTP request using `httpx` as a backup.
        4. **Response handling** — Asynchronously intercepts and processes all responses using a `response` listener,
           invoking type-specific functions from `content_type_functions` when applicable.
        5. **Content extraction** — Optionally extracts HTML text, hyperlinks, keywords, minimal and raw
           web content, and detects open directory listings.
        6. **Cleanup** — Closes all Playwright contexts and removes listeners safely.

    The function updates two global dictionaries:
        - `results["crawledcontent"]`: Contains structured page metadata and extracted content.
        - `results["crawledlinks"]`: Contains discovered hyperlinks for potential future crawling.

    Args:
        url (str): The target URL to crawl.
        playwright: An active Playwright instance used to launch Chromium.

    Returns:
        None: This function does not return a value directly. Instead, it updates the global `results`
        dictionary with two main components:
            - `results["crawledcontent"][url]`: A structured object including:
                - `"url"`: The crawled URL.
                - `"content_type"`: The normalized content type string.
                - `"isopendir"` / `"opendir_pattern"`: Flags and regex patterns if an open directory is detected.
                - `"words"`: Extracted text tokens (if `EXTRACT_WORDS` is enabled).
                - `"raw_webcontent"` / `"min_webcontent"`: Extracted raw and minimized HTML segments.
                - `"visited"`: Boolean indicating the URL was successfully processed.
                - `"parent_host"`: The host domain of the original URL.
                - `"source"`: Always `'get_page_async'`.
            - `results["crawledlinks"]`: A set of newly discovered URLs found in the HTML.

    Raises:
        Exception: All internal exceptions are caught and logged, ensuring that a single failure does not
        interrupt the crawl process. The function prints contextual error messages instead of raising errors.

    Notes:
        - The function employs several internal helpers (`setup_browser`, `crawl`, `handle_response`, etc.)
          that encapsulate specific stages of the crawl pipeline.
        - Scroll-based loading is automatically retried if the initial page load lacks content type.
        - To avoid blocking, response handling is scheduled via `asyncio.create_task()` for each intercepted response.
        - Playwright’s `ignore_https_errors=True` is used to bypass invalid certificates.
        - The crawler is designed to support modular extension via `content_type_functions`, which map
          regex patterns of content types to custom async handlers.
    """    
    user_agent = ua.random
    parent_host = urlsplit(url)[1]
    page_data = {"crawledcontent": {}, "crawledlinks": set()}

    # --- Helper: setup and teardown ---
    async def setup_browser():
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=user_agent, ignore_https_errors=True,)
        page = await context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT_MS)
        return browser, page

    async def teardown_browser(browser, page, handler):
        page.remove_listener("response", handler)
        await browser.close()

    # --- Helper: crawl with or without scroll ---
    async def crawl(page, scroll: bool = False):
        """
        Navigate to the given URL within the provided Playwright page context 
        and return its detected content type.

        This helper performs a controlled page load sequence:
        - Loads the page using `page.goto()` and waits for the `domcontentloaded` event.
        - Normalizes the `Content-Type` header if present.
        - Optionally scrolls through the page to trigger lazy loading (if `scroll=True`).
        - Waits until the network becomes idle to ensure all async JS requests complete.
        - Adds a short post-idle delay (3 seconds) to catch late-loading dynamic content.

        Args:
            page (playwright.async_api.Page): The Playwright page instance to navigate.
            scroll (bool, optional): Whether to auto-scroll the page to trigger lazy content. Defaults to False.

        Returns:
            str | None: The sanitized content type of the loaded page, or None if loading failed.

        Notes:
            - If the response is missing or invalid, the function logs a warning and returns None.
            - The 3-second delay helps capture JS-rendered elements that appear slightly after `networkidle`.
        """        
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
                await asyncio.sleep(3)

            return ctype
        except Exception as e: # pylint: disable=broad-exception-caught
            if DEBUG_PW:
                print(f"[CRAWL] {e}")
            return None

    # --- Helper: fallback using httpx ---
    async def httpx_fallback():
        try:
            headers = {"User-Agent": user_agent}
            async with httpx.AsyncClient(
                verify=False, follow_redirects=True, timeout=PAGE_TIMEOUT_MS / 1000
            ) as client:
                resp = await client.get(url, headers=headers)
                return sanitize_content_type(resp.headers.get("content-type", ""))
        except Exception as e: # pylint: disable=broad-exception-caught
            if DEBUG_HTTPX:
                print(f"[HTTPX fallback failed] {url}: {e}")
            return ""

    # --- Helper: handle responses from Playwright ---
    async def handle_response(response):
        try:
            if page.is_closed():
                return

            rurl = response.url
            host = urlsplit(rurl)[1]
            ctype = response.headers.get("content-type")

            try:
                body_bytes_local = await response.body()
            except Exception as e: # pylint: disable=broad-exception-caught
                if DEBUG_PW:
                    print(f"[HANDLE_RESPONSE reading body_bytes_local ] {e}")
                return

            content = ""
            if body_bytes_local and ctype and any(t in ctype for t in ["text", "json", "xml"]):
                encoding = chardet.detect(body_bytes_local)["encoding"] or "utf-8"
                try:
                    content = body_bytes_local.decode(encoding, errors="replace")
                except Exception as e: # pylint: disable=broad-exception-caught
                    if DEBUG_PW:
                        print(f"[HANDLE_RESPONSE] {e}")
                    content = ""

            if ctype:
                ctype = sanitize_content_type(ctype)

            if (
                not is_host_block_listed(host)
                and is_host_allow_listed(host)
                and not is_url_block_listed(rurl)
                and not has_repeated_segments(rurl)
                and ctype
            ):
                found = False
                for regex, function in content_type_functions:
                    if regex.search(ctype):
                        found = True
                        try:
                            raw_content = (
                                body_bytes_local
                                if isinstance(body_bytes_local, (bytes, bytearray))
                                else None
                            )
                            urlresult = await function({
                                'url': rurl,
                                'content': content,
                                'content_type': ctype,
                                'raw_content': raw_content,
                                'parent_host': parent_host
                            })
                            page_data["crawledcontent"].update(urlresult)
                        except Exception as e: # pylint: disable=broad-exception-caught
                            if DEBUG_PW:
                                print(f"[HANDLE_RESPONSE] {e}")
                if not found:
                    print(f"\033[91mUNKNOWN type -{rurl}- -{ctype}-\033[0m")
        except Exception as e: # pylint: disable=broad-exception-caught
            if DEBUG_PW:
                print(f"[HANDLE_RESPONSE] Error handling response: {e}")


    # --- Helper: extract data from HTML page ---
    async def extract_page_data(page, content_type):
        html_text = await safe_content(page) if is_html_content(content_type) else ""
        links = await get_links_page(page, url) if is_html_content(content_type) else {}
        page_data["crawledlinks"].update(links)

        words = (
            await get_words_from_page(page)
            if EXTRACT_WORDS and is_html_content(content_type)
            else ''
        )
        raw_webcontent = (
            str(html_text)[:MAX_WEBCONTENT_SIZE]
            if EXTRACT_RAW_WEBCONTENT and is_html_content(content_type)
            else ''
        )
        min_webcontent = (
            await get_min_webcontent_page(page)
            if EXTRACT_MIN_WEBCONTENT and is_html_content(content_type)
            else ''
        )
        isopendir, pat = is_open_directory(str(html_text), url)

        return html_text, links, words, raw_webcontent, min_webcontent, isopendir, pat

    # --- Main logic starts here ---
    browser, page = await setup_browser()

    content_type = await crawl(page, scroll=False) or await crawl(page, scroll=True) or ""
    if not content_type:
        content_type = await httpx_fallback()

    #html_text, links, words, raw_webcontent, min_webcontent, isopendir, pat = await extract_page_data(page, content_type)
    _, _, words, raw_webcontent, min_webcontent, isopendir, pat = await extract_page_data(page, content_type)

    def handler(response):
        asyncio.create_task(handle_response(response))

    page.on("response", handler)

    try:
        await page.goto(url, wait_until="domcontentloaded")
    except Exception as e: # pylint: disable=broad-exception-caught
        if DEBUG_PW:
            print(f"[GET_PAGE_ASYNC] Error while fetching {url}: {e}")
    finally:
        await teardown_browser(browser, page, handler)

    found = any(regex.search(content_type) for regex, _ in content_type_functions)
    if not found:
        print(f"\033[91mUNKNOWN type -{url}- -{content_type}-\033[0m")

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


def total_memory_usage(process):
    """
    Calculate the total memory usage of a process and all its child processes.

    This function retrieves the Resident Set Size (RSS) — the portion of memory 
    held in RAM — for the given process and recursively includes all of its 
    child processes. The result is the total physical memory currently consumed 
    by the process tree.

    Args:
        process (psutil.Process): 
            A `psutil.Process` instance representing the root process to measure.

    Returns:
        float: 
            The total memory usage in megabytes (MB), including the process 
            and all its child processes.

    Notes:
        - Uses `process.memory_info().rss` to measure memory resident in RAM.
        - Recursively includes all subprocesses spawned by the main process.
        - Ideal for monitoring multi-process applications that 
          may spawn instances or worker threads.
    """    
    mem = process.memory_info().rss
    for child in process.children(recursive=True):
        mem += child.memory_info().rss
    return mem / (1024**2)  #MB


async def monitor_memory(pid, url, stop_event):
    """
    Monitors total system memory usage and stops the current task 
    if the host is under memory pressure.
    """
    process = psutil.Process(pid)
    while not stop_event.is_set():
        await asyncio.sleep(CHECK_INTERVAL)
        # Measure both process and system memory usage
        system_mem = psutil.virtual_memory().percent  # total system memory usage in %
        process_mem = process.memory_info().rss / (1024 * 1024)  # MB
        if system_mem > MAX_MEMORY_PERCENT and process_mem > ATTENTION_MEMORY_MB:
            print(f"\033[91m[WARN] System memory {system_mem:.1f}% and process_mem {process_mem} — aborting {url}\033[0m")
            stop_event.set()
            return
        # Optional: Also kill if *this* process uses too much memory
        if process_mem > MAX_MEMORY_MB :  # e.g., 6 GB
            print(f"\033[91m[WARN] Process memory {process_mem:.1f}MB — aborting {url}\033[0m")
            stop_event.set()
            return


async def get_page(url, playwright, db):
    """
    Asynchronously crawls a single web page, monitors memory usage, and stores results.

    This function orchestrates the lifecycle of crawling a URL using Playwright, 
    while continuously monitoring system memory usage to prevent crashes caused 
    by excessive resource consumption. It runs two concurrent tasks — the crawler 
    itself and a memory monitor — and aborts the crawl safely if the memory threshold 
    is exceeded.

    On success, the crawled data is preprocessed and saved to the database.
    On memory exhaustion, the function gracefully cancels the crawl, marks the URL 
    as visited, and stores a minimal record to avoid reprocessing.

    Args:
        url (str): The target URL to crawl.
        playwright: An active Playwright instance used for browser automation.
        db: The database interface responsible for persisting crawl results.

    Side Effects:
        - Updates the global `results` variable with crawled content and links.
        - Writes processed results to the database via `db.save_batch()`.
        - Cancels background tasks upon completion or error.

    Raises:
        MemoryError: If memory usage exceeds the configured threshold during the crawl.
        Exception: For any other unexpected errors during execution.

    Notes:
        - The function ensures that memory usage is constantly monitored through 
          `monitor_memory()`, and cancels the crawl if the limit is hit.
        - This design helps maintain crawler stability when handling large or 
          unexpectedly heavy pages.
    """    
    global results # pylint: disable=global-statement
    pid = psutil.Process().pid
    stop_event = asyncio.Event()
    memory_task = asyncio.create_task(monitor_memory(pid, url, stop_event))

    try:
        # Wrap both tasks explicitly
        crawl_task = asyncio.create_task(get_page_async(url, playwright))
        stop_task = asyncio.create_task(stop_event.wait())

        done, _ = await asyncio.wait(
            [crawl_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        if stop_task in done and stop_event.is_set():
            print(f"[INFO] Aborting {url} due to memory threshold.")
            crawl_task.cancel()
            raise MemoryError("Memory limit exceeded")

        if crawl_task in done:
            await crawl_task

        presults = preprocess_crawler_data(results)
        db.save_batch(presults)
        results = {"crawledcontent": {}, "crawledlinks": set()}

    except MemoryError as e:
        print(f"[ERROR] {e}")
        results["crawledcontent"].update({
            url: {
                "url": url,
                "content_type": "",
                "isopendir": False,
                "visited": True,
                "source": 'get_page_outofmemory',
            }
        })        
        presults = preprocess_crawler_data(results)
        db.save_batch(presults)
        results = {"crawledcontent": {}, "crawledlinks": set()}
        try:
            await playwright.stop()
        except Exception as ie: # pylint: disable=broad-exception-caught
            print(f"3675 {ie}")

    except PlaywrightError as e:
        if "Target page, context or browser has been closed" in str(e):
            print(f"[GET_PAGE Fatal] Browser closed unexpectedly while crawling {url}")
            results["crawledcontent"].update({
                url: {
                    "url": url,
                    "content_type": "",
                    "isopendir": False,
                    "visited": True,
                    "source": 'get_page_browser_closed',
                }
            })
            presults = preprocess_crawler_data(results)
            db.save_batch(presults)
            results = {"crawledcontent": {}, "crawledlinks": set()}
            try:
                await playwright.stop()
            except Exception as ie: # pylint: disable=broad-exception-caught
                print(f"3798 ---------------------------------------------  {ie}")
            sys.exit()
        else:
            print(f"3801 [PLAYWRIGHT] Error while crawling {url}: {e}")
            return None
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"3804 [ERROR] Unexpected error while crawling {url}: {e}")
        return None

    finally:
        memory_task.cancel()
        stop_task.cancel()


async def crawler(db):
    """
    Iterate over random urls
    """
    random_urls = get_random_unvisited_domains(db=db)
    for target_url in random_urls:
        try:
            print(f'    {target_url}')
            async with async_playwright() as playwright:
                await get_page(target_url, playwright, db)
        except UnicodeEncodeError:
            pass

async def main():
    """
    Crawler main function
    """
    db = DatabaseConnection()
    #urls_index, content_index = db_create_monthly_indexes(db)
    db_create_monthly_indexes(db)
    create_directories()

    parser = argparse.ArgumentParser(description="Crawler runner")
    parser.add_argument(
        "--initial",
        nargs="?",              # optional value
        const=True,             # means "--initial" without value is valid
        help="Run in initial mode (fetch INITIAL_URL by default, or a custom URL if provided)"
    )
    args = parser.parse_args()

    if args.initial:
        # If user provided a string, it's the URL. If it's just True, use INITIAL_URL.
        initial_url = None if args.initial is True else args.initial
        url = initial_url or INITIAL_URL
        async with async_playwright() as playwright:
            await get_page(url, playwright,db)
    else:
        instance = get_instance_number()
        for iteration in range(ITERATIONS):
            if instance == 1:
                if REMOVE_REPEATED_SEGMENTS:
                    print(f"Instance {instance}, iteration {iteration}: Removing urls with repeated segments.")
                    remove_repeated_segments_urls_from_es_db(db)
                if REMOVE_EMPTY_CTYPE:
                    print(f"Instance {instance}, iteration {iteration}: Removing urls with empty content_type.")
                    remove_empty_content_type_from_es_db(db)
                if REMOVE_BLOCKED_HOSTS:
                    print(f"Instance {instance}, iteration {iteration}: Removing urls from hosts that are blocklisted.")
                    remove_blocked_hosts_from_es_db(db)
                if REMOVE_BLOCKED_URLS:
                    print(f"Instance {instance}, iteration {iteration}: Removing path blocklisted urls.")
                    remove_blocked_urls_from_es_db(db)
                if REMOVE_INVALID_URLS:
                    print(f"Instance {instance}, iteration {iteration}: Deleting invalid urls.")
                    remove_invalid_urls(db)
                print(f"Instance {instance}, iteration {iteration}: Checking for input URL files...")
                await process_input_url_files(db)
                print(f"Instance {instance}, iteration {iteration}: Let's go full crawler mode.")
                await crawler(db)
            elif instance == 2:
                print(f"Instance {instance}, iteration {iteration}: Running fast extension pass only.")
                await run_fast_extension_pass(db)
                #await crawler(db)
            else:
                print(f"Instance {instance}, iteration {iteration}: Running full crawler.")
                await crawler(db)
        db.close()


if __name__ == "__main__":
    asyncio.run(main())

