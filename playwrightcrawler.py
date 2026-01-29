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
import dateutil.parser
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
        r"^application/x-download$",
        r"^application/octetstream$",
        r"^application/octet-stream$",
        r"^application/x-octet-stream$",
        r"^x-application/octet-stream$",
        r"^application/force-download$",
        r"^application/x-www-form-urlencoded$",
        r"^application/octet-stream,text/html$",
        r"^application/octet-streamtext/plain$",
        r"^application/octet-stream,text/plain$",
        r"^application/octet-stream,atext/plain$",
        r"^application/octet-streamCharset=UTF-8$",
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
        r"^GIF$",
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
        r"^image/jpegimage/jpeg$",
        r"^image/vnd\.wap\.wbmp$",
        r"^image/vnd.zbrush.pcx$",
        r"^image/vnd.tencent.tap$",
        r"^image/jpeg,image/jpeg$",
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
        r"^video/asf$",
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
        r"^application/x-gtar$",
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
    """
    Initialize or retrieve the current month's Elasticsearch index names.

    This function ensures that the monthly index naming convention is
    properly resolved for both the URL and content indexes. It uses the
    helper `get_index_name()` to construct the full index names based
    on the current date, typically following a pattern like:
    `urls-2025-11` and `content-2025-11`.

    Args:
        db: Database wrapper that provides access to an Elasticsearch client
            via `db.es`. If `None` or missing a valid client, a ValueError
            is raised.

    Returns:
        bool: 
            Always returns True once index names are successfully generated.

    Raises:
        ValueError: If `db` is None or does not contain a valid `es` client.

    Notes:
        - This function was previously designed to return both index names
          (`urls_index`, `content_index`) but now performs only validation
          and index name generation.
        - The actual index creation may occur elsewhere in the initialization
          pipeline.
    """        
    if db is None or db.es is None:  # <- changed from db.con
        raise ValueError("db connection is required")
    urls_index = get_index_name(LINKS_INDEX)
    content_index = get_index_name(CONTENT_INDEX)
    return True
    #return urls_index, content_index

def get_urls_by_random_timestamp_and_prefix(db, size=RANDOM_SITES_QUEUE, max_attempts=20):
    """
    Efficiently sample up to `size` random URLs (one per host) from Elasticsearch.

    Strategy:
      1. Pick a random character (a-z, 0-9) to filter hostnames by prefix.
      2. Choose a random timestamp between the earliest and latest document.
      3. Use `search_after` pagination to fetch results in small pages,
         stopping once enough unique hosts are collected or the index ends.

    This approach avoids a full index scan, maintains randomness both
    temporally and alphabetically, and scales efficiently to tens of millions
    of URLs.

    Args:
        db: Elasticsearch wrapper (must have `db.es` client).
        size (int): Number of unique host URLs to collect.
        max_attempts (int): Number of random timestamps to try before giving up.

    Returns:
        list[str]: A shuffled list of selected URLs (one per host).
    """
    urls_index = f"{LINKS_INDEX}-*"
    host_to_url = {}

    # Step 1: pick a random host prefix
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    chosen_char = random.choice(chars)
    print(f"[CHAR PICK] Filtering hosts starting with '{chosen_char}'")

    # Step 2: get timestamp range
    aggs_query = {
        "size": 0,
        "aggs": {
            "min_date": {"min": {"field": "created_at"}},
            "max_date": {"max": {"field": "created_at"}}
        }
    }
    stats = db.es.search(index=urls_index, body=aggs_query)
    min_ts = stats["aggregations"]["min_date"]["value"]
    max_ts = stats["aggregations"]["max_date"]["value"]

    if not min_ts or not max_ts:
        print("[WARN] Could not determine timestamp range")
        return []

    # Step 3: sample multiple random time slices until enough URLs are gathered
    for attempt in range(max_attempts):
        random_ts = int(random.uniform(min_ts, max_ts))
        print(f"[RANDOM DATE] Attempt {attempt+1}/{max_attempts} from {random_ts}")

        query = {
            "size": 500,  # page size
            "query": {
                "bool": {
                    "must": [
                        {"range": {"created_at": {"gte": random_ts}}},
                        {"prefix": {"host.keyword": chosen_char}}
                    ]
                }
            },
            "sort": [
                {"created_at": "asc"},
                {"url.keyword": "asc"}
            ]
        }

        last_sort = None
        while len(host_to_url) < size:
            if last_sort:
                query["search_after"] = last_sort

            res = db.es.search(index=urls_index, body=query)
            hits = res["hits"]["hits"]
            if not hits:
                break

            for doc in hits:
                src = doc["_source"]
                host = src.get("host")
                url = src.get("url")
                if not host or not url:
                    continue

                # Keep only one URL per host (replace with small probability)
                if host not in host_to_url or random.random() < 0.3:
                    host_to_url[host] = url

                if len(host_to_url) >= size:
                    break

            last_sort = hits[-1]["sort"]

        if len(host_to_url) >= size:
            break

    # Step 4: shuffle final list
    all_urls = list(host_to_url.values())
    random.shuffle(all_urls)
    print(f"[DONE] Collected {len(all_urls)} URLs from prefix '{chosen_char}'")

    return all_urls[:size]


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
    """
    Efficiently retrieve a random sample of URLs, one per host, using Elasticsearch search_after pagination.

    This version avoids scanning the entire index by:
    1. Selecting a random timestamp as a pivot point.
    2. Fetching documents in sorted order by timestamp using search_after.
    3. Keeping only one random URL per host to ensure wide domain coverage.

    Args:
        db: Elasticsearch database wrapper with an `es` client attribute.
        size (int): Maximum number of URLs to return.

    Returns:
        list[str]: A list of up to `size` URLs, each from a unique host.
    """
    urls_index = f"{LINKS_INDEX}-*"
    host_to_url = {}
    batch_size = 500

    # Pick a random timestamp from the dataset
    ts_query = {
        "size": 1,
        "query": {"match_all": {}},
        "sort": [{"created_at": "asc"}],
        "_source": ["created_at"]
    }
    first = db.es.search(index=urls_index, body=ts_query)
    ts_query["sort"] = [{"created_at": "desc"}]
    last = db.es.search(index=urls_index, body=ts_query)

    if not first["hits"]["hits"] or not last["hits"]["hits"]:
        return []

    first_ts = first["hits"]["hits"][0]["_source"]["created_at"]
    last_ts = last["hits"]["hits"][0]["_source"]["created_at"]

    # Choose a random timestamp between first and last
    if isinstance(first_ts, str):
        import dateutil.parser
        from datetime import datetime
        first_ts = dateutil.parser.isoparse(first_ts)
        last_ts = dateutil.parser.isoparse(last_ts)
    random_ts = first_ts + (last_ts - first_ts) * random.random()

    # Query for URLs newer than the random timestamp
    query = {
        "size": batch_size,
        "query": {
            "range": {
                "created_at": {"gte": random_ts.isoformat()}
            }
        },
        "sort": [{"created_at": "asc"}],
        "_source": ["url"]
    }

    total_fetched = 0
    search_after = None

    while total_fetched < size:
        if search_after:
            query["search_after"] = search_after

        response = db.es.search(index=urls_index, body=query)
        hits = response.get("hits", {}).get("hits", [])
        if not hits:
            break

        for doc in hits:
            url = doc["_source"].get("url")
            if not url:
                continue

            host = urlsplit(url).hostname
            if not host:
                continue

            # Keep only one random URL per host
            if host not in host_to_url or random.random() < 0.5:
                host_to_url[host] = url

            if len(host_to_url) >= size:
                break

        total_fetched += len(hits)
        search_after = hits[-1]["sort"] if hits else None

        if not search_after:
            break

    urls = list(host_to_url.values())
    random.shuffle(urls)
    return urls[:size]


def get_oldest_host_domains(db, size=RANDOM_SITES_QUEUE):
    """
    Retrieve one of the oldest known URLs per host from Elasticsearch.

    This function queries the links index (across all date-partitioned
    indices) sorted by creation time in ascending order, then selects the
    first encountered URL for each distinct host. Because results are sorted
    oldest-first, this effectively returns the oldest known URL per host.

    The process stops early once the requested number of unique hosts is
    collected.

    Parameters
    ----------
    db
        Database connection object providing access to an Elasticsearch client.
        Expected to expose an ``es`` attribute.
    size : int, optional
        Maximum number of unique host URLs to return. Defaults to
        ``RANDOM_SITES_QUEUE``.

    Returns
    -------
    list[str]
        A list of URLs, each belonging to a different host, ordered implicitly
        by earliest discovery time.

    Notes
    -----
    - Queries all indices matching ``{LINKS_INDEX}-*``.
    - Assumes a ``created_at`` field is present and sortable.
    - Only a single URL is returned per host.
    - Stops scanning results as soon as the desired number of hosts is reached
      to reduce unnecessary processing.
    """
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
    """
    Generate a deterministic document ID from a URL.

    This function computes a SHA-256 hash of the given URL and returns its
    hexadecimal representation. The resulting value is suitable for use as
    a stable Elasticsearch document ID, ensuring that the same URL always
    maps to the same identifier.

    Parameters
    ----------
    url : str
        The URL to be hashed.

    Returns
    -------
    str
        A hexadecimal SHA-256 hash representing the URL.

    Notes
    -----
    - Produces a fixed-length, collision-resistant identifier.
    - Avoids issues with special characters or length limits in document IDs.
    - Deterministic by design: identical URLs yield identical IDs.
    """
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def get_index_name(base: str) -> str:
    """
    Generate a time-partitioned Elasticsearch index name.

    This function appends a UTC year-month suffix (``YYYY-MM``) to the given
    base index name, producing indices suitable for monthly partitioning.

    Parameters
    ----------
    base : str
        Base index name without any date suffix.

    Returns
    -------
    str
        The full index name with a ``YYYY-MM`` UTC suffix appended.

    Notes
    -----
    - The timestamp is generated using timezone-aware UTC time.
    - Useful for organizing indices by month while keeping caller logic simple.
    """
    suffix = datetime.now(timezone.utc).strftime("%Y-%m")
    return f"{base}-{suffix}"


class DatabaseConnection:
    """
    Lightweight abstraction layer over the Elasticsearch client.

    This class centralizes Elasticsearch configuration, connection lifecycle
    management, and common operations used by the crawler, such as searching,
    scrolling, and bulk persistence of crawled content and discovered links.

    Index names are automatically suffixed with a UTC year-month component,
    enabling time-partitioned indices without leaking that concern to callers.

    Responsibilities
    ----------------
    - Initialize and configure the Elasticsearch client.
    - Provide proxy methods for core Elasticsearch APIs (e.g. search, scroll).
    - Manage connection lifecycle (open/close).
    - Persist crawled content and links using efficient bulk operations.

    Design Notes
    ------------
    - Acts as a thin wrapper; most methods delegate directly to the underlying
      Elasticsearch client without additional validation or transformation.
    - Intended to decouple the rest of the codebase from the concrete
      Elasticsearch client for easier testing, mocking, and future extension.
    - All timestamps are stored as timezone-aware UTC datetimes.

    Attributes
    ----------
    es : elasticsearch.Elasticsearch
        The underlying Elasticsearch client instance.
    con : elasticsearch.Elasticsearch
        Alias for ``es``, kept for compatibility or semantic clarity.
    """
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
        """
        Close the underlying Elasticsearch client connection.

        This method delegates directly to the Elasticsearch client’s ``close``
        method and should be called when the database connection is no longer
        needed to ensure resources such as open HTTP connections are released.

        Notes
        -----
        - Safe to call during application shutdown or teardown.
        - No additional cleanup or state management is performed.
        """
        self.es.close()

    def search(self, *args, **kwargs):
        """
        Proxy method for the Elasticsearch search API.

        This method forwards all positional and keyword arguments directly to the
        underlying Elasticsearch client’s ``search`` method. It provides a stable
        abstraction layer so calling code does not depend on the concrete client
        implementation.

        Parameters
        ----------
        *args
            Positional arguments passed directly to
            ``elasticsearch.Elasticsearch.search``.
        **kwargs
            Keyword arguments passed directly to
            ``elasticsearch.Elasticsearch.search``.

        Returns
        -------
        Any
            The response returned by the Elasticsearch ``search`` call.

        Notes
        -----
        - No additional logic, validation, or result processing is performed.
        - Useful for mocking, instrumentation, or extending behavior later without
          changing existing call sites.
        """
        return self.es.search(*args, **kwargs)

    def scroll(self, *args, **kwargs):
        """
        Proxy method for the Elasticsearch scroll API.

        This method forwards all positional and keyword arguments directly to the
        underlying Elasticsearch client’s ``scroll`` method. It exists primarily
        to provide a consistent interface or abstraction layer within the class.

        Parameters
        ----------
        *args
            Positional arguments passed directly to ``elasticsearch.Elasticsearch.scroll``.
        **kwargs
            Keyword arguments passed directly to ``elasticsearch.Elasticsearch.scroll``.

        Returns
        -------
        Any
            The response returned by the Elasticsearch ``scroll`` call.

        Notes
        -----
        - No additional logic or validation is performed.
        - Useful for mocking, instrumentation, or future extension without
          changing call sites.
        """        
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

        print(f"[BULK] Inserted {len(actions) - failed_count} docs successfully, {failed_count} failed")




def create_directories():
    """
    Create all required output and working directories.

    This function ensures that every directory used by the crawler and
    downloader subsystems exists. It creates directories for images,
    media files, documents, archives, databases, and input queues as
    defined by the corresponding global path constants.

    Existing directories are left untouched.

    Notes
    -----
    - Directory creation is idempotent via ``exist_ok=True``.
    - All directory paths are expected to be defined as global constants.
    - Intended to be called during application startup or initialization.
    """    
    dirs = [IMAGES_FOLDER, NSFW_FOLDER, SFW_FOLDER , FONTS_FOLDER, VIDEOS_FOLDER,  MIDIS_FOLDER , AUDIOS_FOLDER, PDFS_FOLDER ,DOCS_FOLDER , DATABASES_FOLDER, TORRENTS_FOLDER,COMPRESSEDS_FOLDER , COMICS_FOLDER, INPUT_FOLDER]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def get_host_levels(hostname):
    """
    Decompose a hostname into hierarchical host levels.

    This function splits a hostname into its dot-separated components,
    removes any port information, and returns both an ordered list of host
    levels and a dictionary mapping each level to a named key
    (e.g., ``host_level_1``, ``host_level_2``).

    The resulting structure is useful for grouping, filtering, or aggregating
    data by domain hierarchy in systems such as Elasticsearch.

    Parameters
    ----------
    hostname : str
        A hostname or host:port string (e.g., ``"sub.example.com:8080"``).

    Returns
    -------
    dict
        A dictionary with two keys:
        - ``host_levels`` (list[str]): The hostname split into components,
          ordered from left to right (e.g., ``["sub", "example", "com"]``).
        - ``host_level_map`` (dict[str, str]): A mapping of host level field
          names (``host_level_1`` ... ``host_level_N``) to their corresponding
          hostname components.

    Notes
    -----
    - Port numbers are stripped before processing.
    - No public suffix normalization is performed (e.g., ``co.uk`` handling).
    - Field naming is designed for structured indexing and querying.
    """    
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
    """
    Determine whether a URL uses an embedded or non-navigable scheme.

    This function checks if the given URL starts with schemes that represent
    embedded, inline, or browser-internal resources rather than externally
    fetchable locations. Such URLs are typically ignored by crawlers since
    they do not point to standalone network resources.

    Parameters
    ----------
    url : str
        The URL to evaluate.

    Returns
    -------
    bool
        ``True`` if the URL uses an embedded or internal scheme
        (e.g., ``data:``, ``blob:``, ``about:``, ``javascript:``),
        otherwise ``False``.

    Notes
    -----
    - Embedded URLs often contain inline content or browser-generated objects.
    - Excluding these URLs helps avoid invalid requests and crawler noise.
    """    
    return url.startswith(("data:", "blob:", "about:", "javascript:"))


# pylint: disable= too-many-locals,too-many-statements,too-many-branches
def preprocess_crawler_data(data: dict) -> dict:
    """
    Normalize, enrich, and filter crawler output before persistence.

    This function takes the raw results produced by the crawler and performs
    several preprocessing steps to prepare the data for storage and further
    crawling. It filters invalid or undesired URLs, expands directory paths
    when open-directory hunting is enabled, normalizes URLs, and enriches
    crawled documents with derived metadata.

    The preprocessing pipeline includes:
    - URL normalization and sanitization
    - Host, URL, and pattern allow/block list enforcement
    - Detection and exclusion of embedded or oversized URLs
    - Expansion of directory paths for open-directory discovery
    - Extraction of query parameters and file extensions
    - Computation of host and directory hierarchy levels
    - Injection of crawler-specific metadata (e.g. node ID)

    Parameters
    ----------
    data : dict
        A dictionary containing crawler output with the following keys:
        - ``crawledcontent`` (dict): Mapping of URL to extracted document data.
        - ``crawledlinks`` (set | iterable): Collection of discovered URLs
          pending processing.

    Returns
    -------
    dict
        A dictionary with two keys:
        - ``crawledcontent`` (dict): The filtered and enriched content documents,
          ready for persistence.
        - ``crawledlinks`` (dict): A mapping of normalized URLs to their
          corresponding hostnames, suitable for enqueueing future crawl tasks.

    Notes
    -----
    - URLs with repeated path segments, blocked hosts, blocked URLs, missing
      hosts, embedded data (e.g. base64), or excessive length are discarded.
    - When ``HUNT_OPEN_DIRECTORIES`` is enabled, directory trees are expanded
      recursively to increase discovery coverage.
    - Host and directory levels are padded to fixed sizes
      (``MAX_HOST_LEVELS``, ``MAX_DIR_LEVELS``) to ensure consistent indexing.
    - Query variables and values are extracted only when present.
    - All failures during URL normalization are caught and logged, preventing
      a single malformed URL from interrupting the pipeline.
    """    
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

        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"[PREPROCESS_CRAWLER_DATA NORMALIZATION] Failed to normalize {url}: {e}. This url won't be persisted.")
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

            # --- node_id ---

            insert_only_fields["node_id"] = NODE_ID

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
    """
    Normalize a URL path into fixed directory levels.

    This function splits a URL path into its directory components and pads
    the result to a fixed length defined by ``MAX_DIR_LEVELS``. It returns both
    an ordered list of directory levels and a dictionary mapping each level to
    a named key (e.g., ``directory_level_1``, ``directory_level_2``).

    This structure is useful for indexing, analytics, or grouping URLs by
    hierarchical depth in systems such as Elasticsearch.

    Parameters
    ----------
    url_path : str
        The path portion of a URL (e.g., ``"/a/b/c/"`` or ``"/products/item"``).

    Returns
    -------
    dict
        A dictionary with two keys:
        - ``directory_levels`` (list[str]): A list of directory names, padded
          with empty strings to reach ``MAX_DIR_LEVELS``.
        - ``directory_level_map`` (dict[str, str]): A mapping of directory level
          names (``directory_level_1`` ... ``directory_level_N``) to their
          corresponding directory values.

    Notes
    -----
    - Empty path segments and leading/trailing slashes are ignored.
    - Padding ensures consistent field availability when storing structured
      data in document-oriented databases.
    """    
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
    """
    Decorator factory that registers a function to handle URLs matching specific patterns.

    This factory accepts a list of regular expression patterns and returns a
    decorator. When applied to a function, the decorator compiles each regex
    and stores it along with the function in the global `url_functions` list.
    Later, when the crawler encounters a URL, it can select the appropriate
    handler function based on the first matching regex.

    Parameters
    ----------
    regexp_list : list[str]
        A list of regular expression strings. Each regex represents a URL
        pattern that the decorated function should handle (e.g., mailto
        links, relative paths, absolute HTTP URLs).

    Returns
    -------
    Callable
        A decorator that registers the decorated function as a handler for all
        provided URL patterns.

    Notes
    -----
    - The decorated function is not modified; it is simply registered.
    - Regexes are compiled with case-insensitive (`re.I`) and Unicode (`re.U`)
      flags.
    - A global list named `url_functions` must exist and be writable.
    - This pattern allows modular, extensible URL handling within the crawler.
    """    
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
    """
    Handle relative and partially qualified URLs discovered in a page.

    This function processes URLs that do not include a scheme or hostname,
    such as paths starting with ``/``, ``./``, ``../``, or loosely structured
    relative references commonly found in malformed or dynamically generated
    HTML. The URL is resolved against the parent page URL to produce a fully
    qualified absolute URL suitable for crawling.

    Parameters
    ----------
    args : dict
        A dictionary containing at least:
        - ``url`` (str): The relative or partially qualified URL found in the page.
        - ``parent_url`` (str): The absolute URL of the page where the link appeared.

    Returns
    -------
    list[dict]
        A list containing a single URL record with the following fields:
        - ``url``: The resolved absolute URL obtained via ``urljoin``.
        - ``visited``: Always ``False``, indicating the URL is pending crawl.
        - ``source``: The string ``"relative_url"``.
        - ``parent_host``: Hostname extracted from ``parent_url``.
        - ``host``: Hostname extracted from the resolved URL.

    Notes
    -----
    - This handler is intentionally permissive to catch non-standard or
      malformed relative links often found in real-world HTML.
    - Final URL normalization and de-duplication are expected to be handled
      by downstream components of the crawler.
    """    
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
    """
    Handle fully qualified URLs discovered during crawling.

    This function processes absolute URLs that already include a scheme
    (such as ``http://``, ``https://``, or ``ftp://``). It registers the URL
    as a new crawl candidate, associates it with the parent page host, and
    marks it as unvisited so it can be scheduled for crawling later.

    Parameters
    ----------
    args : dict
        A dictionary containing at least:
        - ``url`` (str): The fully qualified URL that was found.
        - ``parent_url`` (str): The URL of the page where this link appeared.

    Returns
    -------
    list[dict]
        A list containing a single URL record with the following fields:
        - ``url``: The discovered absolute URL.
        - ``visited``: Always ``False``, indicating it has not been crawled yet.
        - ``source``: The string ``"full_url"``.
        - ``parent_host``: Hostname extracted from ``parent_url``.
        - ``host``: Hostname extracted from the discovered URL.

    Notes
    -----
    - This handler does not normalize or de-duplicate URLs; that is expected
      to be handled by later stages of the crawling pipeline.
    - FTP URLs are included for completeness and treated the same as HTTP(S)
      URLs at this stage.
    """    
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
    """
    Extract and normalize email addresses from mail-style URLs.

    This function handles URLs that represent email links or textual email
    prefixes (for example, ``mailto:``, ``email:``, or common misspellings).
    It attempts to extract a valid email address from the URL, validate it
    using a strict regular expression, and return it in a structured result
    suitable for indexing.

    The extracted email is associated with the parent page URL and host,
    allowing correlation between discovered email addresses and the page
    where they were found.

    Parameters
    ----------
    args : dict
        A dictionary containing at least:
        - ``url`` (str): The raw URL or string containing the email prefix.
        - ``parent_url`` (str): The URL of the page where the email was found.

    Returns
    -------
    list[dict]
        A list containing a single result dictionary when a valid email
        address is found, or an empty list if extraction or validation fails.
        Each result includes:
        - ``url``: A composite identifier combining parent URL and email.
        - ``emails``: A list containing the extracted email address.
        - ``visited``: Always ``True`` for successfully parsed entries.
        - ``source``: The string ``"email_url"``.
        - ``parent_host``: Hostname of the parent URL.
        - ``host``: Same as ``parent_host``.
        - ``isopendir``: Always ``False``.

    Notes
    -----
    - Multiple common misspellings and localized variants of email prefixes
      are intentionally supported.
    - Email validation is conservative to avoid indexing malformed addresses.
    """    
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
    """
    Extract and rank relevant words from a BeautifulSoup document.

    This function walks through all text nodes in the provided BeautifulSoup
    object, excluding text that belongs to tags listed in
    `soup_tag_blocklist` (such as scripts or styles). The remaining text is
    concatenated into a single string and processed to extract the most
    relevant or frequent words.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        A parsed HTML document from which visible text content will be
        extracted.

    Returns
    -------
    list[str]
        A list of words extracted from the visible text of the document,
        as determined by `extract_top_words_from_text`.

    Notes
    -----
    - Tag filtering is based on the parent tag name of each text node.
    - The actual word selection, normalization, and ranking logic is delegated
      to `extract_top_words_from_text`.
    """    
    text_parts = [
        t for t in soup.find_all(string=True)
        if t.parent.name not in soup_tag_blocklist
    ]
    combined_text = " ".join(text_parts)
    return extract_top_words_from_text(combined_text)


# pylint: disable= too-many-locals,too-many-statements, dangerous-default-value
def sanitize_url(
        url,
        skip_log_tags=['FINAL_NORMALIZE',
                       'STRIP_WHITESPACE',
                       'NORMALIZE_PATH_SLASHES']):
    """
    Sanitizes and normalizes a URL, correcting malformed schemes, cleaning
    hostnames (including optional userinfo), collapsing redundant slashes,
    and stripping unusual surrounding quotes. The function attempts to
    preserve legitimate URL structure while removing invalid characters,
    default ports, and malformed patterns.

    Behavior summary:
    - Strips whitespace and various quote-like characters around the URL.
    - Fixes common scheme typos (e.g., htpps, http:, http//) and normalizes
      them to http:// or https:// when possible.
    - Cleans the hostname and optional username:password@userinfo, removing
      invalid characters and discarding invalid or default ports.
    - Normalizes the path by collapsing repeated slashes, except inside
      embedded full URLs.
    - Rebuilds the final URL using urlsplit/urlunsplit and strips fragments.
    - Returns an empty string if the input is missing or not a string.

    Parameters:
        url (str): The raw URL to sanitize.
        skip_log_tags (list[str], optional): Tags that may disable
            specific logging or post-processing steps in the caller.
            Not used directly in this function but preserved for
            compatibility with the caller's logging pipeline.

    Returns:
        str: A sanitized and normalized URL. If parsing or normalization
        fails at any point, a best-effort cleaned URL is returned.
    """            
    if skip_log_tags is None:
        skip_log_tags = set()
        

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

    if not url or not isinstance(url, str):
        return ""

    url = url.strip()
    special_quote_pairs = [
        (r'^"(.*)"$', r'\1'),
        (r"^'(.*)'$", r'\1'),
        (r'^\u201C(.*)\u201D$', r'\1'),
        (r'^\u2018(.*)\u2019$', r'\1'),
        (r'^"(.*)″$', r'\1'),
    ]
    for quote_pattern, replacement in special_quote_pairs:
        cleaned = re.sub(quote_pattern, replacement, url)
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
    for scheme_pattern, replacement in scheme_fixes:
        fixed = re.sub(scheme_pattern, replacement, url)
        url = fixed

    cleaned = re.sub(r'^[a-zA-Z."(´]https://', 'https://', url)
    url = cleaned
    cleaned = re.sub(r'^[a-zA-Z."(´]http://', 'http://', url)
    url = cleaned

    url = re.sub(r'^(https?:)/+', r'\1//', url)
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
                url = rebuilt
        else:
            path = re.sub(r'/{2,}', '/', parsed.path)
            rebuilt = urlunsplit(
                    (scheme,
                     netloc,
                     path,
                     parsed.query,
                     parsed.fragment))
            url = rebuilt
    except Exception: # pylint: disable=broad-exception-caught
        fallback = re.sub(r'(https?://[^/]+)/{2,}', r'\1/', url)
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
        return normalized.strip()
    except Exception: # pylint: disable=broad-exception-caught
        return url.strip()


def function_for_content_type(regexp_list):
    """
    Decorator factory that registers a function to handle specific content types.

    This factory receives a list of regular expression patterns and returns a
    decorator. When applied to a function, the decorator compiles each pattern
    and stores it along with the function in the global `content_type_functions`
    list. Later, when processing URLs or resources, the crawler can select the
    appropriate function based on the detected content type that matches one of
    the registered regular expressions.

    Parameters
    ----------
    regexp_list : list[str]
        A list of regular expression strings. Each expression represents a
        content-type pattern that should be associated with the decorated
        function.

    Returns
    -------
    Callable
        A decorator that registers the decorated function as a handler for all
        given content-type patterns.

    Notes
    -----
    - The decorated function is not modified; it is simply registered.
    - Regexes are compiled using case-insensitive (`re.I`) and unicode (`re.U`)
      flags.
    - A global list named `content_type_functions` must exist and be writable.
    """    
    def get_content_type_function(f):
        for regexp in regexp_list:
            content_type_functions.append((re.compile(regexp, flags=re.I | re.U), f))
        return f
    return get_content_type_function


async def get_links_page(page, base_url: str) -> list[str]:
    """
    Extract all link-like URLs from a Playwright-rendered web page.

    This function evaluates JavaScript within the browser context to collect
    URLs from common HTML elements that reference external resources,
    including:

    - ``<a href="...">``
    - ``<link href="...">``
    - ``<script src="...">``
    - ``<img src="...">``

    Extraction is performed through a sandboxed helper coroutine
    (``safe_extract``), which safely queries the DOM and returns only valid
    string attributes. Any failures (DOM exceptions, JS errors, selector
    failures, etc.) are caught and logged without interrupting the crawl.

    Parameters
    ----------
    page : playwright.async_api.Page
        A Playwright Page instance from which link references will be collected.

    base_url : str
        The URL of the page being processed, used only for logging context in
        case of extraction errors.

    Returns
    -------
    list[str]
        A list of raw URLs extracted from the page. These URLs are *not*
        normalized, resolved, or validated — the caller is responsible for
        applying URL joining, filtering, deduplication, or domain checks.

    Notes
    -----
    - The function returns all discovered URLs as-is, which may include
      relative paths, absolute links, JavaScript URLs, or malformed values.
    - The internal ``safe_extract`` helper isolates selector-specific failures
      to prevent one broken tag type from affecting others.
    - Output order is not guaranteed due to the internal use of ``set`` for
      deduplication.
    - Designed to be fast, tolerant, and safe for large-scale crawling.
    """    
    links = set()
    async def safe_extract(selector: str, attr: str, tag_name: str):
        try:
            values = await page.evaluate(f"""
                () => Array.from(document.querySelectorAll('{selector}'))
                          .map(e => e['{attr}'])
            """)
            return [v for v in values if isinstance(v, str)]
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"[WARN] Could not extract <{tag_name}> from {base_url}: {e}")
            return []
    links.update(await safe_extract("a[href]", "href", "a"))
    links.update(await safe_extract("link[href]", "href", "link"))
    links.update(await safe_extract("script[src]", "src", "script"))
    links.update(await safe_extract("img[src]", "src", "img"))
    return list(links)


def get_words(text: bytes | str) -> list[str]:
    """
    Extract a list of high-value words from a text or byte sequence.

    This helper function normalizes input by accepting either ``str`` or
    ``bytes``. Byte content is decoded as UTF-8 with replacement for invalid
    sequences to ensure robustness when dealing with crawled data. The final
    decoded string is passed to ``extract_top_words_from_text`` for token
    extraction, filtering, and ranking.

    Parameters
    ----------
    text : bytes or str
        The textual content to process. May be a raw byte sequence or a
        Unicode string. Empty values are handled gracefully.

    Returns
    -------
    list[str]
        A list of extracted top words. Returns an empty list when the input is
        empty, cannot be decoded, or contains no meaningful text.

    Notes
    -----
    - The UTF-8 decoding path is tolerant of malformed byte sequences
      (invalid data is replaced rather than raising an error).
    - The quality and format of the output words depend on
      ``extract_top_words_from_text``, which typically applies stopword
      removal, tokenization, and frequency-based ranking.
    - Designed to be used across multiple content-type handlers
      (HTML, text files, PDFs, etc.) to maintain consistency in word
      extraction.
    """    
    if not text:
        return []
    if isinstance(text, bytes):
        try:
            text = text.decode('utf-8', errors='replace')
        except Exception: # pylint: disable=broad-exception-caught
            return []
    return extract_top_words_from_text(text)

async def get_words_from_page(page) -> list[str]:
    """
    Extract visible text content from a Playwright page and return a list of
    high-value words.

    This function executes a JavaScript snippet inside the browser context to
    recursively walk the DOM, collecting visible text nodes while ignoring
    non-content elements such as ``<script>``, ``<style>``, ``<noscript>``,
    and ``<iframe>``. The extracted raw text is then combined and passed to
    ``extract_top_words_from_text`` to produce a refined list of relevant
    tokens.

    Parameters
    ----------
    page : playwright.async_api.Page
        A Playwright page instance from which text content will be extracted.

    Returns
    -------
    list[str]
        A list of top words extracted from the page’s visible text content.
        Returns an empty list if evaluation fails or no text is found.

    Notes
    -----
    - The DOM traversal is executed entirely inside the sandboxed browser
      environment using ``page.evaluate``.
    - All errors inside the page’s JS context or during evaluation are caught
      and logged; the function always fails safely.
    - The output word list depends on the implementation of
      ``extract_top_words_from_text`` (e.g., tokenization, stopword removal,
      frequency ranking).
    - Hidden, overlayed, or CSS-invisible text may still be captured if present
      in the DOM; this function focuses on structural filtering, not layout
      visibility.

    """    
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
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"[WARN] get_words_from_page failed: {e}")
        text_parts = []

    combined_text = " ".join(text_parts)
    return extract_top_words_from_text(combined_text)


@function_for_content_type(content_type_all_others_regex)
async def content_type_ignore(args):
    """
    Fallback processor for any Content-Type not handled by specialized
    functions.

    This function is registered using the ``@function_for_content_type``  
    decorator and serves as the default handler for all MIME types that do not
    match any of the more specific content-type regex groups. It does not
    attempt to parse, extract, or download the content; instead, it simply
    records that the resource was visited and ignored.

    Parameters
    ----------
    args : dict
        A dictionary containing metadata about the resource being processed.
        Expected keys include:
        - ``url``: The URL being processed.
        - ``content_type``: The detected MIME type.
        - ``parent_host``: The host where the URL was discovered.
        Additional keys may be included depending on upstream routing.

    Returns
    -------
    dict
        A minimal structured dictionary containing:
        - ``url``: The processed URL.
        - ``content_type``: MIME type.
        - ``visited``: Always True.
        - ``source``: Tag identifying this fallback processing path.
        - ``parent_host``: Host where the URL originated.

    Notes
    -----
    - This is the final catch-all content-type handler.
    - No content inspection, HTML parsing, or file operations are performed.
    - Useful for logging, indexing, or debugging unknown or unsupported MIME types.
    """    
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
    """
    Process a URL whose Content-Type indicates plain text.

    This function is registered via the ``@function_for_content_type`` decorator
    and is executed whenever the crawler encounters a resource matching
    ``content_type_plain_text_regex`` (e.g., text/plain). It optionally extracts
    words from the raw text content and returns a structured metadata record
    describing the processed resource.

    Parameters
    ----------
    args : dict
        A dictionary containing metadata about the resource being processed.
        Expected fields include:
        - ``url``: The text file URL.
        - ``content``: The raw text payload (bytes or string depending on upstream).
        - ``content_type``: MIME type of the resource.
        - ``parent_host``: Source host where the URL was discovered.

    Returns
    -------
    dict
        A dictionary keyed by the URL, containing:
        - ``url``: The processed URL.
        - ``content_type``: Detected MIME type.
        - ``visited``: Marked as True.
        - ``isopendir``: Always False for plain-text resources.
        - ``words``: Extracted word list (empty string if extraction disabled).
        - ``source``: A tag identifying this processing pathway.
        - ``parent_host``: The URL's originating host.

    Notes
    -----
    - Word extraction is controlled by the global ``EXTRACT_WORDS`` flag.
    - Parsing is intentionally minimal since plain text requires no HTML parsing
      or binary inspection.
    """    
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

# pylint: disable= too-many-locals, too-many-positional-arguments, too-many-arguments
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
        except Exception: # pylint: disable=broad-exception-caught
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

    except Exception as e: # pylint: disable=broad-exception-caught
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
    """
    Process a URL whose Content-Type indicates a font resource.

    This function is registered via the ``@function_for_content_type`` decorator
    and is triggered whenever the crawler encounters a URL whose MIME type
    matches any of the patterns in ``content_type_font_regex``. The actual work
    is delegated to ``handle_content_type``, which handles downloading and
    organizing font files for storage or indexing.

    Parameters
    ----------
    args : dict
        A dictionary containing metadata about the resource being processed.
        Expected keys include:
        - ``url``: The target font URL.
        - ``content_type``: The detected MIME type.
        - ``parent_host``: The host where the URL was discovered.
        Additional keys may exist depending on upstream processing.

    Returns
    -------
    dict
        A structured dictionary produced by ``handle_content_type`` containing
        metadata about the processed font file, suitable for Elasticsearch or
        downstream ingestion.

    Notes
    -----
    - ``handle_content_type`` is responsible for:
        * Checking whether font downloads are enabled via ``DOWNLOAD_FONTS``.
        * Saving the file into ``FONTS_FOLDER``.
        * Enforcing crawler size and safety rules.
        * Producing a consistent metadata entry.
    - This function is async because downloading and file I/O are performed
      inside ``handle_content_type``.

    """
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
    """
    Process a URL whose Content-Type indicates a video resource.

    This function is automatically registered via the
    ``@function_for_content_type`` decorator and will be invoked whenever the
    crawler encounters a URL whose MIME type matches any pattern in
    ``content_type_video_regex``. The actual processing logic is delegated to
    ``handle_content_type``, which manages downloading, validating, and storing
    video files in a structured manner.

    Parameters
    ----------
    args : dict
        A dictionary containing information about the resource being processed.
        Expected keys include:
        - ``url``: The target video URL.
        - ``content_type``: The detected MIME type.
        - ``parent_host``: The host where the URL was discovered.
        Additional keys may be included depending on upstream pipeline context.

    Returns
    -------
    dict
        A metadata dictionary generated by ``handle_content_type`` describing
        the processed video file, formatted for storage (e.g. Elasticsearch).

    Notes
    -----
    - ``handle_content_type`` performs:
        * Checking whether video downloads are enabled via ``DOWNLOAD_VIDEOS``.
        * Saving the file into ``VIDEOS_FOLDER``.
        * Applying crawler limits such as max file size.
        * Creating a consistent metadata structure for downstream indexing.
    - This function is asynchronous because downloading and I/O inside
      ``handle_content_type`` require awaiting.

    """    
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
    """
    Process a URL whose Content-Type matches one of the audio MIME patterns.

    This function is registered via the ``@function_for_content_type`` decorator,
    allowing it to be automatically selected whenever a fetched resource matches
    any regex in ``content_type_audio_regex``. The heavy lifting is delegated to
    ``handle_content_type``, which manages downloading, validating, and storing
    audio files.

    Parameters
    ----------
    args : dict
        A dictionary containing contextual information about the URL being
        processed. Typical keys include:
        - ``url``: The resource URL.
        - ``content_type``: The detected MIME type for the resource.
        - ``parent_host``: The host where the URL was found.
        - ``content`` or data fields depending on the pipeline stage.

    Returns
    -------
    dict
        A metadata dictionary generated by ``handle_content_type`` describing the
        processed audio file, formatted for Elasticsearch ingestion.

    Notes
    -----
    - ``handle_content_type`` handles:
        * Validating whether audio downloads are permitted via ``DOWNLOAD_AUDIOS``.
        * Saving the file to ``AUDIOS_FOLDER``.
        * Enforcing size limits and crawler policies.
        * Creating a structured metadata entry.
    - This function is asynchronous because audio downloading and I/O operations
      inside ``handle_content_type`` may require awaiting.
    """    
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
    """
    Process a URL whose Content-Type matches one of the PDF MIME patterns.

    This function is registered through the ``@function_for_content_type``
    decorator so it is automatically selected when a fetched resource matches
    any regex in ``content_type_pdf_regex``. It delegates the actual processing
    to ``handle_content_type``, which manages downloading, storing, and
    recording metadata for PDF files.

    Parameters
    ----------
    args : dict
        A dictionary containing all metadata and crawling context for the URL.
        Expected keys typically include:
        - ``url``: The resource URL.
        - ``content_type``: The detected MIME type.
        - ``parent_host``: The host from which this URL was discovered.
        - ``content`` or ``response`` fields depending on pipeline stage.

    Returns
    -------
    dict
        A structured dictionary representing the processed PDF entry to be
        stored in Elasticsearch. The exact structure is produced by
        ``handle_content_type``.

    Notes
    -----
    - ``handle_content_type`` is responsible for:
        * Validating whether downloads are enabled via ``DOWNLOAD_PDFS``.
        * Writing the PDF to ``PDFS_FOLDER``.
        * Enforcing file size limits and other crawler policies.
        * Returning a metadata dictionary describing the PDF resource.
    - This function is asynchronous because file downloads and I/O operations
      may be awaited within ``handle_content_type``.
    """    
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
    """
    Handle resources identified as document files based on their MIME type.

    This function is registered by the ``@function_for_content_type`` decorator
    and is invoked whenever a URL's MIME type matches ``content_type_doc_regex``.
    All core logic—including file saving, metadata generation, and conditional
    downloading—is delegated to ``handle_content_type`` to maintain a uniform
    structure across all MIME-type handlers.

    Parameters
    ----------
    args : dict
        A dictionary containing metadata for the resource being processed.
        Expected keys include:
        - ``url``: The URL being handled.
        - ``content``: Raw HTTP response body.
        - ``content_type``: The MIME type reported by the server.
        - ``parent_host``: The referring host from which the URL was found.
        - Other internal fields used by ``handle_content_type``.

    Returns
    -------
    dict
        A standardized dictionary keyed by the URL, populated by
        ``handle_content_type``. Fields typically include:
        - ``url``: The processed URL.
        - ``visited``: ``True``.
        - ``source``: Identifier for this handler (``"doc"``).
        - ``parent_host``: The originating host.
        - File metadata if downloading is enabled.

    Notes
    -----
    - Downloads are controlled by the ``DOWNLOAD_DOCS`` configuration flag.
    - Files are stored inside the ``DOCS_FOLDER`` directory.
    - This function serves as a lightweight wrapper to keep all content-type
      handlers simple, declarative, and easy to maintain.
    """    
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
    """
    Handle resources identified as database files based on their MIME type.

    This function is automatically registered by the
    ``@function_for_content_type`` decorator as the handler for any URL whose
    MIME type matches ``content_type_database_regex``. The actual processing,
    saving, and metadata generation logic is delegated to ``handle_content_type``,
    ensuring consistency across all content-type handlers.

    Parameters
    ----------
    args : dict
        Dictionary with metadata about the resource being processed. Expected
        keys include:
        - ``url``: The target URL.
        - ``content``: Raw HTTP response body.
        - ``content_type``: Reported MIME type.
        - ``parent_host``: Host from which the URL was discovered.
        - Other crawler-internal fields used by ``handle_content_type``.

    Returns
    -------
    dict
        A structured dict keyed by the URL, containing standardized fields
        returned by ``handle_content_type`` such as:
        - ``url``: The processed URL.
        - ``visited``: ``True``.
        - ``source``: Identifier for this handler (``"database"``).
        - ``parent_host``: The referring domain.
        - File information if downloads are enabled.

    Notes
    -----
    - Whether database files are downloaded is controlled by
      ``DOWNLOAD_DATABASES``.
    - Stored files are written into ``DATABASES_FOLDER``.
    - This wrapper exists mainly to keep handler definitions clean,
      declarative, and uniform across MIME types.
    """    
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
    """
    Handle resources identified as BitTorrent files based on MIME type.

    This function is automatically registered by the
    ``@function_for_content_type`` decorator, making it the designated handler
    for any URL whose detected MIME type matches ``content_type_torrent_regex``.
    All download logic, validation, folder selection, and metadata formatting
    are delegated to ``handle_content_type`` for consistency across handlers.

    Parameters
    ----------
    args : dict
        Dictionary containing crawler metadata for the resource being processed.
        Expected keys include:
        - ``url``: The target URL.
        - ``content``: Raw HTTP response content.
        - ``content_type``: Server-reported MIME type.
        - ``parent_host``: Referrer host initiating the request.
        - Additional internal fields used by ``handle_content_type``.

    Returns
    -------
    dict
        A normalized structure keyed by URL, containing metadata produced by
        ``handle_content_type`` such as:
        - ``url``: The processed URL.
        - ``visited``: ``True``.
        - ``source``: Identifier for this handler (``"torrent"``).
        - ``parent_host``: The domain that linked to the file.
        - File information if downloading is enabled.

    Notes
    -----
    - Downloads are controlled by the ``DOWNLOAD_TORRENTS`` configuration flag.
    - Saved files are stored under ``TORRENTS_FOLDER``.
    - This function exists primarily as a lightweight wrapper to keep MIME-type
      handlers uniform and maintainable.
    """    
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
    """
    Handle resources identified as comic or manga files based on MIME type.

    This function is registered through the ``@function_for_content_type``
    decorator, making it the designated handler for any URL whose detected
    content type matches one of the patterns in ``content_type_comic_regex``.
    The function itself simply forwards all processing responsibilities to
    ``handle_content_type``, which manages downloading, validation, folder
    placement, and metadata assembly.

    Parameters
    ----------
    args : dict
        Dictionary containing crawler context and metadata for the resource
        being processed. Expected fields include:
        - ``url``: The target URL.
        - ``content``: Raw HTTP response content.
        - ``content_type``: Detected MIME type.
        - ``parent_host``: Host that referred to this URL.
        - Any additional values required by ``handle_content_type``.

    Returns
    -------
    dict
        A normalized result dictionary keyed by URL. The associated value
        contains metadata such as:
        - ``url``: The processed URL
        - ``visited``: ``True`` indicating successful handling
        - ``source``: Identifier for this handler (``"comic"``)
        - ``parent_host``: Referrer host
        - Additional fields produced by ``handle_content_type`` (e.g. file path)

    Notes
    -----
    - Output files are stored inside the folder specified by ``COMICS_FOLDER``.
    - Processing is enabled or disabled by the ``DOWNLOAD_COMICS`` configuration
      flag.
    - This wrapper exists to keep MIME-type-specific handlers organized and
      consistent across the crawler codebase.
    """    
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
    """
    Process and store resources identified as compressed or archive files.

    This function is automatically registered through the
    ``@function_for_content_type`` decorator, making it the handler for any URL
    whose MIME type matches one of the patterns in ``content_type_compressed_regex``.
    It delegates all processing steps to ``handle_content_type``, which manages
    downloading, validation, folder placement, and metadata assembly.

    Parameters
    ----------
    args : dict
        A dictionary carrying crawler context and response information for the
        URL being processed. Expected keys include:
        - ``url``: The resource URL.
        - ``content``: Raw HTTP response content.
        - ``content_type``: The detected MIME type for the resource.
        - ``parent_host``: Hostname that referred to this URL.
        - Any other metadata required by ``handle_content_type``.

    Returns
    -------
    dict
        A standardized result dictionary where the key is the processed URL and
        the value contains fields such as:
        - ``url``: The original URL
        - ``visited``: ``True`` to indicate successful handling
        - ``source``: Identifier for this handler (``"compressed"``)
        - ``parent_host``: The referrer host
        - Additional fields filled by ``handle_content_type`` (e.g., file paths)

    Notes
    -----
    - Output files are stored inside ``COMPRESSEDS_FOLDER``.
    - Processing logic is controlled by the ``DOWNLOAD_COMPRESSEDS`` configuration
      flag.
    - This function is a thin wrapper to maintain clean MIME-type-specific
      organization for all crawler content handlers.
    """    
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
    """
    Process and store resources identified as MIDI files based on their content type.

    This function is registered dynamically via the ``@function_for_content_type``
    decorator, which activates it whenever a URL's MIME type matches one of the
    patterns in ``content_type_midi_regex``. It delegates the actual processing
    to ``handle_content_type``, which performs downloading, validation, and
    metadata extraction.

    Parameters
    ----------
    args : dict
        A dictionary containing metadata and crawler context for the URL being
        processed. Expected keys include:
        - ``url``: The target URL.
        - ``content``: Raw response bytes or text.
        - ``content_type``: The detected MIME type.
        - ``parent_host``: Hostname that linked to this URL.
        - Any other fields required by ``handle_content_type``.

    Returns
    -------
    dict
        A mapping containing processed output for this URL, in the standardized
        format used across the crawler. The dictionary's key is the URL itself,
        and the value contains metadata such as:
        - ``url``: Original URL
        - ``visited``: ``True`` once processed
        - ``source``: Processing source label (``"midi"``)
        - ``parent_host``: The referring host
        - Any fields added by ``handle_content_type`` (e.g., file paths)

    Notes
    -----
    - Files are stored under ``MIDIS_FOLDER``.
    - Processing and downloading behavior depends on the global ``DOWNLOAD_MIDIS``
      flag.
    - This function exists primarily as a thin wrapper to keep MIME-specific
      logic clean, isolated, and consistent across all content handlers.
    """    
    return await handle_content_type(
        args,
        DOWNLOAD_MIDIS,
        content_type_midi_regex,
        MIDIS_FOLDER,
        "midi",
        "midi",
    )


def is_octetstream(content_type: str) -> bool:
    """
    Check whether a content type matches any known octet-stream patterns.

    This function iterates through the global ``content_type_octetstream`` list,
    comparing each regular expression against the provided ``content_type``.
    If any pattern matches (case-insensitive), the function considers the
    content type to represent a generic binary/octet-stream resource.

    Parameters
    ----------
    content_type : str
        The MIME content type string to evaluate.

    Returns
    -------
    bool
        ``True`` if the content type matches any octet-stream regex pattern,
        ``False`` otherwise.

    Notes
    -----
    Octet-stream content types often represent arbitrary binary data and
    typically indicate that the crawler should treat the resource as a file
    rather than HTML or other structured formats.
    """    
    for octet_pattern in content_type_octetstream:
        if re.match(octet_pattern, content_type, re.IGNORECASE):
            return True
    return False


@function_for_content_type(content_type_html_regex)
async def content_type_download(args):
    """
    Process HTML content for a downloaded URL and extract structured web content.

    This function is automatically registered for handling responses whose
    content type matches ``content_type_html_regex``. It receives the parsed
    arguments for a URL (including its HTML content) and attempts to parse
    the HTML with BeautifulSoup. On success, it extracts optional metadata such as:

    - ``words``: Text tokens extracted from the HTML body  
    - ``raw_webcontent``: A truncated raw HTML string  
    - ``min_webcontent``: A minimal cleaned summary extracted from the page  
    - ``isopendir`` and ``opendir_pattern``: Whether the content resembles an open directory listing

    If HTML parsing fails, the function returns a fallback structure marking the
    URL as visited while storing minimal metadata.  
    Extraction of fields depends on global flags:
    ``EXTRACT_WORDS``, ``EXTRACT_RAW_WEBCONTENT``, and ``EXTRACT_MIN_WEBCONTENT``.

    Parameters
    ----------
    args : dict
        A dictionary containing:
        - ``url``: The URL being processed  
        - ``content``: The raw HTML string  
        - ``content_type``: The detected content type  
        - ``parent_host``: Host from which the URL was discovered  

    Returns
    -------
    dict
        A dictionary with the URL as the key and a metadata dictionary as the value.
        Includes parsing results, extracted content, open directory detection flags,
        and visit status.

    Notes
    -----
    - The returned structure is designed to be directly saved into Elasticsearch.
    - HTML output is truncated to ``MAX_WEBCONTENT_SIZE`` to reduce index size.
    - Errors are caught broadly to ensure crawler stability.
    """    
    try:
        content = args['content']
        soup = BeautifulSoup(content, "html.parser")
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"Soup parsing error for {args['url']}: {e}")
        return { args['url'] :
                {
            "url": args['url'],
            "content_type": args['content_type'],
            "visited": True,
            "words": '',
            "min_webcontent": '',
            "raw_webcontent": '',
            "source": 'content_type_html_regex_soup_exception',
            "parent_host": args['parent_host'] }
        }
        
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


async def process_input_url_files(db):
    """
    Process queued URL files from the input directory and crawl their contents.

    This function continuously checks the configured input folder (`INPUT_FOLDER`)
    for text files containing URLs. For each available file, a random one is
    selected, safely read (handling Unicode errors when necessary), and up to
    `MAX_URLS_FROM_FILE` URLs are extracted and crawled using Playwright.

    After crawling:
      - The file is rewritten with any remaining unprocessed lines, or
      - Removed entirely if fully processed or empty.

    This mechanism allows an external process (or manual workflow) to feed URLs
    into the crawler in batches without blocking the main crawling loop.

    Parameters:
        db:
            A database or Elasticsearch client instance, passed to `get_page()`
            and used during page-processing operations.

    Workflow:
        1. Validate that the input folder exists; otherwise, exit immediately.
        2. Loop until no files remain in the folder.
        3. Select a random file to distribute load across file sources.
        4. Attempt UTF-8 decoding; if decoding fails, fall back to a safe
           line-by-line binary read with replacement of invalid characters.
        5. Process the first `MAX_URLS_FROM_FILE` URLs using Playwright.
        6. Rewrite the file with leftover URLs or delete it if fully consumed.

    Notes:
        - Invalid URLs or crawling errors are caught and logged without stopping
          the processing loop.
        - Empty files are automatically deleted to avoid reprocessing.
        - The function is asynchronous because Playwright operations require
          `async` context management (`async with async_playwright()`).

    Returns:
        None
            The function operates for its side effects: crawling URLs and
            updating or deleting input files.
    """    
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
                    except UnicodeDecodeError as ie:
                        print(f"Problem in line {i}: {ie} -> replacing bad chars")
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
                print(f"    [FILE] {url}")
                async with async_playwright() as playwright:
                    await get_page(url, playwright, db)
            except Exception as e: # pylint: disable=broad-exception-caught
                print(f"Error crawling {url}: {e}")

        # Rewrite file with remaining lines
        if remaining:
            with open(file_to_process, "w", encoding="utf-8") as f:
                f.writelines(remaining)
        else:
            os.remove(file_to_process)
            print(f"File fully processed and removed: {file_to_process}")


# pylint: disable=too-many-locals,too-many-statements,too-many-positional-arguments,too-many-arguments
def cleanup_elasticsearch_indexes(
    db,
    remove_repeated_segments=False,
    remove_empty_ctype=False,
    remove_blocked_hosts=False,
    remove_blocked_urls=False,
    remove_invalid_urls=False,
    batch_size=2000
    ):
    """
    Unified cleanup for Elasticsearch indexes.

    Iterates through all documents (using search_after for scalability)
    and applies multiple cleanup rules in one pass.

    Args:
        db: Elasticsearch wrapper (must provide `es` client).
        remove_repeated_segments (bool): Remove URLs with repeated path segments.
        remove_empty_ctype (bool): Remove docs missing or empty content_type.
        remove_blocked_hosts (bool): Remove docs from hosts matching blocklist.
        remove_blocked_urls (bool): Remove docs whose paths match blocklist.
        remove_invalid_urls (bool): Remove malformed or sanitized URLs.
        batch_size (int): Number of docs to fetch per request.

    Returns:
        dict: Summary with counts per cleanup rule.
    """
    es = db.es

    # --- Early exit optimization ---
    if not any([
        remove_repeated_segments,
        remove_empty_ctype,
        remove_blocked_hosts,
        remove_blocked_urls,
        remove_invalid_urls,
    ]):
        print("No cleanup flags enabled. Skipping Elasticsearch scan.")
        return {
            "repeated_segments": 0,
            "empty_ctype": 0,
            "blocked_hosts": 0,
            "blocked_urls": 0,
            "invalid_urls": 0,
        }

    # --- Precompile regexes for blocklist rules ---
    host_blocklist = [re.compile(p) for p in HOST_REGEX_BLOCK_LIST] if remove_blocked_hosts else []
    url_blocklist = [re.compile(p) for p in URL_REGEX_BLOCK_LIST] if remove_blocked_urls else []

    def is_blocked_host(host):
        return any(r.search(host) for r in host_blocklist)

    def is_blocked_path(path):
        return any(r.search(path) for r in url_blocklist)

    cleanup_stats = {
        "repeated_segments": 0,
        "empty_ctype": 0,
        "blocked_hosts": 0,
        "blocked_urls": 0,
        "invalid_urls": 0,
    }

    # pylint: disable=too-many-branches,too-many-locals
    def process_index(index_pattern: str, label: str):
        nonlocal cleanup_stats
        deleted = 0
        processed = 0
        search_after = None

        print(f"Cleaning index: {label}")

        base_query = {
            "size": batch_size,
            "query": {"match_all": {}},
            "_source": ["url", "host", "content_type", "visited"],
            "sort": [
                {"created_at": "asc"},
                {"url.keyword": "asc"}
            ]
        }

        while True:
            if search_after:
                base_query["search_after"] = search_after

            try:
                res = es.search(index=index_pattern, body=base_query)
            except Exception as e: # pylint: disable=broad-exception-caught
                print(f"[{label}] Search error: {e}")
                break

            hits = res.get("hits", {}).get("hits", [])
            if not hits:
                break

            ids_to_delete = []
            for doc in hits:
                src = doc["_source"]
                url = src.get("url")
                if not url:
                    continue

                host = src.get("host") or urlsplit(url).hostname or ""
                path = urlsplit(url).path or ""
                ctype = src.get("content_type")
                visited = src.get("visited", False)

                # --- Apply cleanup rules ---
                if remove_repeated_segments and has_repeated_segments(url):
                    cleanup_stats["repeated_segments"] += 1
                    ids_to_delete.append(doc["_id"])
                    continue

                if remove_empty_ctype and (not ctype or ctype == "") and not visited:
                    cleanup_stats["empty_ctype"] += 1
                    ids_to_delete.append(doc["_id"])
                    continue

                if remove_blocked_hosts and is_blocked_host(host):
                    cleanup_stats["blocked_hosts"] += 1
                    ids_to_delete.append(doc["_id"])
                    continue

                if remove_blocked_urls and is_blocked_path(path):
                    cleanup_stats["blocked_urls"] += 1
                    ids_to_delete.append(doc["_id"])
                    continue

                if remove_invalid_urls:
                    parsed = urlparse(url)
                    sanitized = sanitize_url(url)
                    if parsed.scheme == "" or sanitized != url:
                        cleanup_stats["invalid_urls"] += 1
                        ids_to_delete.append(doc["_id"])
                        continue

            # --- Perform batched deletion ---
            if ids_to_delete:
                try:
                    resp = es.delete_by_query(
                        index=index_pattern,
                        body={"query": {"ids": {"values": ids_to_delete}}},
                        slices="auto",
                        conflicts="proceed",
                        refresh=False,
                        wait_for_completion=True
                    )
                    deleted += resp.get("deleted", 0)
                except Exception as e: # pylint: disable=broad-exception-caught
                    print(f"[{label}] Delete error: {e}")

            processed += len(hits)
            search_after = hits[-1]["sort"]

            print(f"  → Processed {processed:,} | Deleted {deleted:,}")

            if len(hits) < batch_size:
                break

        print(f"Done cleaning {label}. Deleted {deleted:,} docs.")
        return deleted

    process_index(f"{LINKS_INDEX}-*", LINKS_INDEX)
    process_index(f"{CONTENT_INDEX}-*", CONTENT_INDEX)

    print("\n Cleanup summary:")
    for k, v in cleanup_stats.items():
        print(f"  {k}: {v}")

    print(f" Total deleted across all rules: {sum(cleanup_stats.values()):,}")
    return cleanup_stats


def get_min_webcontent(soup) -> str:
    """
    Extract a minimal, cleaned text representation from a BeautifulSoup document.

    This function collects all textual nodes from the parsed HTML while excluding
    any tags listed in `soup_tag_blocklist`. It removes extra whitespace, filters
    out empty fragments, and returns a compact string suitable for indexing,
    deduplication, or lightweight content comparison.

    Parameters:
        soup (bs4.BeautifulSoup):
            A BeautifulSoup-parsed HTML document from which text will be extracted.

    Returns:
        str:
            A single string containing the concatenated cleaned text extracted
            from allowed HTML nodes. All consecutive whitespace is normalized to
            single spaces, and content from blocklisted tags is omitted.

    Notes:
        - `soup_tag_blocklist` must be defined in the surrounding scope.
        - Whitespace, newlines, and empty text nodes are removed.
        - Useful for quick similarity checks or storing minimal textual content
          without full HTML noise.
    """    
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
    """
    Process an image response detected by content-type regex and optionally
    download, classify, and store metadata about the image.

    This function supports two major modes:
      - DOWNLOAD_ALL_IMAGES: Save every image encountered.
      - CATEGORIZE_NSFW: Run NSFW/SFW classification on the image
        and optionally save according to category.

    The function extracts image metadata, computes resolution, converts
    formats when necessary (e.g., CMYK → RGB, palette-based images with
    transparency → RGBA), hashes the image content to generate a stable
    filename, and optionally runs NSFW classification using the loaded model.

    Parameters:
        args (dict):
            A dictionary containing information about the fetched resource.
            Expected keys include:
                - 'url':           The URL of the downloaded image.
                - 'raw_content':   Raw binary image data.
                - 'content_type':  The detected MIME type (e.g., image/png).
                - 'parent_host':   Hostname of the parent page.

    Returns:
        dict:
            A dictionary keyed by the original URL, containing metadata
            describing the processed image. Depending on flow, fields may
            include:

            Common fields:
                {
                    "url": <str>,
                    "content_type": <str>,
                    "source": <str>,
                    "isopendir": False,
                    "visited": True,
                    "parent_host": <str>,
                }

            When successfully processed:
                - "filename":    The generated SHA-512 filename for the image.
                - "resolution":  Total pixel count (width * height).
                - "isnsfw":      Float probability (only when NSFW analysis runs).

            Error handlers set the `source` field accordingly:
                - "content_type_images_unidentified_image_error"
                - "content_type_images_decompression_bomb_error"
                - "content_type_images_oserror"

            When no image download or classification happens:
                - "source": "content_type_images_no_download"

    Behavior Summary:
        - Parses and loads the image using Pillow.
        - Normalizes image mode if required (RGB/RGBA).
        - Computes resolution and generates canonical filename via SHA-512 hash.
        - If DOWNLOAD_ALL_IMAGES is enabled:
            Saves the processed image to the configured folder.
        - If CATEGORIZE_NSFW is enabled:
            - Ensures the image meets MIN_NSFW_RES threshold.
            - Feeds image through the NSFW model.
            - Saves into SFW or NSFW folder depending on classification.
            - Adds `isnsfw` probability to output.
        - Always returns a metadata dictionary describing the outcome.

    Notes:
        - Images failing to decode or triggering Pillow safety limits
          are caught gracefully and logged.
        - CMYK and palette-based images are converted for consistent handling.
        - Raw content is never written directly; only normalized PNG output
          is saved.
    """    
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
                _, nsfw_probability = predictions[0]
                if nsfw_probability > NSFW_MIN_PROBABILITY:
                    print(f"porn {nsfw_probability} {args['url']}")
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
            print(f"[WARN] UnidentifiedImageError: {args['url']} — {e}")
            return {args['url']:
                    {
                    "url":args['url'],
                    "content_type":args['content_type'],
                    "source":"content_type_images_unidentified_image_error",
                    "isopendir":False,
                    "visited":True,
                    "parent_host":args['parent_host'],
                    }
                    }
        except Image.DecompressionBombError as e:
            print(f"[WARN] Image.DecompressionBombError: {args['url']} — {e}")
            return {args['url']:
                    {
                    "url":args['url'],
                    "content_type":args['content_type'],
                    "source":"content_type_images_decompression_bomb_error",
                    "isopendir":False,
                    "visited":True,
                    "parent_host":args['parent_host'],
                    }
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
                    }
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
    """
    Generate a list of parent directory URLs derived from the path of a given URL.

    This function progressively removes trailing path segments from the input URL
    and returns each resulting directory level. It is useful for crawlers that need
    to attempt fallback discovery by walking upward through a site's directory
    structure.

    Example:
        Input:
            https://example.com/a/b/c/file.txt

        Output:
            [
                "https://example.com/a/b/c",
                "https://example.com/a/b",
                "https://example.com/a"
            ]

    Parameters:
        url (str):
            The full URL whose directory hierarchy will be computed.

    Returns:
        list[str]:
            A list of directory URLs, ordered from deepest to shallowest.
            Returns an empty list if the URL is invalid or cannot be parsed.

    Notes:
        - Query parameters and fragments are ignored.
        - The host portion (scheme + domain) is preserved at every level.
        - Intended for use in hierarchical crawling, S3-style path exploration,
          or building breadcrumb-like fallback checks.
    """    
    try:
        host = "://".join(urlsplit(url)[:2])
        dtree = []
        parts = PurePosixPath(unquote(urlparse(url).path)).parts

        # Avoid redefining 'iter' (built-in)
        for idx in range(1, len(parts[0:])):
            dtree.append(str(host + '/' + '/'.join(parts[1:-idx])))

        return dtree

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"[WARN] Skipping invalid URL in get_directory_tree(): {url} — {e}")
        return []


def is_url_block_listed(url):
    """
    Check whether a given URL matches any pattern in the URL blocklist.

    This function iterates over all regular expressions defined in
    `URL_REGEX_BLOCK_LIST` and tests them directly against the full URL
    (including its path). If any regex matches, the URL is considered blocked.

    Args:
        url (str): The full URL string to evaluate.

    Returns:
        bool:
            - True if the URL matches at least one pattern in
              `URL_REGEX_BLOCK_LIST`.
            - False otherwise.

    Notes:
        - Matching is case-insensitive and Unicode-aware (`re.I | re.U`).
        - Intended for filtering out undesired or dangerous paths such as
          admin URLs, tracking URLs, infinite loops, or known problematic
          structures.
    """    
    for regex in URL_REGEX_BLOCK_LIST:
        if re.search(regex, url, flags=re.I | re.U):
            return True
    return False

def is_host_allow_listed(url):
    """
    Check whether a given URL's host matches any pattern in the host allow-list.

    This function tests the provided URL against each regular expression defined
    in `HOST_REGEX_ALLOW_LIST`. If any regex matches, the host is considered
    explicitly allowed and should bypass normal blocking rules.

    Args:
        url (str): The full URL or hostname to evaluate.

    Returns:
        bool:
            - True if the URL matches at least one pattern in
              `HOST_REGEX_ALLOW_LIST`.
            - False otherwise.

    Notes:
        - Matching is case-insensitive and Unicode-aware (`re.I | re.U`).
        - Use this to define exceptions to block-list filters or to give
          priority crawling access to certain trusted domains.
    """    
    for regex in HOST_REGEX_ALLOW_LIST:
        if re.search(regex, url, flags=re.I | re.U):
            return True
    return False

def is_host_block_listed(url):
    """
    Check whether a given URL's host matches any pattern in the host blocklist.

    This function iterates over all regular expressions defined in
    `HOST_REGEX_BLOCK_LIST` and tests them against the provided URL.  
    If any regex matches, the host is considered blocked.

    Args:
        url (str): The full URL or hostname to check.

    Returns:
        bool: 
            - True if the URL matches any pattern in `HOST_REGEX_BLOCK_LIST`.
            - False otherwise.

    Notes:
        - Matching is case-insensitive and Unicode-aware (`re.I | re.U`).
        - Intended for use in cleanup or filtering phases to skip
          undesired or known problematic hosts.
    """    
    for regex in HOST_REGEX_BLOCK_LIST:
        if re.search(regex, url, flags=re.I | re.U):
            return True
    return False

def sanitize_content_type(content_type):
    """
    Cleans and normalizes a raw Content-Type header value.

    This function standardizes Content-Type strings extracted from HTTP responses
    by removing unnecessary prefixes, parameters, whitespace, and quotation marks.
    It ensures the result follows the standard MIME format `type/subtype`, such as 
    `text/html` or `application/pdf`.

    Args:
        content_type (str): 
            The raw Content-Type string (possibly including prefixes, quotes, or 
            parameters, e.g., `"Content-Type: text/html; charset=UTF-8"`).

    Returns:
        str:
            A cleaned MIME type string in the form `type/subtype`, or an empty 
            string if the input is invalid or empty.

    Notes:
        - Removes prefixes like `"Content-Type:"` (case-insensitive).
        - Strips quotes and trims whitespace.
        - Removes optional parameters such as charset or boundary definitions.
        - Intended for use in crawlers, downloaders, and content-type filtering logic.
    """    
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
    """
    Extracts a minimal, text-only representation of a webpage using Playwright.

    This function asynchronously retrieves visible textual content from the page's DOM,
    excluding non-relevant or noisy elements (e.g., <script>, <style>, <noscript>, or
    other tags defined in the global `soup_tag_blocklist`). It ensures the DOM is 
    sufficiently loaded before evaluation and returns a trimmed text version of the 
    document body.

    The JavaScript snippet executed in the browser recursively traverses the DOM to
    collect text nodes, while respecting the defined tag blocklist. The resulting
    content is concatenated into a single string and truncated to `MAX_WEBCONTENT_SIZE`
    characters for memory efficiency.

    Args:
        page (playwright.async_api.Page): 
            A Playwright page instance representing the loaded webpage.

    Returns:
        str: 
            A cleaned and size-limited string containing the visible textual content 
            of the page. Returns an empty string if the load state could not be reached 
            or if extraction fails.

    Notes:
        - The function waits for the `"domcontentloaded"` state (up to 10 seconds) 
          before attempting to extract content.
        - Uses global variables:
            - `soup_tag_blocklist`: A set of HTML tag names to skip during extraction.
            - `MAX_WEBCONTENT_SIZE`: Maximum allowed content length (in characters).
            - `DEBUG_PW`: Enables Playwright debugging logs when True.
        - Broad exceptions are intentionally caught to ensure robustness during 
          large-scale crawling or scraping operations.

    """    
    try:
        # Try waiting a little, but don't block forever
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception as e: # pylint: disable=broad-exception-caught
            if DEBUG_PW:
                print(f"[WARN] Load state not reached for {page.url}: {e}")
                return ""

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

    except Exception as e: # pylint: disable=broad-exception-caught
        if DEBUG_PW:
            print(f"[WARN] Failed extracting minimal webcontent from {page.url}: {e}")
            return ""


def is_open_directory(content, content_url):
    """
    Detects whether a given HTML content corresponds to an open directory listing.

    This function analyzes the HTML body of a fetched web page and attempts to identify 
    patterns that are characteristic of open or automatically generated directory listings.
    It supports detection of multiple common styles such as Apache, Nginx, IIS, Lighttpd, 
    h5ai, DUFS, Directory Lister, and other popular file listing scripts.

    The detection is performed by scanning the content for known HTML or metadata patterns 
    (e.g., titles like "Index of /", "Directory Listing", parent directory links, or 
    generator meta tags). If any of the predefined patterns match, the function reports 
    the page as an open directory.

    Args:
        content (str): The full HTML content of the web page to analyze.
        content_url (str): The URL of the page being checked. Used to extract host 
            information for host-specific pattern matching.

    Returns:
        tuple[bool, str]:
            - A boolean indicating whether the page appears to be an open directory.
            - The regex pattern that matched (or an empty string if no match was found).
    
    Notes:
        - Matching is case-insensitive.
        - The function can detect a variety of directory listing frameworks, 
          including Apache, Lighttpd, IIS, h5ai, DUFS, and pCloud, among others.
        - If no known pattern matches, the function safely returns `(False, "")`.
    """    
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
    """
    Extracts the most frequent words from a given text based on configurable filters.

    The function performs optional normalization steps such as removing special characters
    and converting text to lowercase, then filters words by length and frequency. 
    It finally returns the top `WORDS_MAX_WORDS` most common words.

    Global configuration variables used:
        WORDS_REMOVE_SPECIAL_CHARS (bool): If True, removes punctuation and special characters.
        WORDS_TO_LOWER (bool): If True, converts text to lowercase.
        WORDS_MIN_LEN (int): Minimum allowed word length.
        WORDS_MAX_LEN (int): Maximum allowed word length.
        WORDS_MAX_WORDS (int): Maximum number of top words to return.

    Args:
        text (str): The input text from which to extract words.

    Returns:
        list[str]: A list of the most common words that satisfy the configured filters,
        ordered by descending frequency.
    """   
    if WORDS_REMOVE_SPECIAL_CHARS:
        text = re.sub(r'[^\w\s]', ' ', text, flags=re.UNICODE)
    if WORDS_TO_LOWER:
        text = text.lower()

    words = [word for word in text.split() if
             WORDS_MIN_LEN < len(word) <= WORDS_MAX_LEN]
    most_common = Counter(words).most_common(WORDS_MAX_WORDS)
    return [word for word, _ in most_common]

def get_instance_number():
    """
    Determines the unique instance number for the current crawler process.

    This function uses file-based locking under `/tmp/instance_flags/` to assign
    each running crawler instance a distinct numeric identifier (from 1 to 99).
    It attempts to acquire an exclusive, non-blocking file lock for each numbered
    `.lock` file in ascending order. The first lock that can be acquired determines
    the instance number.

    This allows multiple crawler processes running on the same host to coordinate
    without interfering with one another — each one gets a unique, stable instance
    ID until it terminates (releasing its lock).

    Lock files are automatically created if missing and persist under `/tmp` only
    for the lifetime of the process.

    Returns:
        int: The assigned instance number (1–99).  
             Returns `999` if no available lock could be acquired or if an error occurs.

    Notes:
        - Uses `fcntl.flock()` for interprocess locking, which is only available on Unix-like systems.
        - The global variable `lock_file` must remain open to keep the lock active.
    """    
    global lock_file # pylint: disable=global-variable-undefined
    try:
        os.makedirs("/tmp/instance_flags", exist_ok=True)
        for i in range(1, 100):
            lock_path = f"/tmp/instance_flags/instance_{i}.lock"
            lock_file = open(lock_path, "w")  # pylint: disable=consider-using-with,unspecified-encoding
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return i
            except BlockingIOError:
                lock_file.close()
                continue
    except Exception as e: # pylint: disable=broad-exception-caught
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
        except Exception as e: # pylint: disable=broad-exception-caught
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
    strategy for selecting host domains (e.g., oldest, random, or by prefix).
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
        "oldest": lambda: get_oldest_host_domains(db, size=size),
        "random": lambda: get_random_host_domains(db, size=size),
        "host_prefix": lambda: get_urls_by_random_timestamp_and_prefix(db, size=size)
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

# pylint: disable=too-many-locals
def deduplicate_links_vs_content_es(
    db,
    links_index_pattern=f"{LINKS_INDEX}-*",
    content_index_pattern=f"{CONTENT_INDEX}-*",
    batch_size=10000
):
    """
    Deletes all documents from `links_index_pattern` that already exist in 
    `content_index_pattern` by comparing document IDs (_id field).

    Scroll-free, search_after-based version that adapts automatically
    to very large indices.

    Args:
        db: Elasticsearch database wrapper.
        links_index_pattern (str): Index pattern for links.
        content_index_pattern (str): Index pattern for content.
        batch_size (int): Number of IDs to fetch and delete per iteration.

    Returns:
        int: Total number of deleted documents.
    """
    es = db.es
    print(f" Deduplicating '{links_index_pattern}' against '{content_index_pattern}'...")

    # --- Self-tuning max_docs based on content index size ---
    try:
        content_count = es.count(index=content_index_pattern)["count"]
        max_docs = int(content_count * 1.2)  # +20% buffer
        print(f"[INFO] Content index has {content_count:,} docs → max_docs={max_docs:,}")
    except Exception as e: #pylint: disable=broad-exception-caught
        print(f"[WARN] Could not determine content index size: {e}")
        max_docs = 10_000_000  # fallback
    deleted_total = 0
    processed = 0
    search_after = None


    query = {
        "size": batch_size,
        "query": {"match_all": {}},
        "_source": False,
        "sort": [
            {"created_at": "asc"},       # primary sort
            {"url.keyword": "asc"}      # tie-breaker
        ]
    }

    while processed < max_docs:
        if search_after:
            query["search_after"] = search_after

        try:
            resp = es.search(index=content_index_pattern, body=query)
        except Exception as e: #pylint: disable=broad-exception-caught
            print(f"[deduplicate_links_vs_content_es] Elasticsearch search error: {e}")
            break

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            break

        ids = [h["_id"] for h in hits]
        if not ids:
            break

        # Delete matching IDs from links index
        try:
            response = es.delete_by_query(
                index=links_index_pattern,
                body={"query": {"ids": {"values": ids}}},
                slices="auto",
                conflicts="proceed",
                refresh=False,
                wait_for_completion=True
            )
            deleted_total += response.get("deleted", 0)
        except Exception as e: #pylint: disable=broad-exception-caught
            print(f"[deduplicate_links_vs_content_es] Delete query failed: {e}")

        processed += len(ids)
        search_after = hits[-1]["sort"] if hits else None

        print(f"  → Processed {processed:,} | Deleted {deleted_total:,} so far...")

        if len(hits) < batch_size:
            break

    print(f"Deduplication complete. Total deleted: {deleted_total:,} docs.")
    return deleted_total

# pylint: disable=too-many-statements,too-many-branches,too-many-locals
async def run_fast_extension_pass(db, max_workers=MAX_FAST_WORKERS):
    """
    Perform a fast, targeted crawling pass for URLs ending in specific file extensions.

    This function is designed for high-speed, low-footprint crawling focused on
    non-HTML resources (e.g., PDFs, images, binaries, etc.). It operates by
    selecting a random time pivot within the Elasticsearch index to ensure diverse
    coverage, then progressively collects and filters candidate URLs using a series
    of efficient heuristics.

    Workflow:
        1. **Timestamp sampling** — Determines the earliest and latest timestamps
           in the index and picks a random pivot between them to introduce
           temporal randomness in the crawl.
        2. **Candidate gathering** — Uses Elasticsearch `search_after` pagination
           to efficiently fetch batches of URLs created after the pivot, up to
           `target_count`.
        3. **Extension filtering** — Keeps only URLs whose file extensions match
           entries defined in `EXTENSION_MAP`.
        4. **Host collapsing** — Collapses multiple URLs from the same host into
           one randomly chosen representative to maximize domain spread.
        5. **Asynchronous crawling** — Uses Playwright and asyncio semaphores to
           concurrently process URLs with controlled parallelism.
        6. **Result aggregation** — Normalizes crawler results using
           `preprocess_crawler_data()` and merges them into the global `results`
           structure, which is persisted via `db.save_batch()`.

    Performance notes:
        - Uses `search_after` for scalable pagination (avoiding deep pagination penalties).
        - Random pivoting prevents reprocessing the same time window repeatedly.
        - Global `results` is *not* reinitialized, ensuring cumulative data across runs.
        - One random URL per host ensures broad coverage and reduces fingerprinting.

    Args:
        db: Database wrapper that provides:
            - `es`: Elasticsearch client instance.
            - `save_batch(data: list)`: Persists batched crawl results.
        max_workers (int, optional): Maximum number of concurrent Playwright
            crawling tasks. Defaults to `MAX_FAST_WORKERS`.

    Returns:
        None
            The function updates the global `results` structure and persists
            it through `db.save_batch()`. All progress and summary information
            are logged to stdout.

    Raises:
        ElasticsearchException: If there is a failure during search or pagination.
        Exception: Any uncaught exceptions during asynchronous crawling tasks
                   are caught, logged, and skipped.
    """
    urls_index = f"{LINKS_INDEX}-*"
    target_count = 50_000
    batch_size = 1_000

    # --- Determine time range ---
    ts_query = {
        "size": 1,
        "query": {"match_all": {}},
        "sort": [{"created_at": "asc"}],
        "_source": ["created_at"]
    }
    first = db.es.search(index=urls_index, body=ts_query)
    ts_query["sort"] = [{"created_at": "desc"}]
    last = db.es.search(index=urls_index, body=ts_query)

    if not first["hits"]["hits"] or not last["hits"]["hits"]:
        print("No documents found in index.")
        return

    first_ts = dateutil.parser.isoparse(first["hits"]["hits"][0]["_source"]["created_at"])
    last_ts = dateutil.parser.isoparse(last["hits"]["hits"][0]["_source"]["created_at"])

    random_ts = first_ts + (last_ts - first_ts) * random.random()
    print(f"[FAST EXTENSION] Random pivot time: {random_ts.isoformat()}")

    # --- Search for candidates after random timestamp ---
    query = {
        "size": batch_size,
        "query": {"range": {"created_at": {"gte": random_ts.isoformat()}}},
        "sort": [{"created_at": "asc"}],
        "_source": ["url"]
    }

    candidate_urls = []
    search_after = None

    while len(candidate_urls) < target_count:
        if search_after:
            query["search_after"] = search_after

        resp = db.es.search(index=urls_index, body=query)
        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            url = hit["_source"].get("url")
            if not url:
                continue
            if "." in url.rsplit("/", 1)[-1]:  # crude filter for possible extension
                candidate_urls.append(url)
                if len(candidate_urls) >= target_count:
                    break

        search_after = hits[-1]["sort"] if hits else None
        if not search_after:
            break

    print(f"Collected {len(candidate_urls)} candidate URLs with extensions.")

    # --- Validate extensions in Python side ---
    valid_urls = []
    valid_exts = set(EXTENSION_MAP.keys())

    for url in candidate_urls:
        lower_url = url.lower()
        for ext in valid_exts:
            if lower_url.endswith(ext):
                valid_urls.append(url)
                break

    print(f"{len(valid_urls)} URLs matched known extensions.")

    # --- Collapse to one URL per host ---
    host_to_url = {}
    for url in valid_urls:
        host = urlparse(url).hostname
        if not host:
            continue
        if host not in host_to_url or random.random() < 0.5:
            host_to_url[host] = url

    final_urls = list(host_to_url.values())
    random.shuffle(final_urls)
    print(f"Collapsed to {len(final_urls)} unique hosts.")

    # --- Async crawling ---
    async with async_playwright() as playwright:
        semaphore = asyncio.Semaphore(max_workers)

        async def sem_task(url):
            async with semaphore:
                try:
                    return await fast_extension_crawler(url, EXTENSION_MAP, db, playwright)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print(f"[FAST EXT ERROR] {url}: {e}")
                    return None

        print(f"Starting async crawl with {max_workers} workers...")
        tasks = [sem_task(url) for url in final_urls]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # --- Process results ---
        for result in batch_results:
            if isinstance(result, Exception):
                continue
            if not result:
                continue

            presults = preprocess_crawler_data(result)
            if isinstance(presults, list):
                for item in presults:
                    if "crawledcontent" in item:
                        results["crawledcontent"].update(item["crawledcontent"])
                    if "crawledlinks" in item:
                        results["crawledlinks"].update(item["crawledlinks"])

        if results["crawledcontent"] or results["crawledlinks"]:
            db.save_batch([results])

    print("Fast extension crawling pass complete.")

# pylint: disable=too-many-branches,too-many-locals,too-many-return-statements
async def fast_extension_crawler(url, extension_map, db, playwright):
    """
    Quickly determines how to handle a given URL based on its Content-Type header,
    downloading files of allowed types and delegating others to the standard crawler.

    This asynchronous function:
    - Determines expected content types from the URL extension.
    - Performs a lightweight HEAD request to check the real Content-Type.
    - Validates if it matches expected patterns for that extension.
    - Downloads and processes content if allowed.
    - Falls back to normal crawler (`get_page`) otherwise.
    """
    global results # pylint: disable=global-statement
    print(f"[FAST CRAWLER] -{url}-")
    headers = {"User-Agent": UserAgent().random}

    async def fallback():
        await get_page(url, playwright, db)
        return

    # --- Determine expected content-type patterns from extension ---
    url_lower = url.lower()
    expected_patterns = None
    for ext, regex_list in extension_map.items():
        if url_lower.endswith(ext):
            expected_patterns = regex_list
            break

    if not expected_patterns:
        print(f"[FAST CRAWLER] Unknown extension for {url}")
        return await fallback()

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=10.0),
            verify=False,
            headers=headers,
            follow_redirects=True,
        ) as client:
            head_resp = await client.head(url)
    except Exception as e:  # pylint: disable=broad-exception-caught
        if DEBUG_PW:
            print(f"[fast_extension_crawler fallback] {e}")
        return await fallback()

    if not (200 <= head_resp.status_code < 300):
        return await fallback()

    content_type = head_resp.headers.get("Content-Type", "")
    if not content_type:
        return await fallback()

    content_type = content_type.lower().split(";")[0].strip()

    # --- Match against regexes for this extension only ---
    if not any(re.match(p, content_type) for p in expected_patterns):
        print(f"\033[92m[FAST CRAWLER] Mismatch content type for {url}, got: -{content_type}-\033[0m")
        return await fallback()

    # --- Host validation ---
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
                except Exception as e:  # pylint: disable=broad-exception-caught
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
                presults = preprocess_crawler_data(results)
                db.save_batch(presults)
                results = {"crawledcontent": {}, "crawledlinks": set()}
            break
        else:
            # Executed only if no break happened (no match found)
            print(f"\033[91m[FAST CRAWLER] UNKNOWN type -{url}- -{content_type}-\033[0m")

    except Exception as e:  # pylint: disable=broad-exception-caught
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
                cleanup_elasticsearch_indexes(
                    db,
                    remove_repeated_segments=REMOVE_REPEATED_SEGMENTS,
                    remove_empty_ctype=REMOVE_EMPTY_CTYPE,
                    remove_blocked_hosts=REMOVE_BLOCKED_HOSTS,
                    remove_blocked_urls=REMOVE_BLOCKED_URLS,
                    remove_invalid_urls=REMOVE_INVALID_URLS
                )
                print(f"Instance {instance}, iteration {iteration}: Checking for input URL files...")
                await process_input_url_files(db)
                print(f"Instance {instance}, iteration {iteration}: Let's go full crawler mode.")
                await crawler(db)
            elif instance == 2:
                print(f"Instance {instance}, iteration {iteration}: Running housekeeping, deduplication of links from indexes.")
                deduplicate_links_vs_content_es(db)
                print(f"Instance {instance}, iteration {iteration}: Running fast extension pass.")
                await run_fast_extension_pass(db)
                print(f"Instance {instance}, iteration {iteration}: Running crawler.")
                await crawler(db)
            else:
                print(f"Instance {instance}, iteration {iteration}: Running full crawler.")
                await crawler(db)
        db.close()


if __name__ == "__main__":
    asyncio.run(main())

