# playwrightcrawler
Playwright crawler

## Install

Before installing, consider increasing your tmpfs to 8GB.

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install
sudo venv/bin/playwright install-deps
```
---

# Usage 

This section lists **practical uses** for the crawler project. Each entry explains what you can do with the crawler, what kind of output to expect, and any important notes or caveats.

## Study top-level domain distribution

**What:** Crawl a set of seed URLs and collect the top-level domains (TLDs) for each discovered host (e.g., `.com`, `.edu`, `.br`).
**Output:** Tally of TLD counts, percent distribution, time-series if run periodically.
**Why:** Useful for research, internet geography, and measuring domain diversity of a dataset.


## Study word frequency on pages

**What:** Extract text content from HTML pages and compute word frequency lists, stopword-filtered n-grams, or TF-IDF vectors.
**Output:** Word counts, keyword clouds, CSVs and Elasticsearch indexes for later analysis.
**Why:** Great for topic analysis, content research, building search indexes, or training NLP models.


## Study directory distribution in URLs

**What:** Parse URL paths and analyze directory depth and common path segments (e.g., `/images/`, `/wp-content/`).
**Output:** Histograms of directory depth, frequency of path segments, path templates.
**Why:** Useful for understanding site structure patterns, detecting common CMS paths, and shaping crawling strategies.


## Your own personal search engine

**What:** Index crawled pages into Elasticsearch and expose a search interface over the collected content and metadata.
**Output:** Search index with full-text search, filters for host, content type, and date.
**Why:** Build private/focused search for research, personal archives, or internal documentation.


## A massive file downloader

**What:** Target binary content types (images, videos, archives) and download them efficiently to disk with streaming, deduplication, and size limits.
**Output:** Local file store or object storage with metadata about origin.
**Caveat:** **Only download from sources you are authorized to access.** 


## Noise traffic injection for security purposes

**What:** Generate controlled, low-rate noise or honey traffic patterns to test alerting, IDS/IPS, or honeypots in a controlled lab environment.
**Output:** Traffic logs and triggers for validation of security rules.
**Important:** This is **dual-use**. Only use on infrastructure you own or where you have explicit permission. Always follow an ethical testing plan and applicable laws.


## Internet link and infrastructure testing

**What:** Validate large numbers of links, check for broken links, measure latency, and detect common misconfigurations.
**Output:** Reports of broken/redirecting links.
**Why:** Useful for monitoring link health, OSINT research, and web hygiene audits.


## Website testing (functional & content checks)

**What:** Verify page responses, check presence/absence of expected elements, validate analytics tags, and detect regression in content.
**Output:** Pass/fail reports, screenshots, HTML diffs, and change detection alerts.
**Why:** Useful for QA, CI pipelines, and automated smoke tests.


## Safe-for-Work (SFW) Image Detection

**What:** Use the crawler’s SFW detection module to automatically classify downloaded images as safe or not safe for work (NSFW). This feature is ideal for filtering sensitive content, maintaining dataset compliance, or preparing AI training data that requires only non-explicit imagery.
**Output:** If classification enabled, every image will have a NSFW score in the database.
**Why:** Useful for website categorization and AI training.


## Content change monitoring / archival

**What:** Periodically crawl a set of pages and store snapshots or diffs for archival or monitoring. Useful for tracking updates or censorship.
**Output:** Time-stamped snapshots, diff reports, alerts on major changes.


## Data collection for machine learning

**What:** Gather labeled corpora — images, page text, or structured data — for training classifiers or recommendation systems.
**Output:** Datasets with provenance, labels (if applicable), and checksums.


## Threat intelligence and phishing detection (research)

**What:** Collect suspicious pages, analyze domain patterns, and extract indicators of compromise for research teams.
**Output:** Indicators (domains, URLs, hashes), cluster reports, and timelines.
**Important:** Use responsibly and avoid interacting with live malicious infrastructure without authorization.


## Digital preservation and archiving

**What:** Long-term capture of pages and resources for libraries, research groups, or projects that require preservation.
**Output:** Structured archives with metadata.


# Short guidance & ethics

* **Rate-limit** and use polite concurrency to avoid overwhelming third-party servers (this crawler supports throttling).
* **Data retention & privacy:** When collecting personal data, ensure you comply with privacy laws and your organization’s policies.

# References and sources

* https://tranco-list.eu/ 
