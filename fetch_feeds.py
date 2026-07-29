#!/usr/bin/env python3
"""
Engineering Intelligence Hub — RSS fetcher
Runs via GitHub Actions every hour, writes feeds.json.
Features: parallel fetching, retry logic, ETag caching, smart image filtering.
"""

import json, time, re, os
import feedparser
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

FEEDS = [
    # AUTOMOTIVE
    {"cat": "automotive", "name": "BMW Group PressClub",     "url": "https://www.press.bmwgroup.com/global/rss"},
    {"cat": "automotive", "name": "Porsche Newsroom",        "url": "https://newsroom.porsche.com/rss/en/index.rss"},
    {"cat": "automotive", "name": "Automotive World",        "url": "https://www.automotiveworld.com/feed/"},
    {"cat": "automotive", "name": "Automotive News Europe",  "url": "https://www.autonews.com/arc/outboundfeeds/rss/?outputType=xml"},
    {"cat": "automotive", "name": "InsideEVs",               "url": "https://insideevs.com/rss/news/all"},
    {"cat": "automotive", "name": "Plastics Technology",     "url": "https://www.ptonline.com/rss/blog"},
    {"cat": "automotive", "name": "Composites World",        "url": "https://www.compositesworld.com/rss"},
    {"cat": "automotive", "name": "All3DP",                  "url": "https://all3dp.com/feed/"},
    {"cat": "automotive", "name": "3D Printing Industry",    "url": "https://3dprintingindustry.com/feed"},
    {"cat": "automotive", "name": "Dassault 3DS Blog",       "url": "https://blog.3ds.com/feed/"},
    {"cat": "automotive", "name": "CATI 3DEXPERIENCE",       "url": "https://cati.com/feed"},
    # SPACE & PHYSICS
    {"cat": "space", "name": "NASA News",          "url": "https://www.nasa.gov/news-release/feed/"},
    {"cat": "space", "name": "NASA JPL",           "url": "https://www.nasa.gov/centers-and-facilities/jpl/feed/"},
    {"cat": "space", "name": "NASA Aeronautics",   "url": "https://www.nasa.gov/aeronautics/feed/"},
    {"cat": "space", "name": "ESA Top News",       "url": "https://www.esa.int/rssfeed/TopNews"},
    {"cat": "space", "name": "Space.com",          "url": "https://www.space.com/home/feed/site.xml"},
    {"cat": "space", "name": "SpaceNews",          "url": "https://spacenews.com/feed/"},
    {"cat": "space", "name": "Spaceflight Now",    "url": "https://spaceflightnow.com/feed/"},
    {"cat": "space", "name": "Sky & Telescope",    "url": "https://www.skyandtelescope.com/astronomy-news/feed/"},
    {"cat": "space", "name": "Universe Today",     "url": "https://www.universetoday.com/feed/"},
    {"cat": "space", "name": "Quanta — Physics",   "url": "https://www.quantamagazine.org/physics/feed"},
    {"cat": "space", "name": "Physics World",      "url": "https://physicsworld.com/feed"},
    {"cat": "space", "name": "Phys.org Space",     "url": "https://phys.org/rss-feed/space-news"},
    # GAMING
    {"cat": "gaming", "name": "IGN",                "url": "https://www.ign.com/rss"},
    {"cat": "gaming", "name": "GameSpot",           "url": "https://www.gamespot.com/feeds/news"},
    {"cat": "gaming", "name": "Polygon",            "url": "https://www.polygon.com/rss/index.xml"},
    {"cat": "gaming", "name": "Eurogamer",          "url": "https://www.eurogamer.net/feed"},
    {"cat": "gaming", "name": "Kotaku",             "url": "https://kotaku.com/rss"},
    {"cat": "gaming", "name": "Rock Paper Shotgun", "url": "https://www.rockpapershotgun.com/feed"},
    {"cat": "gaming", "name": "PC Gamer",           "url": "https://www.pcgamer.com/rss/"},
    {"cat": "gaming", "name": "PlayStation Blog",   "url": "https://blog.playstation.com/feed"},
    {"cat": "gaming", "name": "Xbox Wire",          "url": "https://news.xbox.com/en-us/feed/"},
    {"cat": "gaming", "name": "GamesIndustry.biz",  "url": "https://www.gamesindustry.biz/feed"},
    {"cat": "gaming", "name": "Game Developer",     "url": "https://www.gamedeveloper.com/rss.xml"},
    # GENERAL ENGINEERING
    {"cat": "engineering", "name": "IEEE Spectrum",            "url": "https://spectrum.ieee.org/feeds/feed.rss"},
    {"cat": "engineering", "name": "Interesting Engineering",  "url": "https://interestingengineering.com/feed"},
    {"cat": "engineering", "name": "The Engineer (UK)",        "url": "https://www.theengineer.co.uk/feed"},
    {"cat": "engineering", "name": "ScienceDaily Eng.",        "url": "https://www.sciencedaily.com/rss/matter_energy/engineering.xml"},
    {"cat": "engineering", "name": "Design News",              "url": "https://www.designnews.com/rss"},
    {"cat": "engineering", "name": "MIT Technology Review",    "url": "https://www.technologyreview.com/feed/"},
    {"cat": "engineering", "name": "Ars Technica",             "url": "https://feeds.arstechnica.com/arstechnica/index"},
    {"cat": "engineering", "name": "Quanta — Technology",      "url": "https://www.quantamagazine.org/technology/feed"},
    {"cat": "engineering", "name": "Electronic Design",        "url": "https://www.electronicdesign.com/rss"},
    {"cat": "engineering", "name": "ENR",                      "url": "https://www.enr.com/rss/articles"},
]

HEADERS      = {"User-Agent": "EngineeringHub/1.0 (RSS reader; github-actions)"}
TIMEOUT      = 12
MAX_ITEMS    = 10
MAX_WORKERS  = 8          # parallel threads
MAX_RETRIES  = 2
RETRY_DELAY  = 3          # seconds between retries
MIN_IMG_W    = 200        # minimum image width to accept from media:thumbnail
CACHE_FILE   = ".feed_cache.json"  # ETag / Last-Modified cache

IMG_RE  = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)
# Only reject obvious tracking pixels — not broad terms like "icon"
BAD_IMG = re.compile(
    r'(1x1|pixel\.gif|pixel\.png|spacer\.gif|blank\.gif|tracking|'
    r'stat\.wp\.com|feeds\.feedburner\.com/~|gravatar\.com)',
    re.IGNORECASE
)

# ── Cache helpers ──────────────────────────────────────────────────────────────
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass

# ── Image extraction ───────────────────────────────────────────────────────────
def extract_image(entry):
    candidates = []

    # 1. media:thumbnail — check minimum width
    mt = entry.get("media_thumbnail")
    if mt and isinstance(mt, list):
        for t in mt:
            url = t.get("url", "")
            try:
                w = int(t.get("width", 0))
            except (ValueError, TypeError):
                w = 0
            if url and (w == 0 or w >= MIN_IMG_W):
                candidates.append(url)
                break

    # 2. media:content with image type
    mc = entry.get("media_content")
    if mc and isinstance(mc, list):
        for m in mc:
            if "image" in m.get("type", "") or m.get("medium") == "image":
                try:
                    w = int(m.get("width", 0))
                except (ValueError, TypeError):
                    w = 0
                if w == 0 or w >= MIN_IMG_W:
                    candidates.append(m.get("url", ""))

    # 3. enclosures typed as images
    for enc in entry.get("enclosures", []):
        if "image" in enc.get("type", ""):
            candidates.append(enc.get("href", "") or enc.get("url", ""))

    # 4. links with rel=enclosure and image type
    for lnk in entry.get("links", []):
        if lnk.get("rel") == "enclosure" and "image" in lnk.get("type", ""):
            candidates.append(lnk.get("href", ""))

    # 5. First <img> from content/summary HTML
    for field in ("content", "summary", "description"):
        val = entry.get(field)
        if isinstance(val, list):
            val = val[0].get("value", "") if val else ""
        if val:
            imgs = IMG_RE.findall(val)
            candidates.extend(imgs)
            break

    # 6. iTunes image
    ii = entry.get("itunes_image")
    if ii:
        candidates.append(ii.get("href", ""))

    for c in candidates:
        c = (c or "").strip()
        if c.startswith("http") and not BAD_IMG.search(c) and len(c) > 12:
            return c
    return None

def extract_summary(entry):
    for field in ("summary", "description"):
        val = entry.get(field, "")
        if val:
            text = re.sub(r'<[^>]+>', ' ', val)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) > 15:
                return (text[:180].rsplit(' ', 1)[0] + '…') if len(text) > 180 else text
    return ""

def parse_date(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if t:
        return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
    return None

# ── HTTP fetch with retry + ETag caching ──────────────────────────────────────
def http_get(url, cache):
    cached = cache.get(url, {})
    headers = dict(HEADERS)
    if cached.get("etag"):
        headers["If-None-Match"] = cached["etag"]
    if cached.get("last_modified"):
        headers["If-Modified-Since"] = cached["last_modified"]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 304:
                # Not modified — return cached content
                return cached.get("content", ""), cache, True
            r.raise_for_status()
            # Update cache headers
            cache[url] = {
                "etag":          r.headers.get("ETag"),
                "last_modified": r.headers.get("Last-Modified"),
                "content":       r.text,
            }
            return r.text, cache, False
        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                print(f"    retry {attempt}/{MAX_RETRIES} for {url}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                raise

# ── Fetch one feed ─────────────────────────────────────────────────────────────
def fetch(feed_meta, cache):
    try:
        content, cache, from_cache = http_get(feed_meta["url"], cache)
        d = feedparser.parse(content)

        # Feed-level fallback image (channel logo)
        feed_img = None
        fi = d.feed.get("image", {})
        if fi.get("url"):
            feed_img = fi["url"]
        elif d.feed.get("itunes_image"):
            feed_img = d.feed["itunes_image"].get("href")

        items = []
        for e in d.entries[:MAX_ITEMS]:
            title = e.get("title", "").strip()
            link  = e.get("link",  "").strip()
            if not title or not link:
                continue
            img = extract_image(e) or feed_img  # fall back to channel logo
            items.append({
                "title":   title,
                "link":    link,
                "date":    parse_date(e),
                "image":   img,
                "summary": extract_summary(e),
            })

        return {
            "name":       feed_meta["name"],
            "cat":        feed_meta["cat"],
            "items":      items,
            "ok":         True,
            "from_cache": from_cache,
        }, cache

    except Exception as ex:
        print(f"  FAIL {feed_meta['name']}: {ex}")
        return {
            "name":  feed_meta["name"],
            "cat":   feed_meta["cat"],
            "items": [],
            "ok":    False,
            "error": str(ex),
        }, cache

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    t0    = time.time()
    cache = load_cache()
    print(f"Fetching {len(FEEDS)} feeds with {MAX_WORKERS} parallel workers…")

    results      = [None] * len(FEEDS)
    cache_lock   = {}   # each thread gets its own cache slice; merge after
    futures_map  = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_idx = {
            ex.submit(fetch, feed, dict(cache)): i
            for i, feed in enumerate(FEEDS)
        }
        for future in as_completed(future_to_idx):
            i = future_to_idx[future]
            result, updated_cache = future.result()
            results[i] = result
            cache.update(updated_cache)
            status = "cached" if result.get("from_cache") else ("OK" if result["ok"] else "FAIL")
            print(f"  [{status}] {result['name']}")

    save_cache(cache)

    output = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "feeds":   results,
    }
    with open("feeds.json", "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    ok      = sum(1 for r in results if r["ok"])
    cached  = sum(1 for r in results if r.get("from_cache"))
    bad     = len(results) - ok
    imgs    = sum(sum(1 for it in r["items"] if it.get("image")) for r in results)
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s — {ok} OK ({cached} from cache), {bad} failed, {imgs} images. → feeds.json")

if __name__ == "__main__":
    main()
