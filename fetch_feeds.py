#!/usr/bin/env python3
"""
Fetches all RSS feeds and writes feeds.json.
Runs via GitHub Actions — no CORS issues server-side.
"""

import json
import time
import feedparser
import requests
from datetime import datetime, timezone

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
    {"cat": "space", "name": "NASA News",            "url": "https://www.nasa.gov/news-release/feed/"},
    {"cat": "space", "name": "NASA JPL",             "url": "https://www.nasa.gov/centers-and-facilities/jpl/feed/"},
    {"cat": "space", "name": "NASA Aeronautics",     "url": "https://www.nasa.gov/aeronautics/feed/"},
    {"cat": "space", "name": "ESA Top News",         "url": "https://www.esa.int/rssfeed/TopNews"},
    {"cat": "space", "name": "Space.com",            "url": "https://www.space.com/home/feed/site.xml"},
    {"cat": "space", "name": "SpaceNews",            "url": "https://spacenews.com/feed/"},
    {"cat": "space", "name": "Spaceflight Now",      "url": "https://spaceflightnow.com/feed/"},
    {"cat": "space", "name": "Sky & Telescope",      "url": "https://www.skyandtelescope.com/astronomy-news/feed/"},
    {"cat": "space", "name": "Universe Today",       "url": "https://www.universetoday.com/feed/"},
    {"cat": "space", "name": "Quanta — Physics",     "url": "https://www.quantamagazine.org/physics/feed"},
    {"cat": "space", "name": "Physics World",        "url": "https://physicsworld.com/feed"},
    {"cat": "space", "name": "Phys.org Space",       "url": "https://phys.org/rss-feed/space-news"},
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

HEADERS = {"User-Agent": "EngineeringHub/1.0 (RSS reader; github-actions)"}
TIMEOUT  = 12
MAX_ITEMS = 10

def parse_date(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if t:
        return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
    return None

def fetch(feed_meta):
    try:
        r = requests.get(feed_meta["url"], headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        d = feedparser.parse(r.text)
        items = []
        for e in d.entries[:MAX_ITEMS]:
            title = e.get("title", "").strip()
            link  = e.get("link", "").strip()
            if not title or not link:
                continue
            items.append({
                "title": title,
                "link":  link,
                "date":  parse_date(e),
            })
        return {"name": feed_meta["name"], "cat": feed_meta["cat"],
                "items": items, "ok": True}
    except Exception as ex:
        print(f"  FAIL {feed_meta['name']}: {ex}")
        return {"name": feed_meta["name"], "cat": feed_meta["cat"],
                "items": [], "ok": False, "error": str(ex)}

print(f"Fetching {len(FEEDS)} feeds…")
results = []
for i, f in enumerate(FEEDS, 1):
    print(f"  [{i}/{len(FEEDS)}] {f['name']}")
    results.append(fetch(f))
    time.sleep(0.3)   # polite delay

output = {
    "updated": datetime.now(timezone.utc).isoformat(),
    "feeds":   results,
}

with open("feeds.json", "w", encoding="utf-8") as fh:
    json.dump(output, fh, ensure_ascii=False, indent=2)

ok  = sum(1 for r in results if r["ok"])
bad = len(results) - ok
print(f"\nDone — {ok} OK, {bad} failed. Written to feeds.json")
