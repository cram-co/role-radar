"""
Role Radar collector
--------------------
Runs daily on GitHub Actions. Reads companies.csv, auto-detects which
ATS each company uses (Greenhouse, Lever, Ashby, SmartRecruiters,
Recruitee, Workable, Teamtailor), fetches all open roles, plus any
Workday tenants supplied by URL, and writes docs/feed.json for the
Role Radar app to consume.

Detection results are cached in docs/detected.json so each run only
probes a handful of new companies (DETECT_PER_RUN) — the full list
resolves itself over the first week of daily runs.
"""

import csv
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
DOCS.mkdir(exist_ok=True)
COMPANIES_CSV = ROOT / "companies.csv"
CACHE_FILE = DOCS / "detected.json"
FEED_FILE = DOCS / "feed.json"

DETECT_PER_RUN = 50          # new companies probed per run
REQUEST_DELAY = 0.35         # politeness delay between probe requests
TIMEOUT = 15
HEADERS = {"User-Agent": "RoleRadar/1.0 (personal job-search tool)"}

session = requests.Session()
session.headers.update(HEADERS)


# ---------------------------------------------------------------- slugs

def slug_candidates(name: str):
    """Generate likely ATS tokens from a company name."""
    base = re.sub(r"\(.*?\)", "", name)          # drop parentheticals
    base = re.sub(r"[/&].*$", "", base)          # drop trailing alternates
    base = base.strip().lower()
    words = re.findall(r"[a-z0-9]+", base)
    if not words:
        return []
    cands = []
    joined = "".join(words)
    dashed = "-".join(words)
    for c in (joined, dashed, words[0], "".join(words[:2])):
        if c and c not in cands and len(c) > 2:
            cands.append(c)
    return cands[:4]


# ---------------------------------------------------------------- probes
# Each returns a truthy token payload if the slug exists on that ATS.

def _nonempty(x):
    """A probe only counts as a match if the board returns at least one posting.
    Several ATS APIs answer 200 with an empty result set for slugs that don't
    exist (SmartRecruiters most notably), which produced false positives that
    were then cached and blocked the real ATS from ever being tried."""
    return bool(x)


def probe_greenhouse(slug):
    r = session.get(
        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", timeout=TIMEOUT
    )
    return r.status_code == 200 and _nonempty(r.json().get("jobs"))


def probe_breezy(slug):
    r = session.get(f"https://{slug}.breezy.hr/json", timeout=TIMEOUT)
    d = r.json()
    return r.status_code == 200 and isinstance(d, list) and _nonempty(d)


def probe_lever(slug):
    r = session.get(
        f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=TIMEOUT
    )
    d = r.json()
    return r.status_code == 200 and isinstance(d, list) and _nonempty(d)


def probe_ashby(slug):
    r = session.get(
        f"https://api.ashbyhq.com/posting-api/job-board/{slug}", timeout=TIMEOUT
    )
    return r.status_code == 200 and _nonempty(r.json().get("jobs"))


def probe_smartrecruiters(slug):
    r = session.get(
        f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1",
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        return False
    d = r.json()
    return _nonempty(d.get("content")) or (d.get("totalFound") or 0) > 0


def probe_recruitee(slug):
    r = session.get(f"https://{slug}.recruitee.com/api/offers/", timeout=TIMEOUT)
    return r.status_code == 200 and _nonempty(r.json().get("offers"))


def probe_workable(slug):
    r = session.post(
        f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
        json={"query": "", "location": [], "department": []},
        timeout=TIMEOUT,
    )
    return r.status_code == 200 and _nonempty(r.json().get("results"))


def probe_teamtailor(slug):
    r = session.get(f"https://{slug}.teamtailor.com/jobs", timeout=TIMEOUT)
    if r.status_code != 200 or "teamtailor" not in r.text.lower():
        return False
    # a real board links out to individual job pages
    return "/jobs/" in r.text


PROBES = {
    "greenhouse": probe_greenhouse,
    "breezy": probe_breezy,
    "lever": probe_lever,
    "ashby": probe_ashby,
    "smartrecruiters": probe_smartrecruiters,
    "recruitee": probe_recruitee,
    "workable": probe_workable,
    "teamtailor": probe_teamtailor,
}


def detect(name, hint=""):
    """Try slug candidates against each ATS; hinted ATS first."""
    order = list(PROBES)
    hint_ats = next((a for a in PROBES if a in (hint or "").lower()), None)
    if hint_ats:
        order.remove(hint_ats)
        order.insert(0, hint_ats)
    for slug in slug_candidates(name):
        for ats in order:
            try:
                time.sleep(REQUEST_DELAY)
                if PROBES[ats](slug):
                    # SmartRecruiters slugs are case-sensitive company IDs;
                    # try the TitleCase variant too if lowercase worked oddly
                    return {"ats": ats, "token": slug}
            except Exception:
                continue
    return None


# ---------------------------------------------------------------- fetchers

def fetch_greenhouse(token):
    d = session.get(
        f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs", timeout=TIMEOUT
    ).json()
    dept = {}
    try:
        dd = session.get(
            f"https://boards-api.greenhouse.io/v1/boards/{token}/departments",
            timeout=TIMEOUT,
        ).json()
        for dep in dd.get("departments", []):
            for j in dep.get("jobs", []):
                dept[j["id"]] = dep.get("name", "")
    except Exception:
        pass
    return [
        {
            "title": j["title"],
            "location": (j.get("location") or {}).get("name", ""),
            "department": dept.get(j["id"], ""),
            "url": j["absolute_url"],
            "posted_at": j.get("updated_at"),
        }
        for j in d.get("jobs", [])
    ]


def fetch_breezy(token):
    """Breezy HR public board: https://<token>.breezy.hr/json"""
    d = session.get(f"https://{token}.breezy.hr/json", timeout=TIMEOUT).json()
    out = []
    for j in d if isinstance(d, list) else []:
        loc = j.get("location") or {}
        if isinstance(loc, dict):
            country = loc.get("country") or {}
            country = country.get("name", "") if isinstance(country, dict) else str(country)
            parts = [loc.get("city") or loc.get("name") or "", country]
            location = ", ".join([p for p in parts if p]) or loc.get("name", "")
        else:
            location = str(loc)
        dept = j.get("department") or ""
        if isinstance(dept, dict):
            dept = dept.get("name", "")
        url = j.get("url") or ""
        if url and not url.startswith("http"):
            url = f"https://{token}.breezy.hr{url}"
        out.append({
            "title": j.get("name", "") or j.get("title", ""),
            "location": location,
            "department": dept,
            "url": url or f"https://{token}.breezy.hr/",
            "posted_at": j.get("published_date") or j.get("creation_date"),
        })
    return out


def fetch_lever(token):
    d = session.get(
        f"https://api.lever.co/v0/postings/{token}?mode=json", timeout=TIMEOUT
    ).json()
    return [
        {
            "title": j.get("text", ""),
            "location": (j.get("categories") or {}).get("location", "") or "",
            "department": (j.get("categories") or {}).get("department", "")
            or (j.get("categories") or {}).get("team", "")
            or "",
            "url": j.get("hostedUrl", ""),
            "posted_at": datetime.fromtimestamp(
                j["createdAt"] / 1000, tz=timezone.utc
            ).isoformat()
            if j.get("createdAt")
            else None,
        }
        for j in d
    ]


def fetch_ashby(token):
    d = session.get(
        f"https://api.ashbyhq.com/posting-api/job-board/{token}", timeout=TIMEOUT
    ).json()
    return [
        {
            "title": j.get("title", ""),
            "location": j.get("location", "") or "",
            "department": j.get("department", "") or j.get("team", "") or "",
            "url": j.get("jobUrl", ""),
            "posted_at": j.get("publishedAt"),
        }
        for j in d.get("jobs", [])
    ]


def fetch_smartrecruiters(token):
    out, offset = [], 0
    while True:
        d = session.get(
            f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
            f"?limit=100&offset={offset}",
            timeout=TIMEOUT,
        ).json()
        batch = d.get("content", [])
        out.extend(batch)
        if len(batch) < 100 or offset > 900:
            break
        offset += 100
    return [
        {
            "title": j.get("name", ""),
            "location": ", ".join(
                filter(
                    None,
                    [
                        (j.get("location") or {}).get("city"),
                        ((j.get("location") or {}).get("country") or "").upper(),
                    ],
                )
            ),
            "department": (j.get("department") or {}).get("label", "")
            or (j.get("function") or {}).get("label", ""),
            "url": f"https://jobs.smartrecruiters.com/{token}/{j['id']}",
            "posted_at": j.get("releasedDate"),
        }
        for j in out
    ]


def fetch_recruitee(token):
    d = session.get(f"https://{token}.recruitee.com/api/offers/", timeout=TIMEOUT).json()
    return [
        {
            "title": j.get("title", ""),
            "location": j.get("location", "") or "",
            "department": j.get("department", "") or "",
            "url": j.get("careers_url", ""),
            "posted_at": j.get("created_at"),
        }
        for j in d.get("offers", [])
    ]


def fetch_workable(token):
    out, token_page = [], None
    for _ in range(5):
        payload = {"query": "", "location": [], "department": []}
        if token_page:
            payload["token"] = token_page
        d = session.post(
            f"https://apply.workable.com/api/v3/accounts/{token}/jobs",
            json=payload,
            timeout=TIMEOUT,
        ).json()
        out.extend(d.get("results", []))
        token_page = d.get("nextPage")
        if not token_page:
            break
    return [
        {
            "title": j.get("title", ""),
            "location": ", ".join(
                filter(
                    None,
                    [
                        (j.get("location") or {}).get("city"),
                        (j.get("location") or {}).get("country"),
                    ],
                )
            )
            or ("Remote" if j.get("remote") else ""),
            "department": (j.get("department") or [""])[0]
            if isinstance(j.get("department"), list)
            else j.get("department", ""),
            "url": f"https://apply.workable.com/{token}/j/{j.get('shortcode','')}/",
            "posted_at": j.get("published"),
        }
        for j in out
    ]


def fetch_teamtailor(token):
    """Teamtailor has no public JSON API — parse the careers page HTML."""
    html = session.get(f"https://{token}.teamtailor.com/jobs", timeout=TIMEOUT).text
    jobs = []
    for m in re.finditer(
        r'href="(/jobs/[^"]+)"[^>]*>(?:\s*<[^>]+>)*\s*([^<]{4,120})', html
    ):
        url, title = m.group(1), m.group(2).strip()
        if title and not title.lower().startswith(("apply", "read more")):
            jobs.append(
                {
                    "title": title,
                    "location": "",
                    "department": "",
                    "url": f"https://{token}.teamtailor.com{url}",
                    "posted_at": None,
                }
            )
    return jobs


def fetch_workday(url):
    """
    url: a Workday careers URL like
    https://TENANT.wd3.myworkdayjobs.com/SITE
    Uses the public CXS JSON endpoint the career site itself calls.
    """
    m = re.match(r"https://([^.]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z-]+/)?([^/?#]+)", url)
    if not m:
        return []
    tenant, wd, site = m.groups()
    endpoint = f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
    out, offset = [], 0
    for _ in range(15):
        r = session.post(
            endpoint,
            json={"appliedFacets": {}, "limit": 20, "offset": offset, "searchText": ""},
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            break
        d = r.json()
        batch = d.get("jobPostings", [])
        for j in batch:
            out.append(
                {
                    "title": j.get("title", ""),
                    "location": j.get("locationsText", ""),
                    "department": "",
                    "url": f"https://{tenant}.{wd}.myworkdayjobs.com/{site}{j.get('externalPath','')}",
                    "posted_at": None,  # Workday gives "posted N days ago" text only
                    "posted_text": j.get("postedOn", ""),
                }
            )
        offset += 20
        if offset >= d.get("total", 0) or not batch:
            break
        time.sleep(REQUEST_DELAY)
    return out


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "breezy": fetch_breezy,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "smartrecruiters": fetch_smartrecruiters,
    "recruitee": fetch_recruitee,
    "workable": fetch_workable,
    "teamtailor": fetch_teamtailor,
}


# ---------------------------------------------------------------- main

def main():
    companies = []
    with open(COMPANIES_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("company"):
                companies.append(row)

    cache = {}
    if CACHE_FILE.exists():
        cache = json.loads(CACHE_FILE.read_text())

    # --- detection pass (budgeted) ---
    probed = 0
    for c in companies:
        name = c["company"].strip()
        if name in cache or probed >= DETECT_PER_RUN:
            continue
        if (c.get("workday_url") or "").strip():
            cache[name] = {"ats": "workday", "url": c["workday_url"].strip()}
            continue
        hint = c.get("ats_hint", "") or c.get("ats", "")
        if "workday" in hint.lower():
            cache[name] = {"ats": "workday_pending"}  # needs a URL in the CSV
            continue
        print(f"Detecting: {name}")
        result = detect(name, hint)
        cache[name] = result or {"ats": "unknown"}
        probed += 1

    CACHE_FILE.write_text(json.dumps(cache, indent=1))

    # --- fetch pass ---
    all_jobs = []
    for c in companies:
        name = c["company"].strip()
        info = cache.get(name) or {}
        ats = info.get("ats")
        try:
            if ats == "workday":
                jobs = fetch_workday(info["url"])
            elif ats in FETCHERS:
                jobs = FETCHERS[ats](info["token"])
            else:
                continue
            for j in jobs:
                j.update(company=name, ats=ats, source="collector")
            all_jobs.extend(jobs)
            print(f"{name}: {len(jobs)} roles ({ats})")
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"{name}: FAILED ({e})")

    feed = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "job_count": len(all_jobs),
        "jobs": all_jobs,
    }
    FEED_FILE.write_text(json.dumps(feed, indent=1))
    unknown = [n for n, v in cache.items() if v.get("ats") in ("unknown", "workday_pending")]
    print(f"\nFeed written: {len(all_jobs)} jobs")
    print(f"Unresolved companies ({len(unknown)}): {', '.join(unknown[:30])}")


if __name__ == "__main__":
    main()
