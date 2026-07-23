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
import html
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
RETRY_UNKNOWN_DAYS = 10      # re-probe "unknown" companies after this many days
RETRY_PER_RUN = 30           # how many stale unknowns to re-probe each run
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


def _gh_jobs(slug):
    """Greenhouse boards hosted in the EU region still answer on the main API,
    so try that first and only then the eu-specific host."""
    for host in ("boards-api.greenhouse.io", "boards-api.eu.greenhouse.io"):
        try:
            r = session.get(f"https://{host}/v1/boards/{slug}/jobs", timeout=TIMEOUT)
            if r.status_code == 200:
                jobs = r.json().get("jobs")
                if jobs:
                    return jobs
        except Exception:
            continue
    return None


def probe_greenhouse_eu(slug):
    return _nonempty(_gh_jobs(slug))


def probe_bamboohr(slug):
    r = session.get(f"https://{slug}.bamboohr.com/careers/list", timeout=TIMEOUT)
    if r.status_code != 200:
        return False
    try:
        d = r.json()
    except Exception:
        return False
    # must be an actual list of postings — an empty board returns {"result": []}
    items = d.get("result") if isinstance(d, dict) else d
    return isinstance(items, list) and len(items) > 0


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
    "greenhouse_eu": probe_greenhouse_eu,
    "bamboohr": probe_bamboohr,
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


def fetch_greenhouse_eu(token):
    """Same shape as Greenhouse; resolves against whichever region answers."""
    d = {"jobs": _gh_jobs(token) or []}
    return [
        {
            "title": j["title"],
            "location": (j.get("location") or {}).get("name", ""),
            "department": "",
            "url": j["absolute_url"],
            "posted_at": j.get("updated_at"),
        }
        for j in d.get("jobs", [])
    ]


def fetch_bamboohr(token):
    """BambooHR public careers list: https://<token>.bamboohr.com/careers/list"""
    d = session.get(f"https://{token}.bamboohr.com/careers/list", timeout=TIMEOUT).json()
    items = d.get("result") if isinstance(d, dict) else d
    out = []
    for j in items or []:
        loc = j.get("location") or {}
        if isinstance(loc, dict):
            parts = [loc.get("city"), loc.get("state"), loc.get("country")]
            location = ", ".join([p for p in parts if p])
        else:
            location = str(loc or "")
        if j.get("isRemote"):
            location = (location + " (Remote)").strip()
        out.append({
            "title": j.get("jobOpeningName", "") or j.get("title", ""),
            "location": location,
            "department": j.get("departmentLabel", "") or j.get("department", ""),
            "url": f"https://{token}.bamboohr.com/careers/{j.get('id','')}",
            "posted_at": j.get("datePosted") or j.get("originalOpenDate"),
        })
    return out


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
    m = re.match(r"https://([^.]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-zA-Z]{2}-[a-zA-Z]{2}/)?([^/?#]+)", url)
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
    "greenhouse_eu": fetch_greenhouse_eu,
    "bamboohr": fetch_bamboohr,
    "breezy": fetch_breezy,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "smartrecruiters": fetch_smartrecruiters,
    "recruitee": fetch_recruitee,
    "workable": fetch_workable,
    "teamtailor": fetch_teamtailor,
}



# ---------------------------------------------------------------- agency boards
# Recruiter sites don't run a public ATS, so each needs its own reader. These are
# best-effort: if a site changes its markup the scraper returns [] and logs a
# warning rather than crashing the run.

AGENCY_UA = {"User-Agent": "Mozilla/5.0 (compatible; RoleRadar/1.0; +personal job-search tool)"}

# location words that appear at the tail of a Pentasia slug
_LOC_WORDS = {
    "malta","europe","remote","uk","usa","gibraltar","cyprus","ireland","spain","portugal",
    "italy","germany","france","netherlands","sweden","denmark","poland","romania","bulgaria",
    "greece","serbia","croatia","estonia","latvia","lithuania","ukraine","georgia","armenia",
    "australia","canada","brazil","mexico","colombia","peru","argentina","india","philippines",
    "singapore","japan","israel","turkey","uae","dubai","london","gibraltar","isleofman",
}


def scrape_pentasia():
    """pentasia.com — server-rendered Next.js listing, paginated ?page=N (0-indexed)."""
    out, seen = [], set()
    for page in range(0, 12):
        url = "https://www.pentasia.com/cm/candidates/jobs"
        if page:
            url += f"?page={page}"
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
            if r.status_code != 200:
                break
            html = r.text
        except Exception:
            break
        found = re.findall(
            r'href="(https://www\.pentasia\.com/careers/[^"?]+)[^"]*"[^>]*>([^<]{3,140})</a>', html
        )
        new = 0
        for link, title in found:
            title = html.unescape(re.sub(r"\s+", " ", title)).strip()
            if not title or title.lower() in ("apply now", "learn more", "view job"):
                continue
            if link in seen:
                continue
            seen.add(link)
            new += 1
            slug = link.rsplit("/", 1)[-1]
            slug = re.sub(r"-\d+-\d+$", "", slug)          # drop the trailing ids
            tail = slug.rsplit("-", 1)[-1] if "-" in slug else ""
            out.append({
                "title": title,
                "location": tail.title() if tail in _LOC_WORDS else "",
                "department": "",
                "url": link,
                "posted_at": None,
            })
        if not new:
            break
        time.sleep(REQUEST_DELAY)
    return out


def _try_json(url, label):
    """Fetch a candidate endpoint and report what came back. Used to discover the
    real jobs API on recruiter sites whose listings are rendered client-side."""
    try:
        r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        ct = r.headers.get("content-type", "")
        if r.status_code != 200:
            print(f"      {label}: HTTP {r.status_code}")
            return None
        if "json" not in ct:
            print(f"      {label}: 200 but {ct.split(';')[0]} (not json)")
            return None
        data = r.json()
        n = len(data) if isinstance(data, list) else len(data.get("data", data.get("results", data.get("jobs", [])) or []))
        print(f"      {label}: 200 JSON, {n} records  <-- USABLE" if n else f"      {label}: 200 JSON but empty")
        return data if n else None
    except Exception as e:
        print(f"      {label}: error ({type(e).__name__})")
        return None


def _normalise(items, base, source):
    """Best-effort mapping of an unknown JSON job shape onto our schema."""
    out = []
    if isinstance(items, dict):
        items = items.get("data") or items.get("results") or items.get("jobs") or []
    for j in items or []:
        if not isinstance(j, dict):
            continue
        title = j.get("title") or j.get("name") or j.get("job_title") or ""
        if isinstance(title, dict):
            title = title.get("rendered", "")
        title = html.unescape(re.sub(r"<[^>]+>", "", str(title))).strip()
        if not title:
            continue
        loc = j.get("location") or j.get("city") or j.get("job_location") or ""
        if isinstance(loc, dict):
            loc = loc.get("name") or loc.get("city") or ""
        elif isinstance(loc, list):
            loc = ", ".join(str(x) for x in loc if x)
        url = j.get("link") or j.get("url") or j.get("apply_url") or base
        out.append({
            "title": title,
            "location": html.unescape(str(loc)).strip(),
            "department": str(j.get("category") or j.get("department") or "" ) or "",
            "url": url,
            "posted_at": j.get("date_gmt") or j.get("date") or j.get("published_at") or j.get("created_at"),
        })
    return out


def _discover(base, candidates, source):
    """Try each candidate endpoint in turn; first one with records wins.
    Every attempt is logged so the run output tells us which path is live."""
    print(f"   {source}: probing {len(candidates)} candidate endpoints")
    for path in candidates:
        url = base + path
        data = _try_json(url, path)
        if data:
            jobs = _normalise(data, base, source)
            if jobs:
                print(f"   {source}: FOUND -> {path}  ({len(jobs)} jobs)")
                return jobs
        time.sleep(REQUEST_DELAY)
    return []


BETTINGJOBS_CANDIDATES = [
    "/wp-json/wp/v2/job?per_page=100",
    "/wp-json/wp/v2/jobs?per_page=100",
    "/wp-json/wp/v2/vacancy?per_page=100",
    "/wp-json/af/v1/jobs",
    "/wp-json/applyflow/v1/jobs",
    "/api/v1/jobs?limit=200",
    "/api/jobs?limit=200",
    "/jobs.json",
    "/wp-json/wp/v2/search?subtype=job&per_page=100",
]

VANKAIZEN_CANDIDATES = [
    "/wp-json/wp/v2/vacancy?per_page=100",
    "/wp-json/wp/v2/job?per_page=100",
    "/wp-json/wp/v2/jobs?per_page=100",
    "/wp-json/wp/v2/posts?per_page=100",
    "/api/vacancies",
    "/api/jobs",
    "/vacancies.json",
]


def scrape_bettingjobs():
    """bettingjobs.com — WordPress + Applyflow, listing rendered client-side.
    Discovers the live endpoint on first run, then falls back to sector pages."""
    jobs = _discover("https://www.bettingjobs.com", BETTINGJOBS_CANDIDATES, "BettingJobs")
    if jobs:
        return jobs
    print("   BettingJobs: no JSON endpoint found, falling back to sector pages")
    seen, res = set(), []
    sectors = ["hr-finance","marketing","executive-senior-appointments","it-technical",
               "analytics-bi","commercial","trading-sportsbook","operations",
               "compliance-legal","product"]
    for sec in sectors:
        try:
            r = session.get(f"https://www.bettingjobs.com/{sec}/", headers=AGENCY_UA, timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            for link, title in re.findall(
                r'href="(https://www\.bettingjobs\.com/job/[^"?]+)[^"]*"[^>]*>([^<]{3,140})</a>', r.text
            ):
                title = html.unescape(re.sub(r"\s+", " ", title)).strip()
                if not title or title.lower() in ("learn more", "apply now") or link in seen:
                    continue
                seen.add(link)
                res.append({"title": title, "location": "", "department": sec.replace("-", " ").title(),
                            "url": link, "posted_at": None})
            time.sleep(REQUEST_DELAY)
        except Exception:
            continue
    return res


def scrape_vankaizen():
    """vankaizen.com — bespoke board with load-more pagination."""
    jobs = _discover("https://www.vankaizen.com", VANKAIZEN_CANDIDATES, "Van Kaizen")
    if jobs:
        return jobs
    print("   Van Kaizen: no JSON endpoint found, falling back to page parse")
    try:
        r = session.get("https://www.vankaizen.com/vacancies", headers=AGENCY_UA, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        seen, res = set(), []
        for link, title in re.findall(
            r'href="(https?://(?:www\.)?vankaizen\.com/(?:vacancy|vacancies|job)/[^"?]+)[^"]*"[^>]*>([^<]{3,140})</a>',
            r.text,
        ):
            title = html.unescape(re.sub(r"\s+", " ", title)).strip()
            if not title or link in seen:
                continue
            seen.add(link)
            res.append({"title": title, "location": "", "department": "", "url": link, "posted_at": None})
        return res
    except Exception:
        return []


AGENCY_BOARDS = {
    "Pentasia": scrape_pentasia,
    "BettingJobs": scrape_bettingjobs,
    "Van Kaizen": scrape_vankaizen,
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

    # --- manual overrides always win (ats_token column: "greenhouse:midnite") ---
    for c in companies:
        name = c["company"].strip()
        override = (c.get("ats_token") or "").strip()
        if not override:
            continue
        if override.lower() in ("skip", "none", "-"):
            cache[name] = {"ats": "skip", "manual": True}
            continue
        # NB: check the URL form first — "https://..." also contains a colon
        if override.startswith("http"):
            cache[name] = {"ats": "workday", "url": override, "manual": True}
        elif ":" in override:
            ats, token = override.split(":", 1)
            ats, token = ats.strip().lower(), token.strip()
            if ats in FETCHERS and token:
                cache[name] = {"ats": ats, "token": token, "manual": True}
            else:
                print(f"  ! unrecognised ats_token for {name}: {override}")
        else:
            print(f"  ! malformed ats_token for {name}: {override}")

    # --- detection pass (budgeted) ---
    now = time.time()
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
        cache[name] = result or {"ats": "unknown", "checked": now}
        probed += 1

    # --- retry stale "unknown" companies -------------------------------------
    # A probe only matches when a board returns at least one posting, so a
    # company whose board was empty (or briefly erroring) gets recorded as
    # unknown. Without this, that verdict would stand forever.
    known = {c["company"].strip() for c in companies}
    stale = [
        n for n, v in cache.items()
        if n in known
        and v.get("ats") == "unknown"
        and not v.get("manual")
        and (now - v.get("checked", 0)) > RETRY_UNKNOWN_DAYS * 86400
    ]
    hints = {c["company"].strip(): (c.get("ats_hint") or "") for c in companies}
    for name in stale[:RETRY_PER_RUN]:
        print(f"Re-probing (was unknown): {name}")
        result = detect(name, hints.get(name, ""))
        cache[name] = result or {"ats": "unknown", "checked": now}

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

    # --- agency boards (recruiter sites, no public ATS) ---
    for name, fn in AGENCY_BOARDS.items():
        try:
            jobs = fn()
            for j in jobs:
                j.update(company=name, ats="agency", source="agency")
            all_jobs.extend(jobs)
            print(f"{name}: {len(jobs)} roles (agency board)"
                  + ("  <-- CHECK: returned nothing" if not jobs else ""))
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
