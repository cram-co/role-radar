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
    so try that first and only then the eu-specific host. Uses the same payload
    helper so `first_published` is picked up rather than `updated_at`."""
    for host in ("boards-api.greenhouse.io", "boards-api.eu.greenhouse.io"):
        try:
            jobs = _greenhouse_payload(slug, host).get("jobs")
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


SF_HOSTS = ["career2.successfactors.eu", "career4.successfactors.com",
            "career5.successfactors.eu", "career10.successfactors.com",
            "careersd2.successfactors.eu", "performancemanager.successfactors.eu"]


def _sf_parts(token):
    """Accept either 'company' or 'host|company' / 'host/company'."""
    tok = token.replace("/", "|")
    if "|" in tok:
        host, company = tok.split("|", 1)
        return [host.strip()], company.strip()
    return SF_HOSTS, tok.strip()


def fetch_successfactors(token):
    """SAP SuccessFactors publishes no public jobs API, but career sites expose
    an XML sitemap carrying title, location and employer. Falls back to parsing
    jobreqcareer links off the rendered listing."""
    hosts, company = _sf_parts(token)
    out = []
    for host in hosts:
        base = f"https://{host}"
        # 1) the sitemap (documented as sitemap.xml, occasionally sitemal.xml)
        for name in ("sitemap.xml", "sitemal.xml"):
            try:
                r = session.get(f"{base}/{name}?company={company}",
                                headers=AGENCY_UA, timeout=TIMEOUT)
                if r.status_code != 200 or "xml" not in r.headers.get("content-type", ""):
                    print(f"      sf {company} {host}/{name}: HTTP {r.status_code} "
                          f"{r.headers.get('content-type','?').split(';')[0]}")
                    continue
                blocks = re.findall(r"<url>(.*?)</url>", r.text, re.S)
                for b in blocks:
                    loc = re.search(r"<loc>\s*([^<\s]+)\s*</loc>", b)
                    if not loc or "jobId=" not in loc.group(1):
                        continue
                    title = re.search(r"<(?:title|job:title)>(.*?)</", b, re.S)
                    place = re.search(r"<(?:location|job:location)>(.*?)</", b, re.S)
                    t = html.unescape(re.sub(r"<[^>]+>", "", title.group(1))).strip() if title else ""
                    if not t:
                        continue
                    out.append({
                        "title": t,
                        "location": html.unescape(re.sub(r"<[^>]+>", "", place.group(1))).strip() if place else "",
                        "department": "",
                        "url": loc.group(1),
                        "posted_at": None,
                    })
                if out:
                    print(f"      successfactors {company}: sitemap on {host} -> {len(out)} jobs")
                    return out
            except Exception:
                continue
        # 2) the rendered listing — pull jobreqcareer links and their anchor text
        for path in (f"/career?company={company}&career_ns=job_listing",
                     f"/careers?company={company}",
                     f"/career?company={company}"):
            try:
                r = session.get(base + path, headers=AGENCY_UA, timeout=TIMEOUT)
                if r.status_code != 200:
                    print(f"      sf {company} {host}{path[:28]}: HTTP {r.status_code}")
                    continue
                hits = _links_with_titles(r.text, base, "jobreqcareer")
                if not hits:
                    ids = set(re.findall(r"jobId=(\d+)", r.text))
                    if ids:
                        print(f"      successfactors {company}: {len(ids)} jobIds on {host} but no titles parsed")
                    continue
                for u, t in hits:
                    out.append({"title": t, "location": "", "department": "",
                                "url": u, "posted_at": None})
                if out:
                    print(f"      successfactors {company}: listing on {host} -> {len(out)} jobs")
                    return out
            except Exception:
                continue
    print(f"      successfactors {company}: nothing found across {len(hosts)} host(s)")
    return out


# NB: deliberately absent from PROBES. Auto-detection would mean 6 hosts x 3
# paths per company, and SuccessFactors IDs are never derivable from the company
# name (OPAP is "opapsa"), so this lane is manual-pin only.


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
        headers={"Accept": "application/json"}, timeout=TIMEOUT,
    )
    if r.status_code != 200:
        return False
    d = r.json()
    return _nonempty(d.get("results") or d.get("jobs"))


def _tt_base(token):
    """Teamtailor boards are usually <token>.teamtailor.com, but customers can
    put them on their own domain (MrQ use careers.lindar.com)."""
    if "." in token:
        return token if token.startswith("http") else f"https://{token}"
    return f"https://{token}.teamtailor.com"


def probe_teamtailor(slug):
    r = session.get(f"{_tt_base(slug)}/jobs", timeout=TIMEOUT)
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

def _greenhouse_payload(token, host="boards-api.greenhouse.io"):
    """Greenhouse's plain jobs endpoint only exposes `updated_at`, which resets
    whenever a posting is edited — so a bulk edit makes an entire board look
    posted today. Asking for content=true also returns `first_published`, the
    real go-live date. Falls back to the plain endpoint if that call fails."""
    try:
        r = session.get(
            f"https://{host}/v1/boards/{token}/jobs?content=true", timeout=TIMEOUT
        )
        if r.status_code == 200:
            d = r.json()
            if d.get("jobs") and any(j.get("first_published") for j in d["jobs"]):
                return d
    except Exception:
        pass
    return session.get(f"https://{host}/v1/boards/{token}/jobs", timeout=TIMEOUT).json()


def fetch_greenhouse(token):
    d = _greenhouse_payload(token)
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
            "posted_at": j.get("first_published") or j.get("updated_at"),
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
            "posted_at": j.get("first_published") or j.get("updated_at"),
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
    """Workable's v3 board API. Their schema has shifted over time — `location`
    may be a dict, a list, or absent in favour of `locations` — so every field is
    read defensively and a single odd record can't take the whole board down."""
    out, page_token = [], None
    for _ in range(6):
        payload = {"query": "", "location": [], "department": []}
        if page_token:
            payload["token"] = page_token
        try:
            r = session.post(
                f"https://apply.workable.com/api/v3/accounts/{token}/jobs",
                json=payload, headers={"Accept": "application/json"}, timeout=TIMEOUT,
            )
            if r.status_code != 200:
                print(f"      workable {token}: HTTP {r.status_code}")
                break
            d = r.json()
        except Exception as e:
            print(f"      workable {token}: {type(e).__name__}")
            break
        batch = d.get("results") or d.get("jobs") or []
        out.extend(batch)
        page_token = d.get("nextPage")
        if not page_token or not batch:
            break
        time.sleep(REQUEST_DELAY)

    def place(j):
        loc = j.get("location") or j.get("locations") or {}
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        if isinstance(loc, dict):
            parts = [loc.get("city"), loc.get("region"), loc.get("country")]
            txt = ", ".join([p for p in parts if isinstance(p, str) and p])
        else:
            txt = str(loc)
        return txt or ("Remote" if j.get("remote") or j.get("workplace") == "remote" else "")

    def dept(j):
        d = j.get("department")
        if isinstance(d, list):
            d = d[0] if d else ""
        if isinstance(d, dict):
            d = d.get("name", "")
        return str(d or "")

    jobs = []
    for j in out:
        if not isinstance(j, dict):
            continue
        title = str(j.get("title") or "").strip()
        if not title:
            continue
        try:
            jobs.append({
                "title": title,
                "location": place(j),
                "department": dept(j),
                "url": j.get("url") or j.get("shortlink")
                       or f"https://apply.workable.com/{token}/j/{j.get('shortcode','')}/",
                "posted_at": j.get("published") or j.get("published_on") or j.get("created_at"),
            })
        except Exception:
            continue          # never let one malformed record lose the whole board
    if out and not jobs:
        print(f"      workable {token}: {len(out)} records but none mapped "
              f"(keys: {sorted(out[0].keys())[:10]})")
    return jobs


def fetch_teamtailor(token):
    """Teamtailor has no public JSON API — parse the careers page HTML.
    Works for <token>.teamtailor.com and for boards on a customer's own domain."""
    base = _tt_base(token)
    try:
        page = session.get(f"{base}/jobs", headers=AGENCY_UA, timeout=TIMEOUT).text
    except Exception:
        return []
    jobs = []
    for url, title in _links_with_titles(page, base, "/jobs/"):
        if url.rstrip("/").endswith("/jobs"):
            continue
        jobs.append({"title": title, "location": "", "department": "",
                     "url": url, "posted_at": None})
    return jobs


def fetch_workday(url):
    """
    url: a Workday careers URL like
    https://TENANT.wd3.myworkdayjobs.com/SITE
    Uses the public CXS JSON endpoint the career site itself calls.
    """
    m = re.match(r"https://([^.]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-zA-Z]{2}-[a-zA-Z]{2}/)?([^/?#]+)", url)
    if not m:
        print(f"      workday: URL didn't parse -> {url}")
        return []
    tenant, wd, site = m.groups()
    # The CXS tenant is usually the subdomain, but not always — some tenants use
    # the site name instead, so try both before giving up.
    for cxs in (tenant, site.lower(), site):
        probe = f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{cxs}/{site}/jobs"
        try:
            t = session.post(probe, json={"appliedFacets": {}, "limit": 1, "offset": 0,
                                          "searchText": ""}, timeout=TIMEOUT)
            if t.status_code == 200 and (t.json().get("total") or t.json().get("jobPostings")):
                tenant = cxs
                break
            print(f"      workday {tenant}/{site}: cxs '{cxs}' -> HTTP {t.status_code}"
                  f"{' (0 results)' if t.status_code == 200 else ''}")
        except Exception as e:
            print(f"      workday {tenant}/{site}: cxs '{cxs}' error ({type(e).__name__})")
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
    "successfactors": fetch_successfactors,
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

AGENCY_UA = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# location words that appear at the tail of a Pentasia slug
_LOC_WORDS = {
    "malta","europe","remote","uk","usa","gibraltar","cyprus","ireland","spain","portugal",
    "italy","germany","france","netherlands","sweden","denmark","poland","romania","bulgaria",
    "greece","serbia","croatia","estonia","latvia","lithuania","ukraine","georgia","armenia",
    "australia","canada","brazil","mexico","colombia","peru","argentina","india","philippines",
    "singapore","japan","israel","turkey","uae","dubai","london","gibraltar","isleofman",
}



# slug fragments that indicate a listing/category page rather than a real vacancy
_NOT_A_JOB = {
    "vacancies", "vacancy", "jobs", "job", "careers", "career", "search",
    "page", "all", "index", "apply", "roles", "opportunities", "live-roles",
}

# shorthand that reads badly when title-cased from a slug
_FIX_CASE = {
    "Aml": "AML", "Cft": "CFT", "Amlcft": "AML/CFT", "Kyc": "KYC", "Coo": "COO",
    "Ceo": "CEO", "Cfo": "CFO", "Cto": "CTO", "Cmo": "CMO", "Cpo": "CPO",
    "Vp": "VP", "Md": "MD", "Gm": "GM", "Hr": "HR", "It": "IT", "Bi": "BI",
    "Crm": "CRM", "Seo": "SEO", "Ppc": "PPC", "Vip": "VIP", "Ux": "UX",
    "Ui": "UI", "Qa": "QA", "Uk": "UK", "Us": "US", "Eu": "EU", "Latam": "LATAM",
    "Dach": "DACH", "Emea": "EMEA", "Apac": "APAC", "B2b": "B2B", "B2c": "B2C",
    "Ftd": "FTD", "Pam": "PAM", "Okr": "OKR", "Psp": "PSP",
}

BETTINGJOBS_CANDIDATES = [
    "/wp-json/wp/v2/job?per_page=100",
    "/wp-json/wp/v2/jobs?per_page=100",
    "/wp-json/wp/v2/vacancy?per_page=100",
    "/wp-json/af/v1/jobs",
    "/wp-json/applyflow/v1/jobs",
    "/api/v1/jobs?limit=200",
    "/api/jobs?limit=200",
    "/jobs.json",
]

VANKAIZEN_CANDIDATES = [
    "/wp-json/wp/v2/vacancy?per_page=100",
    "/wp-json/wp/v2/job?per_page=100",
    "/wp-json/wp/v2/jobs?per_page=100",
    "/wp-json/wp/v2/posts?per_page=100",
    "/api/vacancies",
    "/api/jobs",
]


def _links_with_titles(html_text, base, path_marker):
    """(url, title) pairs for links whose path contains `path_marker`.
    Deliberately tolerant: hrefs may be relative or absolute, and the visible
    title is often wrapped in nested tags rather than sitting directly inside
    the anchor, so tags are stripped rather than excluded."""
    out, seen = [], set()
    rx = re.compile(
        r'href="((?:https?://[^"]*?)?' + re.escape(path_marker) + r'[^"?#]+)[^"]*"[^>]*>(.*?)</a>',
        re.S | re.I,
    )
    for href, inner in rx.findall(html_text):
        title = html.unescape(re.sub(r"<[^>]+>", " ", inner))
        title = re.sub(r"\s+", " ", title).strip()
        if not title or len(title) < 3 or len(title) > 140:
            continue
        if title.lower() in ("apply now", "learn more", "view job", "read more", "more info"):
            continue
        url = href if href.startswith("http") else base.rstrip("/") + href
        if url in seen:
            continue
        seen.add(url)
        out.append((url, title))
    return out


def _next_data_jobs(html_text, base):
    """Next.js ships page data as JSON in a __NEXT_DATA__ script tag. Where it
    exists it's far more reliable than parsing rendered markup."""
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, re.S)
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
    except Exception:
        return []
    found, stack = [], [data]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        if not isinstance(node, dict):
            continue
        title = node.get("jobTitle") or node.get("title") or node.get("name")
        link = node.get("url") or node.get("slug") or node.get("link")
        if isinstance(title, str) and isinstance(link, str) and "/careers/" in link:
            t = html.unescape(re.sub(r"<[^>]+>", "", title)).strip()
            if 3 <= len(t) <= 140:
                u = link if link.startswith("http") else base.rstrip("/") + link
                loc = node.get("location") or node.get("jobLocation") or ""
                if isinstance(loc, dict):
                    loc = loc.get("name") or loc.get("city") or ""
                found.append({"title": t, "location": str(loc)[:60], "department": "",
                              "url": u, "posted_at": node.get("datePosted") or node.get("createdAt")})
        stack.extend(v for v in node.values() if isinstance(v, (dict, list)))
    seen, out = set(), []
    for j in found:
        k = (j["title"], j["url"])
        if k in seen:
            continue
        seen.add(k)
        out.append(j)
    return out


def _jsonld_jobs(url, source):
    """schema.org JobPosting objects from a page's JSON-LD blocks. Job boards
    embed these for Google for Jobs, and Google doesn't reliably run JavaScript,
    so the markup is usually server-rendered even when the listing isn't."""
    try:
        r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"      json-ld {url}: HTTP {r.status_code}")
            return []
        blocks = re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            r.text, re.S | re.I,
        )
    except Exception as e:
        print(f"      json-ld {url}: error ({type(e).__name__})")
        return []
    out = []
    for blk in blocks:
        try:
            data = json.loads(blk.strip())
        except Exception:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
                continue
            if not isinstance(node, dict):
                continue
            for key in ("@graph", "itemListElement", "item"):
                if key in node:
                    stack.append(node[key])
            if node.get("@type") not in ("JobPosting", ["JobPosting"]):
                continue
            title = html.unescape(re.sub(r"<[^>]+>", "", str(node.get("title", "")))).strip()
            if not title:
                continue
            loc = ""
            jl = node.get("jobLocation")
            if isinstance(jl, list):
                jl = jl[0] if jl else {}
            if isinstance(jl, dict):
                addr = jl.get("address", {})
                if isinstance(addr, dict):
                    loc = ", ".join(
                        str(addr.get(k)) for k in ("addressLocality", "addressRegion", "addressCountry")
                        if isinstance(addr.get(k), str)
                    )
            if not loc and node.get("jobLocationType") == "TELECOMMUTE":
                loc = "Remote"
            out.append({
                "title": title, "location": loc, "department": "",
                "url": node.get("url") or url, "posted_at": node.get("datePosted"),
            })
    if out:
        print(f"      json-ld {url}: {len(out)} JobPosting blocks  <-- USABLE")
    return out


def _sitemap_job_urls(base, must_contain, limit=600):
    """Walk the site's XML sitemaps for URLs that look like job pages.
    Static XML, so it works regardless of how the site renders."""
    found, seen_maps = [], set()
    queue = [base + "/sitemap.xml", base + "/sitemap_index.xml", base + "/job-sitemap.xml"]
    while queue and len(found) < limit:
        sm = queue.pop(0)
        if sm in seen_maps:
            continue
        seen_maps.add(sm)
        try:
            r = session.get(sm, headers=AGENCY_UA, timeout=TIMEOUT)
            if r.status_code != 200 or "xml" not in r.headers.get("content-type", ""):
                continue
            locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", r.text)
        except Exception:
            continue
        for u in locs:
            if u.endswith(".xml") and len(seen_maps) < 25:
                queue.append(u)
            elif must_contain in u:
                found.append(u)
        time.sleep(REQUEST_DELAY)
    if found:
        print(f"      sitemap {base}: {len(found)} job URLs")
    return found[:limit]


def _titles_from_urls(urls, source):
    """Derive a title from a job URL slug — last resort, gives title only."""
    out, seen = [], set()
    for u in urls:
        slug = u.rstrip("/").rsplit("/", 1)[-1]
        if slug.lower() in _NOT_A_JOB:
            continue
        slug = re.sub(r"[-_]?\d{4,}[-_]?\d*$", "", slug)
        slug = re.sub(r"[-_]\d{1,3}$", "", slug)
        words = [w for w in re.split(r"[-_]+", slug) if w]
        if not words or len("".join(words)) < 3:
            continue
        title = " ".join(_FIX_CASE.get(w.title(), w.title()) for w in words)
        if title.lower() in seen:
            continue
        seen.add(title.lower())
        out.append({"title": title, "location": "", "department": "",
                    "url": u, "posted_at": None})
    return out


def _try_json(url, label):
    """Fetch a candidate endpoint and report what came back."""
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
        n = len(data) if isinstance(data, list) else len(
            data.get("data", data.get("results", data.get("jobs", [])) or []))
        print(f"      {label}: 200 JSON, {n} records" + ("  <-- USABLE" if n else " but empty"))
        return data if n else None
    except Exception as e:
        print(f"      {label}: error ({type(e).__name__})")
        return None


def _normalise(items, base, source):
    """Map an unknown JSON job shape onto our schema."""
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
        out.append({
            "title": title, "location": html.unescape(str(loc)).strip(),
            "department": str(j.get("category") or j.get("department") or ""),
            "url": j.get("link") or j.get("url") or j.get("apply_url") or base,
            "posted_at": j.get("date_gmt") or j.get("date") or j.get("published_at") or j.get("created_at"),
        })
    return out


def _discover(base, candidates, source):
    """Try each candidate endpoint; first with records wins. Every attempt is
    logged so the run output shows which path is live."""
    print(f"   {source}: probing {len(candidates)} candidate endpoints")
    for path in candidates:
        data = _try_json(base + path, path)
        if data:
            jobs = _normalise(data, base, source)
            if jobs:
                print(f"   {source}: FOUND -> {path}  ({len(jobs)} jobs)")
                return jobs
        time.sleep(REQUEST_DELAY)
    return []


def scrape_pentasia():
    """pentasia.com — Next.js listing, paginated ?page=N (0-indexed).
    Order: __NEXT_DATA__ JSON, then JSON-LD, then tolerant link parsing."""
    base = "https://www.pentasia.com"
    out, seen = [], set()
    for page in range(0, 12):
        url = base + "/cm/candidates/jobs" + (f"?page={page}" if page else "")
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        except Exception as e:
            print(f"      Pentasia page {page}: error ({type(e).__name__})")
            break
        if r.status_code != 200:
            print(f"      Pentasia page {page}: HTTP {r.status_code}")
            break
        html_text = r.text
        new = 0

        for j in _next_data_jobs(html_text, base):
            if j["url"] in seen: continue
            seen.add(j["url"]); out.append(j); new += 1
        if new and page == 0:
            print(f"      Pentasia: __NEXT_DATA__ gave {new} jobs on page 0")

        if not new:
            for j in _jsonld_jobs(url, "Pentasia"):
                if j["url"] in seen: continue
                seen.add(j["url"]); out.append(j); new += 1

        if not new:
            for u, t in _links_with_titles(html_text, base, "/careers/"):
                if u in seen: continue
                seen.add(u)
                slug = re.sub(r"-\d+-\d+$", "", u.rstrip("/").rsplit("/", 1)[-1])
                tail = slug.rsplit("-", 1)[-1] if "-" in slug else ""
                out.append({"title": t, "location": tail.title() if tail in _LOC_WORDS else "",
                            "department": "", "url": u, "posted_at": None})
                new += 1

        if page == 0 and not new:
            print(f"      Pentasia page 0: HTTP 200, {len(html_text)} bytes, "
                  f"{html_text.count('/careers/')} '/careers/' refs, "
                  f"{'__NEXT_DATA__ present' if '__NEXT_DATA__' in html_text else 'no __NEXT_DATA__'}, "
                  f"{html_text.count('<a ')} anchors")
        if not new:
            break
        time.sleep(REQUEST_DELAY)
    return out


def scrape_bettingjobs():
    """bettingjobs.com — WordPress + Applyflow, listing rendered client-side.
    Tries, in order: JSON-LD on the listing, the XML sitemap, candidate API
    endpoints, then sector-page link parsing."""
    base = "https://www.bettingjobs.com"
    print("   BettingJobs: strategy 1 — JSON-LD")
    jobs = _jsonld_jobs(base + "/jobs/", "BettingJobs")
    if jobs:
        return jobs
    print("   BettingJobs: strategy 2 — sitemap")
    urls = _sitemap_job_urls(base, "/job/")
    if urls:
        detailed = []
        for u in urls[:120]:
            detailed.extend(_jsonld_jobs(u, "BettingJobs"))
            time.sleep(REQUEST_DELAY)
            if len(detailed) >= 120:
                break
        if detailed:
            return detailed
        return _titles_from_urls(urls, "BettingJobs")
    print("   BettingJobs: strategy 3 — endpoint discovery")
    jobs = _discover(base, BETTINGJOBS_CANDIDATES, "BettingJobs")
    if jobs:
        return jobs
    print("   BettingJobs: strategy 4 — sector pages")
    print("   BettingJobs: no JSON endpoint found, falling back to sector pages")
    seen, res = set(), []
    sectors = ["hr-finance","marketing","executive-senior-appointments","it-technical",
               "analytics-bi","commercial","trading-sportsbook","operations",
               "compliance-legal","product"]
    base = "https://www.bettingjobs.com"
    for sec in sectors:
        try:
            r = session.get(f"{base}/{sec}/", headers=AGENCY_UA, timeout=TIMEOUT)
            if r.status_code != 200:
                print(f"      /{sec}/: HTTP {r.status_code}")
                continue
            hits = _links_with_titles(r.text, base, "/job/")
            if not hits and sec == sectors[0]:
                print(f"      /{sec}/: HTTP 200, {len(r.text)} bytes, "
                      f"{r.text.count('/job/')} '/job/' refs, {r.text.count('<a ')} anchors")
            for link, title in hits:
                if link in seen:
                    continue
                seen.add(link)
                res.append({"title": title, "location": "",
                            "department": sec.replace("-", " ").title(),
                            "url": link, "posted_at": None})
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"      /{sec}/: error ({type(e).__name__})")
            continue
    return res


def scrape_vankaizen():
    """vankaizen.com — bespoke board with load-more pagination."""
    base = "https://www.vankaizen.com"
    print("   Van Kaizen: strategy 1 — JSON-LD")
    jobs = _jsonld_jobs(base + "/vacancies", "Van Kaizen")
    if jobs:
        return jobs
    print("   Van Kaizen: strategy 2 — sitemap")
    urls = _sitemap_job_urls(base, "/vacanc")
    if urls:
        detailed = []
        for u in urls[:120]:
            detailed.extend(_jsonld_jobs(u, "Van Kaizen"))
            time.sleep(REQUEST_DELAY)
        if detailed:
            return detailed
        return _titles_from_urls(urls, "Van Kaizen")
    print("   Van Kaizen: strategy 3 — endpoint discovery")
    jobs = _discover(base, VANKAIZEN_CANDIDATES, "Van Kaizen")
    if jobs:
        return jobs
    print("   Van Kaizen: strategy 4 — page parse")
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


def _spa_api_hunt(base, source, max_bundles=4):
    """Single-page apps fetch their data from an API whose URL is baked into the
    JS bundle. Rather than guessing endpoint names, pull the bundle and read the
    URLs out of it. Works for any React/Vue careers app, not just this one."""
    try:
        r = session.get(base, headers=AGENCY_UA, timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"      {source}: shell HTTP {r.status_code}")
            return []
        html_text = r.text
    except Exception as e:
        print(f"      {source}: shell error ({type(e).__name__})")
        return []

    srcs = re.findall(r'<script[^>]+src="([^"]+)"', html_text)
    srcs = [u if u.startswith("http") else base.rstrip("/") + "/" + u.lstrip("/") for u in srcs]
    print(f"      {source}: {len(srcs)} script bundle(s) referenced")

    found = set()
    for u in srcs[:max_bundles]:
        try:
            b = session.get(u, headers=AGENCY_UA, timeout=TIMEOUT)
            if b.status_code != 200:
                continue
            txt = b.text[:4_000_000]
        except Exception:
            continue
        # absolute api urls, and relative api paths
        for m in re.findall(r'https?://[A-Za-z0-9._-]+/[A-Za-z0-9/_-]*api[A-Za-z0-9/_-]*', txt):
            found.add(m)
        for m in re.findall(r'"(/api/[^"\s]{2,80})"', txt):
            found.add(base.rstrip("/") + m)
        for m in re.findall(r'"(https?://[A-Za-z0-9._-]*azurewebsites\.net[^"]{0,80})"', txt):
            found.add(m)
    urls = sorted(found)[:25]
    if urls:
        print(f"      {source}: API-looking URLs in bundle:")
        for u in urls:
            print(f"         {u}")
    else:
        print(f"      {source}: no API URLs found in bundle")
    return urls


def scrape_fortuna():
    """Fortuna Entertainment Group run a bespoke 'Easy Apply' React app on Azure
    rather than a standard ATS. Hunt the API out of the JS bundle, then try the
    usual listing paths against whatever host it points at."""
    base = "https://app-azeun-p-hr-easyapply-fe.azurewebsites.net"
    print("   Fortuna: hunting API in the SPA bundle")
    hits = _spa_api_hunt(base, "Fortuna")

    # anything that already looks like a job listing endpoint, plus sensible guesses
    tries, seen = [], set()
    for u in hits:
        if re.search(r"(job|vacanc|position|offer|advert)", u, re.I):
            tries.append(u)
    roots = {base}
    for u in hits:
        m = re.match(r"(https?://[^/]+)", u)
        if m:
            roots.add(m.group(1))
    for root in roots:
        for path in ("/api/jobs", "/api/Jobs", "/api/vacancies", "/api/Vacancies",
                     "/api/positions", "/api/JobOffers", "/api/joboffers",
                     "/api/v1/jobs", "/api/adverts"):
            tries.append(root + path)
    for u in tries:
        if u in seen:
            continue
        seen.add(u)
        data = _try_json(u, u)
        if data:
            jobs = _normalise(data, base, "Fortuna")
            if jobs:
                print(f"   Fortuna: FOUND -> {u}  ({len(jobs)} jobs)")
                return jobs
        time.sleep(REQUEST_DELAY)
    return []


def scrape_betfred():
    """Betfred run TalosATS, a client-rendered careers app. Same approach as
    Fortuna: read the API URL out of the JS bundle rather than guessing."""
    base = "https://betfredgroup.talosats-careers.com"
    print("   Betfred: hunting API in the TalosATS bundle")
    hits = _spa_api_hunt(base, "Betfred")
    tries, seen = [], set()
    for u in hits:
        if re.search(r"(job|vacanc|position|advert)", u, re.I):
            tries.append(u)
    roots = {base} | {m.group(1) for u in hits
                      if (m := re.match(r"(https?://[^/]+)", u))}
    for root in roots:
        for path in ("/api/jobs", "/api/Jobs", "/api/vacancies", "/api/job/search",
                     "/api/jobs/search", "/api/v1/jobs", "/api/adverts"):
            tries.append(root + path)
    for u in tries:
        if u in seen:
            continue
        seen.add(u)
        data = _try_json(u, u)
        if data:
            jobs = _normalise(data, base, "Betfred")
            if jobs:
                print(f"   Betfred: FOUND -> {u}  ({len(jobs)} jobs)")
                return jobs
        time.sleep(REQUEST_DELAY)
    return []


def scrape_tabcorp():
    """Tabcorp's careers site is server-rendered, so the listing parses directly.
    (Their underlying ATS is PageUp, but the public site is easier to read.)"""
    base = "https://careers.tabcorp.com.au"
    out, seen = [], set()
    for page in range(1, 8):
        url = f"{base}/jobs/search?page={page}&query="
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        except Exception as e:
            print(f"      Tabcorp page {page}: error ({type(e).__name__})")
            break
        if r.status_code != 200:
            print(f"      Tabcorp page {page}: HTTP {r.status_code}")
            break
        new = 0
        for u, t in _links_with_titles(r.text, base, "/jobs/"):
            if u in seen or u.endswith("/jobs/search"):
                continue
            seen.add(u)
            out.append({"title": t, "location": "", "department": "",
                        "url": u, "posted_at": None})
            new += 1
        if page == 1 and not new:
            print(f"      Tabcorp: HTTP 200, {len(r.text)} bytes, "
                  f"{r.text.count('/jobs/')} '/jobs/' refs, no titles parsed")
        if not new:
            break
        time.sleep(REQUEST_DELAY)
    return out


# Sites with no supported ATS behind them. Rather than a bespoke scraper each,
# one routine walks the same ladder: JSON-LD -> sitemap -> SPA bundle API ->
# tolerant link parsing. Config is just a base URL and the path job links share.
CUSTOM_BOARDS = {
    "Cirsa":        dict(base="https://joblink.allibo.com", marker="job-offer",
                         listing=["/ats2/job-offer.aspx", "/ats2/"]),
    "Codere Online":dict(base="https://codere.hiringroom.com", marker="/jobs/",
                         listing=["/jobs/"], extra=["https://codereargentina.hiringroom.com/jobs/"]),
    "Casumo":       dict(base="https://www.casumocareers.com", marker="/jobs/",
                         listing=["/jobs/"]),
    "Betika":       dict(base="https://betika.seamlesshiring.com", marker="/h/",
                         listing=["/h/"]),
    "PawaTech":     dict(base="https://careers.pawatech.com", marker="/job/",
                         listing=["/job/", "/jobs/"]),
    "Lucky Group":  dict(base="https://careers.lckygroup.com", marker="/jobs/",
                         listing=["/jobs/"]),
    "bet9ja":       dict(base="https://bet9jacareers.com", marker="/JobApplications/",
                         listing=["/JobApplications/Apply/", "/"]),
}


def scrape_custom(name):
    """Try, in order: JSON-LD on the listing, the XML sitemap, the SPA bundle's
    API, then tolerant link parsing. Logs which rung worked."""
    cfg = CUSTOM_BOARDS[name]
    base, marker = cfg["base"], cfg["marker"]
    pages = [base.rstrip("/") + p for p in cfg["listing"]] + cfg.get("extra", [])

    for url in pages:
        ld = _jsonld_jobs(url, name)
        if ld:
            print(f"   {name}: JSON-LD on {url}")
            return ld

    urls = _sitemap_job_urls(base, marker)
    if urls:
        detailed = []
        for u in urls[:80]:
            detailed.extend(_jsonld_jobs(u, name))
            time.sleep(REQUEST_DELAY)
            if len(detailed) >= 80:
                break
        if detailed:
            print(f"   {name}: sitemap + JSON-LD -> {len(detailed)}")
            return detailed
        derived = _titles_from_urls(urls, name)
        if derived:
            print(f"   {name}: sitemap slugs -> {len(derived)}")
            return derived

    hits = _spa_api_hunt(base, name)
    for u in hits:
        if re.search(r"(job|vacanc|position|advert|offer)", u, re.I):
            data = _try_json(u, u)
            if data:
                jobs = _normalise(data, base, name)
                if jobs:
                    print(f"   {name}: bundle API {u} -> {len(jobs)}")
                    return jobs
            time.sleep(REQUEST_DELAY)

    out, seen = [], set()
    for url in pages:
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
            if r.status_code != 200:
                print(f"      {name} {url}: HTTP {r.status_code}")
                continue
            found = _links_with_titles(r.text, base, marker)
            if not found:
                print(f"      {name} {url}: 200, {len(r.text)} bytes, "
                      f"{r.text.count(marker)} '{marker}' refs, no titles parsed")
            for u, t in found:
                if u in seen:
                    continue
                seen.add(u)
                out.append({"title": t, "location": "", "department": "",
                            "url": u, "posted_at": None})
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"      {name} {url}: error ({type(e).__name__})")
    if out:
        print(f"   {name}: link parse -> {len(out)}")
    return out


AGENCY_BOARDS = {
    "Fortuna Entertainment Group": scrape_fortuna,
    **{n: (lambda n=n: scrape_custom(n)) for n in CUSTOM_BOARDS},
    "Betfred": scrape_betfred,
    "Tabcorp": scrape_tabcorp,
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
