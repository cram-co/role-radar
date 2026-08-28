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

import contextlib
import csv
import html
import io
import json
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from collections import Counter

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

# tokens too generic to be anyone's unique board id
_GENERIC_SLUGS = {
    "new", "the", "all", "one", "our", "job", "jobs", "pro", "max", "top", "key",
    "big", "get", "now", "web", "app", "inc", "ltd", "plc", "group", "global",
    "digital", "online", "gaming", "games", "sports", "media", "tech", "data",
    "play", "bet", "win", "live", "next", "first", "prime", "core", "edge",
    "strive", "blip", "spark", "pulse", "nova", "apex", "vega", "orbit",
}


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
        # Reject short or generic tokens outright. A three-letter slug like
        # "new", "sts" or "pro" will almost always belong to someone else, and a
        # false match is far more damaging than a miss — it silently fills the
        # feed with another industry's jobs.
        if not c or c in cands:
            continue
        if len(c) < 5 or c in _GENERIC_SLUGS:
            continue
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


# A whole-run ceiling on time lost to rate limiting. Without this the retry sits
# inside the pagination loop, so waits multiply: attempts x pages x companies.
_RATE_LIMIT_BUDGET = 180.0        # seconds, across the entire run
_rate_limit_spent = 0.0

# Detection probes are speculative; fetches are known-good pins. Left unchecked
# the probes eat the whole budget first — a run spent it all on bodog, bovada,
# racing1 and inplaysoft, so the six real Workable companies got nothing. Once a
# host rate-limits us this many times during detection, stop probing it.
_HOST_429_LIMIT = 2
_host_429 = {}


def _host_of(url):
    m = re.match(r"https?://([^/]+)", url or "")
    return m.group(1) if m else ""


def _host_is_throttled(url):
    return _host_429.get(_host_of(url), 0) >= _HOST_429_LIMIT


# Workable throttles hard when several companies are pulled back to back. The
# retry helper alone just burns the budget losing the same requests, so pace the
# requests instead: never hit the same host more often than this.
_HOST_MIN_GAP = {"apply.workable.com": 5.0}
_host_last = {}


def _pace(url):
    host = _host_of(url)
    gap = _HOST_MIN_GAP.get(host)
    if not gap:
        return
    last = _host_last.get(host)
    if last is not None:
        wait = gap - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
    _host_last[host] = time.time()


class _Throttled:
    """Stands in for a response when we skip a host we know is rate-limiting."""
    status_code = 429
    headers = {}

    def json(self):
        return {}


def _request(method, url, tries=3, probing=False, **kw):
    """Wrap a request so a 429 backs off and retries instead of losing the board.

    Adding two more Workable companies pushed the run past their rate limit and
    every Workable company returned zero at once — including several that had
    been working. Honours Retry-After when the server sends it."""
    global _rate_limit_spent
    if probing and _host_is_throttled(url):
        return _Throttled()
    _pace(url)
    delay = 2.0
    for attempt in range(tries):
        try:
            r = session.request(method, url, timeout=TIMEOUT, **kw)
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(delay)
            delay *= 2
            continue
        if r.status_code != 429:
            return r
        h = _host_of(url)
        _host_429[h] = _host_429.get(h, 0) + 1
        wait = delay
        ra = r.headers.get("Retry-After")
        if ra:
            try:
                wait = float(ra)
            except ValueError:
                pass
        wait = min(wait, 15)
        if attempt == tries - 1 or _rate_limit_spent + wait > _RATE_LIMIT_BUDGET:
            if _rate_limit_spent + wait > _RATE_LIMIT_BUDGET:
                print(f"      rate-limit budget spent, giving up on {url.split('?')[0]}")
            else:
                print(f"      rate limited after {tries} attempts: {url.split('?')[0]}")
            return r
        print(f"      429 from {url.split('//')[-1].split('/')[0]}, waiting {wait:.0f}s")
        time.sleep(wait)
        _rate_limit_spent += wait
        delay *= 2
    return r


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
    """SAP SuccessFactors career sites.

    The listing is rendered client-side through a DWR (Direct Web Remoting)
    call, which is why the old sitemap and HTML-scraping approach found 180KB
    and zero jobs. Two steps:

      1. GET /career?company=X — establishes a session cookie and returns a page
         carrying an _s.crb token.
      2. POST that token as x-ajax-token / x-csrf-token to
         careerJobSearchControllerProxy.getInitialJobSearchData.dwr

    The reply is JavaScript rather than JSON, assigning properties onto numbered
    objects (s37.title="..."), so it's parsed by grouping those assignments.

    Token is 'company' or 'host|company'."""
    hosts, company = _sf_parts(token)
    for host in hosts:
        out = _sf_try_host(host, company)
        if out:
            return out
    return []


_SF_CRB = re.compile(r"_s\.crb=([^\"'&\\\s]+)")


def _dwr_script_session_id(sess, base, page):
    """Ask DWR's __System.generateId for a server-registered scriptSessionId.

    The reply is JavaScript of the form
        dwr.engine.remote.handleCallback("0","0","B0BEA1F...");
    so the id is the third quoted argument."""
    url = f"{base}/xi/ajax/remoting/call/plaincall/__System.generateId.dwr"
    body = "\n".join([
        "callCount=1",
        f"page={page}",
        "httpSessionId=",
        "scriptSessionId=",
        "c0-scriptName=__System",
        "c0-methodName=generateId",
        "c0-id=0",
        "batchId=0",
        "",
    ])
    try:
        r = sess.post(url, data=body.encode("utf-8"), timeout=TIMEOUT, headers={
            **AGENCY_UA, "Accept": "*/*", "Content-Type": "text/plain",
            "Origin": base, "Referer": base + page,
        })
    except Exception as e:
        print(f"      sf: generateId {type(e).__name__}")
        return None
    if r.status_code != 200:
        print(f"      sf: generateId HTTP {r.status_code}")
        return None
    m = re.search(r'handleCallback\(\s*"[^"]*"\s*,\s*"[^"]*"\s*,\s*"([^"]+)"', r.text)
    if not m:
        m = re.search(r'"([A-Za-z0-9_/+-]{16,64})"\s*\)\s*;', r.text)
    if m:
        print(f"      sf: generateId issued {m.group(1)[:14]}...")
        return m.group(1)
    print(f"      sf: generateId 200 but no id parsed from {len(r.text)} bytes: "
          f"{re.sub(chr(92)+'s+', ' ', r.text)[:120]!r}")
    return None


def _sf_try_host(host, company):
    """Uses its OWN session, not the shared one.

    The shared session accumulates cookies from every board in the run — by the
    time SuccessFactors is reached it holds a JSESSIONID that may belong to
    Workday, plus thirty others. SAP replies "you are not authorized to access
    the functionality you have requested", so a clean jar removes the most
    likely cause before anything more elaborate is tried."""
    sf = requests.Session()
    base = f"https://{host}"
    listing = f"{base}/career?company={company}"
    try:
        r = sf.get(listing, headers=AGENCY_UA, timeout=TIMEOUT)
    except Exception as e:
        print(f"      sf {company} {host}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      sf {company} {host}: HTTP {r.status_code} on the career page")
        return []

    m = _SF_CRB.search(r.text)
    if not m:
        print(f"      sf {company} {host}: no _s.crb token in {len(r.text)} bytes")
        return []
    crb = m.group(1)

    page = (f"/career?company={company}&career%5Fns=job%5Flisting%5Fsummary"
            f"&navBarLevel=JOB%5FSEARCH&_s.crb={crb}")
    # load the listing page itself before calling its backend
    try:
        rl = sf.get(base + page, headers={**AGENCY_UA, "Referer": listing},
                         timeout=TIMEOUT)
        m2 = _SF_CRB.search(rl.text)
        if m2:
            crb = m2.group(1)          # the listing page may issue a fresher token
            page = (f"/career?company={company}&career%5Fns=job%5Flisting%5Fsummary"
                    f"&navBarLevel=JOB%5FSEARCH&_s.crb={crb}")
    except Exception:
        pass
    # DWR does not let the client invent a scriptSessionId — the server issues
    # one through __System.generateId, and calls carrying an unregistered id are
    # refused. "You are not authorized to access the functionality you have
    # requested" is how DWR words that refusal, which is what we kept hitting
    # with a random hex string. Ask for a real one first.
    ssid = _dwr_script_session_id(sf, base, page) or uuid.uuid4().hex.upper()[:24]
    body = "\n".join([
        "callCount=1",
        f"page={page}",
        "httpSessionId=",
        f"scriptSessionId={ssid}",
        "c0-scriptName=careerJobSearchControllerProxy",
        "c0-methodName=getInitialJobSearchData",
        "c0-id=0",
        "c0-e1=string:",
        "c0-e2=string:",
        "c0-e3=string:",
        "c0-e4=string:Europe%2FLondon",
        ("c0-param0=Object_Object:{filterOnly:reference:c0-e1, "
         "jobAlertId:reference:c0-e2, returnToList:reference:c0-e3, "
         "browserTimeZone:reference:c0-e4}"),
        "batchId=0",
        "",
    ])
    url = (f"{base}/xi/ajax/remoting/call/plaincall/"
           f"careerJobSearchControllerProxy.getInitialJobSearchData.dwr")
    have = sorted(sf.cookies.get_dict().keys())
    if "JSESSIONID" not in have:
        print(f"      sf {company} {host}: no JSESSIONID after the page loads "
              f"(cookies: {have}) — the session was never established")
    try:
        rp = sf.post(url, data=body.encode("utf-8"), timeout=TIMEOUT, headers={
            **AGENCY_UA,
            "Accept": "*/*",
            "Content-Type": "text/plain",
            "Origin": base,
            "Referer": base + page,
            "x-ajax-token": crb,
            "x-csrf-token": crb,
            "x-sap-page-info": f"companyId={company}",
            "x-subaction": "0",
            "viewid": "/ui/rcmcareer/pages/careersite/career.jsp.xhtml",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        })
    except Exception as e:
        print(f"      sf {company} {host}: DWR {type(e).__name__}")
        return []
    if rp.status_code != 200:
        print(f"      sf {company} {host}: DWR HTTP {rp.status_code}")
        return []

    jobs = _sf_parse_dwr(rp.text, base, company)
    if jobs:
        print(f"      sf {company} {host}: {len(jobs)} roles")
    else:
        print(f"      sf {company} {host}: DWR 200, {len(rp.text)} bytes but no titles parsed")
        if "<html" in rp.text[:400].lower():
            # an HTML page rather than a DWR reply — the useful part is its
            # title and any visible message, not the doctype
            t = re.search(r"<title[^>]*>(.*?)</title>", rp.text, re.S | re.I)
            body_txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", rp.text))
            print(f"         got an HTML page, title={ (t.group(1).strip() if t else '?')!r}")
            print(f"         page text: {body_txt.strip()[:200]!r}")
            print(f"         cookies held: {sorted(sf.cookies.get_dict().keys())}")
        else:
            print(f"         reply began: {re.sub(chr(92)+'s+', ' ', rp.text)[:200]!r}")
    return jobs


def _sf_parse_dwr(text, base, company):
    """DWR replies assign onto numbered objects: s37.title="Product Owner".
    Group the assignments by object, then keep those that look like a posting."""
    objs = {}
    for idx, key, val in re.findall(
            r"\bs(\d+)\.([A-Za-z_][\w]*)\s*=\s*(\"(?:[^\"\\\\]|\\\\.)*\"|[^;\n]+);",
            text):
        v = val.strip()
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1]
            v = v.encode("utf-8").decode("unicode_escape", "ignore")
        objs.setdefault(idx, {})[key] = v.strip()

    out, seen = [], set()
    for o in objs.values():
        title = html.unescape(str(o.get("title") or "")).strip()
        if not title or len(title) < 3 or len(title) > 160:
            continue
        if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
            continue
        jid = (o.get("jobReqId") or o.get("jobId") or o.get("id") or "").strip().strip('"')
        if not re.fullmatch(r"\d{2,}", jid or ""):
            continue
        if jid in seen:
            continue
        seen.add(jid)
        loc = ""
        for k in ("location", "locationName", "city", "geozoneDescription", "country"):
            if o.get(k):
                loc = html.unescape(str(o[k])).strip().strip('[]"')
                break
        out.append({
            "title": title,
            "location": loc,
            "department": html.unescape(str(o.get("department") or "")).strip(),
            "url": f"{base}/career?company={company}&career_job_req_id={jid}",
            "posted_at": o.get("postedDate") or o.get("jobStartDate") or None,
        })
    return out

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
    # No speculative probing of Workable. Their account slugs aren't derivable
    # from a company name — "openbet-1", "entaingroup", "rhino-entertainment" —
    # so guessing almost never lands, while every attempt is paced at 12s and
    # counts against a limit the real fetches need. 80 probes a run was adding
    # 16 minutes for nothing. Workable companies are pinned in companies.csv.
    return None
    r = _request("POST", f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
                 json={"query": "", "location": [], "department": []},
                 headers={**AGENCY_UA, "Accept": "application/json",
                          "Content-Type": "application/json",
                          "Origin": "https://apply.workable.com",
                          "Referer": f"https://apply.workable.com/{slug}/"}, probing=True)
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

def _json_list(url, label, key=None, **kw):
    """GET a board's JSON and hand back a list, or [] with a reason logged.

    The original seven fetchers (greenhouse, bamboohr, lever, ashby,
    smartrecruiters, recruitee, workable) called .json() straight off the
    response with no status check, so any 404, HTML error page or odd payload
    raised. They were caught per-company so a run survived, but the log said
    only "FAILED" instead of naming the problem — and they are our
    highest-volume platforms."""
    try:
        r = _request("GET", url, **kw)
    except Exception as e:
        print(f"      {label}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      {label}: HTTP {r.status_code}")
        return []
    try:
        d = r.json()
    except Exception:
        print(f"      {label}: 200 but not JSON")
        return []
    if key:
        if not isinstance(d, dict):
            print(f"      {label}: 200 but payload is {type(d).__name__}")
            return []
        d = d.get(key)
    if d is None:
        return []
    if not isinstance(d, list):
        print(f"      {label}: expected a list, got {type(d).__name__}")
        return []
    return [x for x in d if isinstance(x, dict)]


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
    try:
        d = _greenhouse_payload(token)
    except Exception as e:
        print(f"      greenhouse {token}: {type(e).__name__}")
        return []
    if not isinstance(d, dict):
        print(f"      greenhouse {token}: unexpected payload {type(d).__name__}")
        return []
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
    items = _json_list(f"https://{token}.bamboohr.com/careers/list",
                       f"bamboohr {token}", key="result")
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


def _ultipro_loc(j):
    """UKG's payload has shifted between tenants: some send Locations[], some a
    flat Location object, some plain City/State/Country fields. Read whichever
    is present rather than assuming one shape."""
    def name(v):
        if isinstance(v, dict):
            return (v.get("Name") or v.get("name") or v.get("Description")
                    or v.get("Code") or v.get("code") or "")
        return v if isinstance(v, str) else ""

    src = None
    # Bally's send Locations: [] alongside a populated MatchedLocations — the
    # diagnostic named both keys, and reading only the first gave 436 roles
    # with no location at all
    for key in ("Locations", "locations", "MatchedLocations", "matchedLocations"):
        v = j.get(key)
        if isinstance(v, list) and v:
            src = v[0] if isinstance(v[0], dict) else {"City": v[0]}
            break
    if src is None:
        for key in ("Location", "location", "PrimaryLocation", "JobLocation"):
            v = j.get(key)
            if isinstance(v, dict):
                src = v
                break
            if isinstance(v, str) and v.strip():
                return html.unescape(v).strip()
    if src is None:
        src = j                                   # flat fields on the record

    # UKG nest the real fields under Address on some tenants
    for wrap in ("Address", "address", "LocationAddress"):
        if isinstance(src.get(wrap), dict):
            src = {**src, **src[wrap]}
            break

    parts = [name(src.get(k)) for k in
             ("City", "city", "Municipality", "State", "state", "StateProvince",
              "Country", "country")]
    seen, bits = set(), []
    for x in parts:
        x = (x or "").strip()
        if x and x.lower() not in seen:
            seen.add(x.lower())
            bits.append(x)
    out = ", ".join(bits)
    if not out:
        for k in ("Address", "FormattedAddress", "LocationName", "WorkLocation"):
            v = name(src.get(k)) or name(j.get(k))
            if v:
                return html.unescape(v).strip()
    return html.unescape(out).strip()


def fetch_ultipro(token):
    """UKG/UltiPro job board. Token is "companyId|boardGuid" — both appear in the
    board URL: recruiting.ultipro.com/<companyId>/JobBoard/<boardGuid>/..."""
    if "|" not in token:
        print(f"      ultipro {token}: need companyId|boardGuid")
        return []
    company, board = token.split("|", 1)
    base = f"https://recruiting.ultipro.com/{company}/JobBoard/{board}"
    out, page = [], 1
    for _ in range(10):
        try:
            r = session.post(
                f"{base}/JobBoardView/LoadSearchResults",
                json={"opportunitySearch": {"Top": 100, "Skip": (page - 1) * 100,
                                            "QueryString": "", "OrderBy": [], "Filters": []},
                      "matchCriteria": {"PreferredJobs": [], "Educations": [], "LicenseAndCertifications": [],
                                        "Skills": [], "hasNoLicenses": False, "SkippedSkills": []}},
                headers={**AGENCY_UA, "Content-Type": "application/json",
                         "Accept": "application/json"},
                timeout=TIMEOUT,
            )
            if r.status_code != 200:
                print(f"      ultipro {company}: HTTP {r.status_code}")
                break
            d = r.json()
        except Exception as e:
            print(f"      ultipro {company}: {type(e).__name__}")
            break
        if isinstance(d, list):
            items = d
        elif isinstance(d, dict):
            items = (d.get("opportunities") or d.get("Opportunities") or [])
        else:
            print(f"      ultipro {company}: 200 but payload is {type(d).__name__}")
            break
        if not items:
            break
        for j in items:
            if not isinstance(j, dict):
                continue
            title = j.get("Title") or j.get("title") or ""
            if not title:
                continue
            loc = _ultipro_loc(j)
            if not loc and not out:
                # Naming the top-level keys wasn't enough: Bally's have both
                # Locations and MatchedLocations and NEITHER yielded anything,
                # so print what is actually inside them.
                shape = {}
                for k in ("Locations", "MatchedLocations", "Location",
                          "JobLocationType", "City", "Country"):
                    if k in j:
                        v = j[k]
                        shape[k] = (f"list[{len(v)}] {str(v[:1])[:160]}"
                                    if isinstance(v, list) else str(v)[:120])
                print(f"      ultipro {company}: no location parsed")
                for k, v in shape.items():
                    print(f"         {k} = {v}")
            out.append({
                "title": html.unescape(str(title)).strip(),
                "location": loc,
                "department": "",
                "url": f"{base}/OpportunityDetail?opportunityId={j.get('Id') or j.get('id','')}",
                "posted_at": j.get("PostedDate") or j.get("postedDate"),
            })
        if len(items) < 100:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


def fetch_icims(token):
    """iCIMS careers portal. Token is the subdomain, e.g. "careers-gamesglobal".
    No public JSON API, so the search pages are parsed for /jobs/<id>/<slug> links."""
    base = f"https://{token}.icims.com"
    out, seen = [], set()
    for page in range(1, 11):
        paths = [f"/jobs/search?ss=1&in_iframe=1&pr={page - 1}",
                 f"/jobs/search?pr={page - 1}",
                 f"/jobs/search?ss=1&searchRelation=keyword_all&pr={page - 1}",
                 f"/jobs?pr={page - 1}",
                 f"/jobs/search?ss=1&hashed=-625966102&mobile=false&width=1200&pr={page - 1}",
                 f"/search?pr={page - 1}"]
        url = base + paths[0]
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        except Exception as e:
            print(f"      icims {token}: {type(e).__name__}")
            break
        if r.status_code != 200:
            print(f"      icims {token}: HTTP {r.status_code}")
            break
        if len(r.text) < 800 and page == 1:
            for alt in paths[1:]:
                try:
                    r2 = session.get(base + alt, headers=AGENCY_UA, timeout=TIMEOUT)
                    if r2.status_code == 200 and len(r2.text) > len(r.text):
                        print(f"      icims {token}: using {alt} ({len(r2.text)} bytes)")
                        r = r2
                        break
                except Exception:
                    continue
                time.sleep(REQUEST_DELAY)
        hits = _links_with_titles(r.text, base, "/jobs/")
        new = 0
        for u, t in hits:
            if u in seen or re.search(r"/jobs/search", u):
                continue
            seen.add(u)
            out.append({"title": t, "location": "", "department": "",
                        "url": u, "posted_at": None})
            new += 1
        if page == 1 and not new:
            print(f"      icims {token}: 200, {len(r.text)} bytes")
            print(f"         link shapes: {_href_shapes(r.text)}")
            urls = _sitemap_job_urls(base, "/jobs/")
            if urls:
                derived = _titles_from_urls(urls, f"icims:{token}")
                if derived:
                    print(f"      icims {token}: sitemap -> {len(derived)}")
                    return derived
        if not new:
            break
        time.sleep(REQUEST_DELAY)
    return out


def fetch_oracle(token):
    """Oracle HCM Cloud (Fusion) recruiting. Token is "host|siteNumber", both
    visible in a job URL: https://<host>/hcmUI/CandidateExperience/en/sites/<site>/job/123
    Oracle exposes a public REST endpoint that the career site itself calls."""
    if "|" not in token:
        print(f"      oracle {token}: need host|siteNumber")
        return []
    host, site = token.split("|", 1)
    base = f"https://{host}"
    api = f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    out, offset = [], 0
    for _ in range(10):
        url = (f"{api}?onlyData=true&expand=requisitionList.secondaryLocations"
               f"&finder=findReqs;siteNumber={site},limit=200,offset={offset},"
               f"sortBy=POSTING_DATES_DESC")
        try:
            r = session.get(url, headers={**AGENCY_UA, "Accept": "application/json"},
                            timeout=TIMEOUT)
            if r.status_code != 200:
                print(f"      oracle {site}: HTTP {r.status_code}")
                break
            d = r.json()
        except Exception as e:
            print(f"      oracle {site}: {type(e).__name__}")
            break
        if not isinstance(d, dict):
            print(f"      oracle {site}: 200 but payload is {type(d).__name__}")
            break
        items = d.get("items") or []
        reqs = []
        for it in items:
            if isinstance(it, dict):
                reqs.extend(it.get("requisitionList") or [])
        if not reqs:
            break
        for j in reqs:
            if not isinstance(j, dict):
                continue
            title = html.unescape(str(j.get("Title") or "")).strip()
            if not title:
                continue
            loc = j.get("PrimaryLocation") or j.get("Location") or ""
            out.append({
                "title": title,
                "location": html.unescape(str(loc)).strip(),
                "department": str(j.get("JobFamily") or ""),
                "url": f"{base}/hcmUI/CandidateExperience/en/sites/{site}/job/{j.get('Id','')}",
                "posted_at": j.get("PostedDate") or j.get("PostingStartDate"),
            })
        if len(reqs) < 200:
            break
        offset += 200
        time.sleep(REQUEST_DELAY)
    return out


def fetch_freshteam(token):
    """Freshworks Freshteam board at https://<token>.freshteam.com/jobs.
    Server-rendered, but each anchor carries the full job blurb, so the title is
    taken from the URL slug (clean and deterministic) rather than the link text.
    The location is read off the tail of the anchor, which ends "City, Region Type"."""
    base = f"https://{token}.freshteam.com"
    try:
        r = session.get(f"{base}/jobs", headers=AGENCY_UA, timeout=TIMEOUT)
    except Exception as e:
        print(f"      freshteam {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      freshteam {token}: HTTP {r.status_code}")
        return []
    out, seen = [], set()
    for url, text in _links_with_titles(r.text, base, "/jobs/"):
        if url in seen or url.rstrip("/").endswith("/jobs"):
            continue
        seen.add(url)
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        words = [w for w in re.split(r"[-_]+", slug) if w]
        if not words:
            continue
        title = " ".join(_FIX_CASE.get(w.title(), w.title()) for w in words)
        loc = ""
        # no dots in the class, or the trailing "..." of the blurb gets swallowed
        m = re.search(r"([A-Z][A-Za-z '-]{2,24},\s*[A-Z][A-Za-z '-]{2,24})\s*"
                      r"(?:Full Time|Part Time|Contract|Internship|Freelance)\s*$", text)
        if m:
            loc = m.group(1).strip()
        out.append({"title": title, "location": loc, "department": "",
                    "url": url, "posted_at": None})
    if not out:
        print(f"      freshteam {token}: listing had no job links, trying the sitemap")
        urls = _sitemap_job_urls(base, "/jobs/")
        if urls:
            out = _titles_from_urls(urls, f"freshteam:{token}")
            print(f"      freshteam {token}: sitemap -> {len(out)}")
    return out


def fetch_jobvite(token):
    """Jobvite careers portal.

    The listing is at jobs.jobvite.com/<token>/JOBS, not the bare token URL —
    that missing segment is why AGS silently returned nothing for weeks. The
    page is server-rendered as tables grouped by category, with the location in
    the second column. Categories holding more than 20 roles show a "Show More"
    link to /search?c=<category>&p=N, which is followed and paged."""
    base = f"https://jobs.jobvite.com/{token}"
    out, seen = [], set()

    row_rx = re.compile(
        r'href="([^"]*?/job/[^"?#]+)"[^>]*>(.*?)</a>\s*</td>\s*<td[^>]*>(.*?)</td>',
        re.S | re.I)
    link_rx = re.compile(r'href="([^"]*?/job/[^"?#]+)"[^>]*>(.*?)</a>', re.S | re.I)

    def clean(v):
        return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", v or ""))).strip()

    def harvest(text):
        added = 0
        # preferred: title and location from the same table row
        for href, title_html, loc_html in row_rx.findall(text):
            title, loc = clean(title_html), clean(loc_html)
            if not title or len(title) < 3 or len(title) > 140:
                continue
            if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
                continue
            u = href if href.startswith("http") else "https://jobs.jobvite.com" + href
            if u in seen:
                continue
            seen.add(u)
            out.append({"title": title, "location": loc, "department": "",
                        "url": u, "posted_at": None})
            added += 1
        # fallback for any link the row pattern missed
        for href, title_html in link_rx.findall(text):
            title = clean(title_html)
            if not title or len(title) < 3 or len(title) > 140:
                continue
            if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
                continue
            u = href if href.startswith("http") else "https://jobs.jobvite.com" + href
            if u in seen:
                continue
            seen.add(u)
            out.append({"title": title, "location": "", "department": "",
                        "url": u, "posted_at": None})
            added += 1
        return added

    try:
        r = _request("GET", f"{base}/jobs", headers=AGENCY_UA)
    except Exception as e:
        print(f"      jobvite {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      jobvite {token}: HTTP {r.status_code}")
        return []
    harvest(r.text)

    # categories capped at 20 link out to a paged search
    more = set(re.findall(r'href="([^"]*?/search\?c=[^"]+)"', r.text, re.I))
    for link in sorted(more):
        cat = re.sub(r".*[?&]c=([^&]*).*", r"\1", link)
        for page in range(20):
            u = f"https://jobs.jobvite.com{link}" if link.startswith("/") else link
            u = re.sub(r"[?&]p=\d+", "", u) + f"&p={page}"
            try:
                rp = _request("GET", u, headers=AGENCY_UA)
            except Exception:
                break
            if rp.status_code != 200:
                break
            if not harvest(rp.text):
                break
            time.sleep(REQUEST_DELAY)

    if out:
        print(f"      jobvite {token}: {len(out)} roles across {len(more) + 1} listings")
    else:
        print(f"      jobvite {token}: 200, {len(r.text)} bytes but no /job/ links")
        print(f"         link shapes: {_href_shapes(r.text)}")
    return out

def fetch_betterteam(token):
    """Betterteam board at <token>.betterteam.com — a simple server-rendered list."""
    base = f"https://{token}.betterteam.com"
    try:
        r = session.get(base, headers=AGENCY_UA, timeout=TIMEOUT)
    except Exception as e:
        print(f"      betterteam {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      betterteam {token}: HTTP {r.status_code}")
        return []
    out, seen = [], set()
    for marker in ("/job/", "/jobs/", "/careers/"):
        for url, title in _links_with_titles(r.text, base, marker):
            if url in seen or url.rstrip("/").endswith(("/job", "/jobs")):
                continue
            seen.add(url)
            out.append({"title": title, "location": "", "department": "",
                        "url": url, "posted_at": None})
        if out:
            break
    if not out:
        print(f"      betterteam {token}: 200, {len(r.text)} bytes")
        print(f"         link shapes: {_href_shapes(r.text)}")
    return out


def fetch_rippling(token):
    """Rippling ATS board at ats.rippling.com/<token>/jobs."""
    base = "https://ats.rippling.com"
    for path in (f"/{token}/jobs", f"/en-GB/{token}/jobs", f"/en-US/{token}/jobs"):
        try:
            r = session.get(base + path, headers=AGENCY_UA, timeout=TIMEOUT)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        out, seen = [], set()
        for url, title in _links_with_titles(r.text, base, "/jobs/"):
            if url in seen or url.rstrip("/").endswith("/jobs"):
                continue
            seen.add(url)
            out.append({"title": title, "location": "", "department": "",
                        "url": url, "posted_at": None})
        if out:
            return out
        print(f"      rippling {token}{path}: 200, {len(r.text)} bytes")
        print(f"         link shapes: {_href_shapes(r.text)}")
    return []


def fetch_wordpress(token):
    """WordPress sites that run a jobs plugin expose it through the standard WP
    REST API. Token is the host. Huddle's filter labels (Departments, Locations,
    Employment Types) are the WP Job Openings defaults, so that post type is
    tried first, then the other common ones."""
    # token may name the post type: "jobs.888.com|888-position"
    token, _, custom_type = token.partition("|")
    base = f"https://{token}" if not token.startswith("http") else token
    # WordPress serves a post type at its REST_BASE, not its slug, and the two
    # often differ: WP Job Manager registers the type "job_listing" but exposes
    # it at /wp/v2/JOB-LISTINGS. Asking for the slug 404s. Inspired sat at zero
    # for exactly this reason. The rest_base forms are tried first.
    TYPES = ["job-listings", "awsm_job_openings", "job-openings", "job_listing",
             "jobs", "job", "career", "careers", "vacancy", "vacancies",
             "position", "positions", "open-roles", "job-postings"]
    if custom_type.strip():
        TYPES = [custom_type.strip()] + [t for t in TYPES if t != custom_type.strip()]
    for t in TYPES:
        items, page = [], 1
        while page <= 10:
            url = f"{base}/wp-json/wp/v2/{t}?per_page=100&page={page}&_embed=1"
            try:
                r = _request("GET", url, headers={**AGENCY_UA, "Accept": "application/json"})
            except Exception as e:
                # the host is unreachable, not just missing this post type —
                # trying nine more costs ~50s each on timeouts and retries.
                # Huddle spent 511s this way for nothing.
                print(f"      wordpress {token}: {type(e).__name__} on {t}, "
                      f"host unreachable — skipping the remaining types")
                return []
            if r.status_code in (429, 500, 502, 503, 504) and page == 1:
                print(f"      wordpress {token}: HTTP {r.status_code} on {t}, "
                      f"host is struggling — skipping the remaining types")
                return []
            if r.status_code != 200:
                break
            try:
                batch = r.json()
            except Exception:
                break
            if not isinstance(batch, list) or not batch:
                break
            items.extend(batch)
            if len(batch) < 100:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        if not items:
            continue
        out = []
        for j in items:
            if not isinstance(j, dict):
                continue
            t = j.get("title")
            t = t.get("rendered") if isinstance(t, dict) else t
            title = html.unescape(str(t or "")).strip()
            if not title:
                continue
            # taxonomy terms arrive under _embedded when _embed=1 is asked for
            loc = ""
            for group in ((j.get("_embedded") or {}).get("wp:term") or []):
                for term in group or []:
                    tax = str(term.get("taxonomy") or "")
                    if any(k in tax for k in ("location", "city", "country",
                                              "region", "office", "site")):
                        loc = html.unescape(str(term.get("name") or ""))
                        break
                if loc:
                    break
            # the title often carries a truer location than the field
            loc = _loc_from_title(title, loc)
            out.append({
                "title": title,
                "location": loc,
                "department": "",
                "url": j.get("link") or f"{base}/?p={j.get('id','')}",
                "posted_at": j.get("date_gmt") or j.get("date"),
            })
        if out:
            print(f"      wordpress {token}: {t} -> {len(out)}")
            return out
    print(f"      wordpress {token}: no job post type found among {len(TYPES)} tried")
    return []


def _flatten_loc(v):
    """Locations arrive as a string, a dict, or a list of either."""
    if v is None:
        return ""
    if isinstance(v, list):
        parts = [_flatten_loc(x) for x in v]
        return "; ".join(p for p in parts if p)
    if isinstance(v, dict):
        bits = [v.get(k) for k in ("name", "town", "city", "state", "region", "country") if v.get(k)]
        return ", ".join(str(b) for b in bits)
    return html.unescape(str(v)).strip()


def fetch_hibob(token):
    """HiBob careers boards at {token}.careers.hibob.com.

    The public careers site is an Angular SPA served from a shared build, so the
    HTML is an empty shell. It calls /api/job-ad, identifying the company through
    a companyIdentifier header taken from the subdomain — without that header the
    endpoint answers 401, which is how the pattern was confirmed.

    One board per company, but a single shared platform, so this covers every
    HiBob customer. Note the subdomain is not always the obvious abbreviation:
    BetVictor is "bvgroup", not "bvg" (which exists but is empty)."""
    base = f"https://{token}.careers.hibob.com"
    hdr = {**AGENCY_UA, "Accept": "application/json",
           "companyIdentifier": token, "Referer": f"{base}/jobs"}
    # /api/job-ad answered 200 but yielded nothing on the first run. It may be
    # the single-ad route with the list living at the plural path, so try both
    # and keep whichever returns usable JSON.
    try:
        r = _request("GET", f"{base}/api/job-ad", headers=hdr)
    except Exception as e:
        print(f"      hibob {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      hibob {token}: HTTP {r.status_code}")
        return []
    try:
        d = r.json()
    except Exception:
        print(f"      hibob {token}: 200 but not JSON")
        return []

    # the payload has been seen as a bare list and as a wrapper — accept both
    if isinstance(d, list):
        items = d
    elif isinstance(d, dict):
        items = (d.get("jobAdDetails") or d.get("jobAds") or d.get("data")
                 or d.get("items") or d.get("jobs") or d.get("results") or [])
    else:
        print(f"      hibob {token}: 200 but payload is {type(d).__name__}")
        return []
    if not isinstance(items, list):
        print(f"      hibob {token}: unexpected payload shape {list(d)[:8]}")
        return []
    if not items:
        # say what came back, so one more run identifies the real shape
        shape = (f"dict keys {list(d)[:10]}" if isinstance(d, dict)
                 else f"{type(d).__name__} len {len(d) if hasattr(d,'__len__') else '?'}")
        print(f"      hibob {token}: 200 but nothing extracted — payload is {shape}")
        return []

    out = []
    for j in items:
        if not isinstance(j, dict):
            continue
        title = html.unescape(str(_pick(j, "title", "jobTitle", "name", "positionName") or "")).strip()
        if not title:
            continue
        jid = _pick(j, "id", "jobAdId", "uuid", "externalId") or ""
        out.append({
            "title": title,
            "location": _hibob_location(j),
            "department": str(_pick(j, "department", "departmentName", "team") or ""),
            "url": _pick(j, "url", "applyUrl") or f"{base}/jobs/{jid}",
            "posted_at": _pick(j, "publishedAt", "postedAt", "createdAt", "creationDate"),
        })
    if not out:
        print(f"      hibob {token}: 200 but no usable postings in {len(items)} items")
    return out


def _hibob_location(j):
    """Location arrives variously as a string, an object, or a list of either."""
    v = _pick(j, "location", "locations", "site", "office", "city")
    if isinstance(v, list):
        v = v[0] if v else None
    if isinstance(v, dict):
        parts = [v.get(k) for k in ("name", "city", "state", "country") if v.get(k)]
        return ", ".join(str(x) for x in parts)
    return html.unescape(str(v)).strip() if v else ""


def _pick(d, *keys):
    """First present, non-empty value among keys. Tolerates camel/snake variants."""
    for k in keys:
        for variant in (k, _snake(k)):
            if variant in d and d[variant] not in (None, "", [], {}):
                return d[variant]
    return None


def _snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def fetch_pawatalent(token):
    """pawaTalent — recruitment software pawaTech built themselves, serving their
    own public board at careers.pawatech.com. Plain GET, no pagination: their
    /all route is a single unpaginated view and the API matches it."""
    base = f"https://{token}" if not token.startswith("http") else token
    try:
        r = _request("GET", f"{base}/api/public/positions",
                     headers={**AGENCY_UA, "Accept": "application/json"})
    except Exception as e:
        print(f"      pawatalent {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      pawatalent {token}: HTTP {r.status_code}")
        return []
    try:
        items = r.json()
    except Exception:
        print(f"      pawatalent {token}: 200 but not JSON")
        return []
    if isinstance(items, dict):
        items = items.get("positions") or items.get("data") or items.get("items") or []
    if not isinstance(items, list):
        print(f"      pawatalent {token}: unexpected payload shape")
        return []

    out = []
    for j in items:
        if not isinstance(j, dict):
            continue
        title = html.unescape(str(_pick(j, "title", "name") or "")).strip()
        if not title:
            continue
        loc = _flatten_loc(_pick(j, "location", "city", "locations"))
        if _pick(j, "remote") is True and "remote" not in loc.lower():
            loc = f"{loc}, Remote".strip(", ")
        out.append({
            "title": title,
            "location": loc,
            "department": str(_pick(j, "department", "team") or ""),
            "url": f"{base}/job/{_pick(j, 'id') or ''}",
            "posted_at": _pick(j, "createdAt", "publishedAt"),
        })
    return out


def fetch_pinpoint(token):
    """Pinpoint ATS at {token}.pinpointhq.com. Documented public JSON endpoint;
    the careers home page renders its listing dynamically, so the HTML shows only
    an empty-state message — the JSON is the way in."""
    token, _, title_filter = token.partition("|")
    title_filter = title_filter.strip().lower()
    base = f"https://{token}.pinpointhq.com" if "." not in token else (
        token if token.startswith("http") else f"https://{token}")
    try:
        r = _request("GET", f"{base}/postings.json",
                     headers={**AGENCY_UA, "Accept": "application/json",
                              "X-Requested-With": "XMLHttpRequest"})
    except Exception as e:
        print(f"      pinpoint {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      pinpoint {token}: HTTP {r.status_code}")
        return []
    try:
        d = r.json()
    except Exception:
        print(f"      pinpoint {token}: 200 but not JSON")
        return []
    items = d.get("data") if isinstance(d, dict) else d
    if not isinstance(items, list):
        print(f"      pinpoint {token}: unexpected payload shape")
        return []

    out = []
    skipped = 0
    for j in items:
        if not isinstance(j, dict):
            continue
        a = j.get("attributes") if isinstance(j.get("attributes"), dict) else j
        title = html.unescape(str(_pick(a, "title", "name") or "")).strip()
        if not title:
            continue
        if title_filter and title_filter not in title.lower():
            skipped += 1
            continue
        loc = _pick(a, "location", "locationName", "city")
        if isinstance(loc, dict):
            loc = loc.get("name") or loc.get("city") or ""
        out.append({
            "title": title,
            "location": html.unescape(str(loc or "")).strip(),
            "department": str(_pick(a, "department", "departmentName", "team") or ""),
            "url": _pick(a, "url", "applyUrl", "permalink") or f"{base}/jobs",
            "posted_at": _pick(a, "publishedAt", "createdAt", "postedAt"),
        })
    if title_filter:
        print(f"      pinpoint {token}: {len(out)} matched '{title_filter}', {skipped} others skipped")
    return out


def fetch_avature(token):
    """Avature enterprise portals at {tenant}.avature.net.

    Server-rendered, which makes it one of the easier boards: the listing lives
    at /careers/SearchJobs and pages via jobOffset. Job URLs carry the title as
    a slug (/careers/JobDetail/Art-Director/41215) and the location sits on the
    line beneath each heading as "address - country - hours".

    Token is the tenant, optionally with a culture code: "amswh" or
    "amswh|en-GB"."""
    tenant, _, culture = token.partition("|")
    base = f"https://{tenant}.avature.net"
    path = f"/{culture}/careers/SearchJobs" if culture else "/careers/SearchJobs"

    job_rx = re.compile(
        r'href="([^"]*?/careers/JobDetail/[^"?#]+)"[^>]*>(.*?)</a>', re.S | re.I)
    out, seen = [], set()
    per, offset, total = 5, 0, None
    for _ in range(60):                      # 5 a page, so allow for a big board
        url = f"{base}{path}/?jobRecordsPerPage={per}&jobOffset={offset}"
        try:
            r = _request("GET", url, headers=AGENCY_UA)
        except Exception as e:
            print(f"      avature {tenant}: {type(e).__name__}")
            break
        if r.status_code != 200:
            print(f"      avature {tenant}: HTTP {r.status_code} at offset {offset}")
            break
        page_new = 0
        body = _STYLE_SCRIPT.sub(" ", r.text)
        raw_matches = list(job_rx.finditer(body))
        # each job is linked twice — once from its title, once from Apply — so
        # collapse to the first match per URL. Counting both double-advanced the
        # offset (skipping roles) and left a one-space window for the location.
        matches, seen_here = [], set()
        for mt in raw_matches:
            href = mt.group(1)
            u_key = href if href.startswith("http") else base + href
            if u_key in seen_here:
                continue
            seen_here.add(u_key)
            matches.append(mt)
        if not matches:
            print(f"      avature {tenant}: 200 ({len(body)} bytes) but no /careers/JobDetail/ links")
            print("        " + _href_shapes(body))
        for i, mt in enumerate(matches):
            href, inner = mt.group(1), mt.group(2)
            # everything between this link and the next is where the location sits
            nxt = matches[i + 1].start() if i + 1 < len(matches) else min(mt.end() + 1200, len(body))
            tail = body[mt.end():nxt][:1200]
            title = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", inner))).strip()
            if not title or len(title) < 3 or len(title) > 140:
                continue
            if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
                continue
            u = href if href.startswith("http") else base + href
            if u in seen:
                continue
            seen.add(u)
            loc = _avature_loc(tail)
            if not loc and not out and tail.strip():
                snip = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", tail))[:150]
                print(f"        no location parsed; window began: {snip!r}")
            out.append({"title": title, "location": loc,
                        "department": "", "url": u, "posted_at": None})
            page_new += 1
        if total is None:
            mt_total = re.search(r"There (?:are|is)\s+(\d+)\s+jobs?", body, re.I)
            if mt_total:
                total = int(mt_total.group(1))
                print(f"      avature {tenant}: board reports {total} jobs")
        if not page_new:
            break
        offset += len(matches)
        if total and len(out) >= total:
            break
        time.sleep(REQUEST_DELAY)
    if out:
        print(f"      avature {tenant}: {len(out)} roles")
    else:
        print(f"      avature {tenant}: nothing found at {path}")
    return out


def _strip_tag_debris(v):
    """Remove any half-written tag or entity left by a truncated window."""
    v = re.sub(r"<[^>]*>?", " ", v or "")
    v = re.sub(r"&#?\w{0,8};?\s*$", "", v)
    return re.sub(r"\s+", " ", v).strip(" ,;")


def _avature_loc(tail):
    """The line under an Avature heading reads "address - country - hours".
    Keep the place, drop the hours, and prefer the country over a street.

    The window after a heading also contains the job description, so find the
    bullet-separated line specifically rather than splitting the whole block."""
    if not tail:
        return ""
    # unescape FIRST: an escaped &lt;span&gt; survives a tag strip and then
    # becomes a real tag afterwards, which is how '<span class="separator"'
    # ended up inside evoke's location strings
    raw = html.unescape(tail)
    # the window is cut at a fixed length, so it can end mid-tag. An
    # unterminated "<span class=..." has no closing bracket for the stripper
    # below to match, and it reached the feed as part of the location:
    # 'Leeds, UK, </' and '...Metro Manila <span class="separator" aria-hidden'
    raw = re.sub(r"<[^>]*$", " ", raw)
    # break into lines at block boundaries, then take the first with bullets
    # span is inline and holds the separator bullets — treating it as a line
    # break split "address <span>bullet</span> country" across two lines
    marked = re.sub(r"</(p|div|li|h[1-6])\s*>|<br\s*/?>", "\n", raw, flags=re.I)
    plain = re.sub(r"<[^>]+>", " ", marked)
    line = ""
    for ln in plain.split("\n"):
        ln = re.sub(r"[ \t]+", " ", ln).strip()
        if ln.count("\u2022") >= 1 or ln.count("\u00b7") >= 1:
            line = ln
            break
    txt = line or re.sub(r"\s+", " ", plain).strip()
    if not txt:
        return ""
    parts = [p.strip() for p in re.split(r"[\u2022\u00b7|\u2013\u2014]|\s{3,}", txt) if p.strip()]
    # drop anything that's just an hours figure like "37.5" or "18- 30"
    parts = [p for p in parts if not re.fullmatch(r"[\d.\s-]+", p)]
    if not parts:
        return ""
    parts = [_strip_tag_debris(p) for p in parts]
    parts = [p for p in parts if p]
    if not parts:
        return ""
    if len(parts) >= 2:
        place, country = parts[0], parts[1]
        if len(place) > 40:
            # a full street address — keep the town, not the postcode
            segs = [x.strip() for x in place.split(",") if x.strip()]
            segs = [x for x in segs if not _POSTCODE.fullmatch(x)]
            place = segs[-1] if segs else place
        return f"{place}, {country}"
    return parts[0][:120]


# UK/US/CA postcodes and similar, so a shop address doesn't display as "G78 1SN"
_POSTCODE = re.compile(
    r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}|\d{5}(-\d{4})?|[A-Z]\d[A-Z]\s*\d[A-Z]\d", re.I)



def _dayforce_site_config(portal_html):
    """Dayforce portals are Next.js apps: the board's own settings ship in the
    __NEXT_DATA__ script on the page. Two independent write-ups of this ATS say
    the search call is built FROM those values rather than from the URL, which
    is the likeliest reason a payload assembled purely from our token is
    refused with a bare 403 while the page itself loads fine.

    Returns whatever site fields are present, keyed as the page spells them."""
    m = re.search(r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', portal_html or "", re.S)
    if not m:
        return {}
    try:
        data = json.loads(m.group(1))
    except Exception:
        return {}
    wanted = {"clientnamespace", "jobboardcode", "clientsitexrefcode",
              "culturecode", "companyid", "clientsiteid", "jobboardid",
              "siteid", "companyname"}
    found, stack, budget = {}, [data], 40000
    while stack and budget > 0:
        node = stack.pop()
        budget -= 1
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(v, (dict, list)):
                    stack.append(v)
                elif k.lower() in wanted and v not in (None, "") and k not in found:
                    found[k] = v
        elif isinstance(node, list):
            stack.extend(node[:500])
    return found


def fetch_dayforce(token):
    """Dayforce HCM candidate portals. The board is client-rendered but there is
    a public POST search endpoint that returns everything in one call.

    Token is the client namespace, optionally with a board code and culture:
    "rankgroup" or "rankgroup|CANDIDATEPORTAL|en-GB"."""
    parts = token.split("|")
    ns = parts[0]
    board = parts[1] if len(parts) > 1 and parts[1] else "CANDIDATEPORTAL"
    culture = parts[2] if len(parts) > 2 and parts[2] else "en-US"
    url = f"https://jobs.dayforcehcm.com/api/geo/{ns}/jobposting/search"

    portal = f"https://jobs.dayforcehcm.com/{culture}/{ns}/{board}"
    # a bare POST gets 403 even with browser headers — load the portal first so
    # the session picks up whatever cookie their edge expects. The warm-up now
    # sends a full Chrome fingerprint and its status is REPORTED rather than
    # swallowed: if the portal itself is refused the block is at their edge on
    # the whole tenant, not on the search call, and that is worth knowing from
    # the log instead of guessing.
    site = {}
    try:
        w = session.get(portal, headers=BROWSER_UA, timeout=TIMEOUT)
        if w.status_code != 200:
            print(f"      dayforce {ns}: portal warm-up HTTP {w.status_code}")
        else:
            site = _dayforce_site_config(w.text)
            if site:
                print(f"      dayforce {ns}: site config {site}")
            else:
                print(f"      dayforce {ns}: no __NEXT_DATA__ site config on portal")
    except Exception as e:
        print(f"      dayforce {ns}: portal warm-up {type(e).__name__}")

    out, seen = [], set()
    start = 0
    for _ in range(12):
        payload = {"clientNamespace": ns, "jobBoardCode": board,
                   "cultureCode": culture, "distanceUnit": 0,
                   "paginationStart": start}
        # overlay anything the page itself declared. The board's own spelling
        # of these wins over our guess from the token: a tenant whose site code
        # differs from the URL segment is exactly the case a hardcoded payload
        # gets wrong, and this costs nothing when they agree.
        for k, v in site.items():
            if k.lower() != "companyname":
                payload[k] = v
        try:
            r = _request("POST", url, json=payload,
                         headers={**BROWSER_XHR,
                                  "Content-Type": "application/json",
                                  "Origin": "https://jobs.dayforcehcm.com",
                                  "Referer": portal,
                                  "X-Requested-With": "XMLHttpRequest"})
        except Exception as e:
            print(f"      dayforce {ns}: {type(e).__name__}")
            break
        if r.status_code != 200:
            # print a short body snippet: a WAF challenge page and a plain
            # "forbidden" are different problems and the body is what tells
            # them apart. Without it a 403 says nothing about what to try next.
            snip = re.sub(r"\s+", " ", (r.text or "")[:160]).strip()
            print(f"      dayforce {ns}: HTTP {r.status_code}"
                  + (f" — {snip}" if snip else ""))
            break
        try:
            d = r.json()
        except Exception:
            print(f"      dayforce {ns}: 200 but not JSON")
            break
        # the payload has always been a dict, but a bare list would have made
        # .get() raise and taken the whole run down with it
        if isinstance(d, list):
            items = d
        elif isinstance(d, dict):
            items = (d.get("results") or d.get("jobPostings") or d.get("data")
                     or d.get("items") or d.get("jobs") or [])
        else:
            print(f"      dayforce {ns}: 200 but payload is {type(d).__name__}")
            break
        if not isinstance(items, list) or not items:
            if start == 0:
                shape = (f"keys {list(d)[:8]}" if isinstance(d, dict)
                         else f"{type(d).__name__} len {len(d) if hasattr(d,'__len__') else '?'}")
                print(f"      dayforce {ns}: 200 but nothing extracted — {shape}")
            break
        page_new = 0
        for j in items:
            if not isinstance(j, dict):
                continue
            title = html.unescape(str(_pick(j, "title", "jobTitle", "Title") or "")).strip()
            if not title:
                continue
            jid = _pick(j, "id", "jobPostingId", "referenceNumber", "ReferenceNumber") or ""
            u = (_pick(j, "jobDetailsUrl", "JobDetailsUrl", "url")
                 or f"https://jobs.dayforcehcm.com/{culture}/{ns}/{board}/jobs/{jid}")
            if u in seen:
                continue
            seen.add(u)
            bits = [_pick(j, "city", "City"), _pick(j, "state", "State"),
                    _pick(j, "country", "Country")]
            out.append({
                "title": title,
                "location": ", ".join(str(b) for b in bits if b) or _flatten_loc(_pick(j, "location")),
                "department": str(_pick(j, "department", "Department") or ""),
                "url": u,
                "posted_at": _pick(j, "datePosted", "DatePosted", "postedDate"),
            })
            page_new += 1
        if not page_new:
            break
        start += len(items)
        if isinstance(d, dict) and start >= int(d.get("maxCount") or 0):
            break
        if isinstance(d, list):
            break                            # a bare list is the whole payload
        time.sleep(REQUEST_DELAY)
    if out:
        print(f"      dayforce {ns}: {len(out)} roles")
    return out


def fetch_orangehrm(token):
    """OrangeHRM publishes vacancies as RSS at /recruitmentApply/jobs.rss —
    documented by them as the way to syndicate openings to websites and job
    boards, so it's the intended route rather than scraping the app.

    Token is the host: "pinnacle" or "pinnacle.orangehrmlive.com"."""
    base = (f"https://{token}.orangehrmlive.com" if "." not in token
            else (token if token.startswith("http") else f"https://{token}"))
    try:
        r = _request("GET", f"{base}/recruitmentApply/jobs.rss",
                     headers={**AGENCY_UA, "Accept": "application/rss+xml, application/xml"})
    except Exception as e:
        print(f"      orangehrm {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      orangehrm {token}: HTTP {r.status_code}")
        return []

    items = re.findall(r"<item>(.*?)</item>", r.text, re.S | re.I)
    if not items:
        print(f"      orangehrm {token}: 200 but no <item> in the feed")
        return []

    def tag(block, name, keep_breaks=False):
        m = re.search(rf"<{name}[^>]*>(.*?)</{name}>", block, re.S | re.I)
        if not m:
            return ""
        v = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", m.group(1), flags=re.S)
        v = html.unescape(v)
        if keep_breaks:
            # mark block boundaries before stripping, so "Location: Curacao"
            # doesn't run into the next paragraph
            v = re.sub(r"</(p|div|li|h[1-6])\s*>|<br\s*/?>", "\n", v, flags=re.I)
        v = re.sub(r"<[^>]+>", " ", v)
        v = re.sub(r"[ \t]+", " ", v)
        return v.strip() if keep_breaks else re.sub(r"\s+", " ", v).strip()

    out, seen = [], set()
    for block in items:
        title = tag(block, "title")
        if not title or _MARKUP_JUNK.search(title):
            continue
        link = tag(block, "link") or base
        if link in seen:
            continue
        seen.add(link)
        # the description often carries "Location: X" or similar
        desc = tag(block, "description", keep_breaks=True)
        loc = ""
        m = re.search(r"(?:location|city|based in)\s*[:\-]\s*([^\n.,;|]{2,40})", desc, re.I)
        if m:
            loc = m.group(1).strip()
        out.append({
            "title": title,
            "location": loc,
            "department": tag(block, "category"),
            "url": link,
            "posted_at": tag(block, "pubDate") or None,
        })
    print(f"      orangehrm {token}: {len(out)} roles")
    return out


def fetch_talos(token):
    """Talos360 careers sites.

    The board is a client-rendered shell, but it calls a plain public JSON API
    with no cookies, session or CSRF token at all. Each careers site is keyed by
    a "careersSiteObfuscatedId" UUID, which is the only thing needed.

    Companies can white-label onto their own domain: Betfred sit on
    betfredgroup.talosats-careers.com while UK Tote use careers.uktotegroup.com,
    but the page shells are byte-identical and both hit the same API.

    Token is 'uuid|host' — the host is only used to build job links, e.g.
    'f08b3d92-b104-43d5-bb6e-266542f8affa|careers.uktotegroup.com'."""
    site_id, _, host = token.partition("|")
    site_id = site_id.strip()
    host = (host or "").strip().replace("https://", "").rstrip("/")
    base = f"https://{host}" if host else ""

    try:
        r = _request("POST", "https://api-careers-sites.talos360.com/api/careerssite/vacancies/search",
                     json={"careersSiteObfuscatedId": site_id, "whereCriteria": None,
                           "metadataFilters": [], "preFilters": [], "siteType": "External"},
                     headers={**AGENCY_UA, "Accept": "application/json, text/plain, */*",
                              "Content-Type": "application/json",
                              "Origin": base or "https://careers.uktotegroup.com",
                              "Referer": (base or "https://careers.uktotegroup.com") + "/"})
    except Exception as e:
        print(f"      talos {site_id[:8]}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      talos {site_id[:8]}: HTTP {r.status_code}")
        return []
    try:
        d = r.json()
    except Exception:
        print(f"      talos {site_id[:8]}: 200 but not JSON")
        return []

    items = _talos_items(d)
    if not items:
        shape = (f"dict keys {list(d)[:10]}" if isinstance(d, dict)
                 else f"{type(d).__name__} len {len(d) if hasattr(d, '__len__') else '?'}")
        print(f"      talos {site_id[:8]}: 200 but nothing extracted — payload is {shape}")
        return []

    out, seen = [], set()
    for j in items:
        if not isinstance(j, dict):
            continue
        title = html.unescape(str(_pick(j, "title", "vacancyTitle", "jobTitle", "name") or "")).strip()
        if not title or _MARKUP_JUNK.search(title):
            continue
        jid = str(_pick(j, "id", "vacancyId", "jobId", "reference", "obfuscatedId") or "").strip()
        url = _pick(j, "url", "applyUrl", "link")
        if not url:
            url = f"{base}/job/{jid}" if base and jid else base
        if not url or url in seen:
            continue
        seen.add(url)
        out.append({
            "title": title,
            "location": _flatten_loc(_pick(j, "location", "locations", "town", "city", "region")),
            "department": str(_pick(j, "department", "category", "function") or ""),
            "url": url,
            "posted_at": _pick(j, "publishedDate", "datePosted", "createdDate", "liveDate"),
        })
    print(f"      talos {site_id[:8]}: {len(out)} roles")
    return out


def _talos_items(d):
    """Find the vacancy list wherever it sits in the payload."""
    if isinstance(d, list):
        return d
    if not isinstance(d, dict):
        return []
    # careersSiteVacancies is the real key, named by the 5 Aug run's diagnostic
    for k in ("careersSiteVacancies", "vacancies", "results", "data", "items",
              "jobs", "records", "searchResults"):
        v = d.get(k)
        if isinstance(v, list) and v:
            return v
        if isinstance(v, dict):                 # sometimes nested one deeper
            for k2 in ("vacancies", "results", "data", "items"):
                if isinstance(v.get(k2), list) and v[k2]:
                    return v[k2]
    return []


def _sfcsb_loc_from_url(url, title):
    """CSB builds its job URLs as /job/{City}-{Title}-{postcode}/{id}/, so the
    location is recoverable without another request. Only IGT's board uses the
    table layout that gives us a location cell; Brightstar, Amusnet and Luckia
    fall back to link parsing, and this is what saves those from arriving blank.

    Works by locating the slugified title inside the slug: whatever precedes it
    is the city, whatever follows is a postcode and is dropped."""
    m = re.search(r"/job/([^/]+)/", url or "")
    if not m:
        return ""
    slug = m.group(1)
    from urllib.parse import unquote
    slug = unquote(slug)

    def squash(v):
        """Strip to letters and digits, keeping each character's original index
        so a match can be mapped back. Needed because CSB slugifies punctuation
        inconsistently — a title's "(H/M)" becomes "HM" in the slug."""
        out, idx = [], []
        for i, ch in enumerate(v or ""):
            if ch.isalnum():
                out.append(ch.lower())
                idx.append(i)
        return "".join(out), idx

    ns, ns_idx = squash(slug)
    nt, _ = squash(title)
    if not nt or nt not in ns:
        return ""
    at = ns.index(nt)
    if at == 0:
        return ""                       # title starts the slug, so no city
    city = slug[:ns_idx[at]]
    city = re.sub(r"[-_]+", " ", city)
    city = re.sub(r"\s+", " ", city).strip()
    if len(city) < 2 or len(city) > 40 or re.fullmatch(r"[\d\s-]+", city):
        return ""
    return city


def fetch_sfcsb(token):
    """SAP SuccessFactors CAREER SITE BUILDER — the modern front end.

    Not to be confused with the legacy career?company= pages, whose DWR backend
    refuses us (SAP disabled __System.generateId, so we can't get a registered
    scriptSessionId). CSB is a different product entirely: server-rendered,
    marked meta-robots:index because it exists to be crawled by Google, and
    laid out as a plain HTML table.

    Listing is /search/location paginated by startrow in steps of 25. Each row
    carries the job link, its location and its posting date.

    Token is the host: "jobs.igt.com", optionally with a path prefix for sites
    that use one: "jobs.igt.com|/default"."""
    host, _, prefixes = token.partition("|")
    base = f"https://{host.strip()}" if not host.startswith("http") else host.strip()
    # a tenant can host several CSB "brands", each its own microsite with its own
    # search. Amusnet runs /Interactive alongside the default one, and reading
    # only the first gave 13 of their 28 roles. Comma-separate to read both,
    # with an empty entry meaning the default (no prefix).
    parts = [x.strip().rstrip("/") for x in (prefixes or "").split(",")]
    out_all, seen_all = [], set()
    for prefix in parts:
        for j in _sfcsb_one(base, prefix, host):
            if j["url"] in seen_all:
                continue
            seen_all.add(j["url"])
            out_all.append(j)
    if len(parts) > 1:
        print(f"      sfcsb {host}: {len(out_all)} roles across {len(parts)} brands")
    return out_all


def _sfcsb_one(base, prefix, host):

    # a table row: the job link, then the location cell, then the date cell
    row_rx = re.compile(
        r'href="([^"]*?/job/[^"]*?)"[^>]*>(.*?)</a>.*?'
        r'<td[^>]*class="[^"]*colLocation[^"]*"[^>]*>(.*?)</td>.*?'
        r'<td[^>]*class="[^"]*colDate[^"]*"[^>]*>(.*?)</td>',
        re.S | re.I)
    link_rx = re.compile(r'href="([^"]*?/job/[^"?#]+)"[^>]*>(.*?)</a>', re.S | re.I)

    def clean(v):
        v = re.sub(r"<[^>]+>", " ", v or "")
        return re.sub(r"\s+", " ", html.unescape(v)).strip()

    # IGT lists at /search/location, Luckia at /search/ — try the first that works
    listing = "/search/location"
    try:
        probe = _request("GET", f"{base}{prefix}/search/location?q=", headers=AGENCY_UA)
        if probe.status_code != 200 or "/job/" not in probe.text:
            listing = "/search"
    except Exception:
        listing = "/search"

    out, seen, total = [], set(), None
    for page in range(40):                    # 25 a page, so up to 1,000 roles
        url = (f"{base}{prefix}{listing}?q=&sortColumn=referencedate"
               f"&sortDirection=desc" + (f"&startrow={page * 25}" if page else ""))
        try:
            r = _request("GET", url, headers=AGENCY_UA)
        except Exception as e:
            print(f"      sfcsb {host}: {type(e).__name__}")
            break
        if r.status_code != 200:
            print(f"      sfcsb {host}: HTTP {r.status_code} at row {page * 25}")
            break
        body = _STYLE_SCRIPT.sub(" ", r.text)

        if total is None:
            # IGT prints "Results 1 - 25 of <b>133</b>", Brightstar
            # "Showing 1 to 25 of 113 Jobs" — allow tags between "of" and the number
            mt = re.search(r"\bof\s*(?:<[^>]{0,40}>\s*)*([\d,]{1,7})\b", body, re.I)
            if mt:
                total = int(mt.group(1).replace(",", ""))
                print(f"      sfcsb {host}: board reports {total} jobs")

        found = row_rx.findall(body)
        page_new = 0
        for href, title_html, loc_html, date_html in found:
            title = clean(title_html)
            if not title or len(title) < 3 or len(title) > 140:
                continue
            if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
                continue
            u = href if href.startswith("http") else base + href
            u = u.split("?")[0]
            if u in seen:
                continue
            seen.add(u)
            loc = re.sub(r"\s*\+\d+ more.*$", "", clean(loc_html)).strip()
            out.append({
                "title": title,
                "location": loc or _sfcsb_loc_from_url(u, title),
                "department": "",
                "url": u,
                "posted_at": _sfcsb_date(clean(date_html)),
            })
            page_new += 1

        if not page_new:
            # the row pattern depends on CSB's default column classes; fall back
            # to plain link parsing so a themed site still returns something
            for href, title_html in link_rx.findall(body):
                title = clean(title_html)
                if not title or len(title) < 3 or len(title) > 140:
                    continue
                if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
                    continue
                u = (href if href.startswith("http") else base + href).split("?")[0]
                if u in seen:
                    continue
                seen.add(u)
                out.append({"title": title,
                            "location": _sfcsb_loc_from_url(u, title),
                            "department": "", "url": u, "posted_at": None})
                page_new += 1
            if page_new and page == 0:
                got_loc = sum(1 for j in out if j["location"])
                print(f"      sfcsb {host}: table columns not matched, using link "
                      f"parse ({got_loc}/{len(out)} locations recovered from urls)")

        if not page_new:
            if page == 0:
                print(f"      sfcsb {host}: 200 ({len(body)} bytes) but no /job/ links")
                print("        " + _href_shapes(body))
            break
        if total and len(out) >= total:
            break
        time.sleep(REQUEST_DELAY)

    print(f"      sfcsb {host}: {len(out)} roles")
    return out


_SFCSB_MONTHS = {}
for _i, _names in enumerate([
        ("jan", "ene", "gen"), ("feb", "fev"), ("mar", "mär"), ("apr", "abr"),
        ("may", "mai", "mag"), ("jun", "giu"), ("jul", "lug"), ("aug", "ago"),
        ("sep", "set"), ("oct", "okt", "ott", "out"), ("nov",), ("dec", "dic", "dez")], 1):
    for _n in _names:
        _SFCSB_MONTHS[_n] = _i


def _sfcsb_date(txt):
    """CSB localises its dates: "Aug 7, 2026" in English, "7 ago 2026" in
    Spanish, and so on. Accept the month either side of the day."""
    t = (txt or "").strip()
    m = re.search(r"([A-Za-z]{3})[a-z]*\.?\s+(\d{1,2}),?\s*(\d{4})", t)   # Aug 7, 2026
    if m:
        mon, day, year = m.group(1), m.group(2), m.group(3)
    else:
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\.?\s+(\d{4})", t)  # 7 ago 2026
        if not m:
            return None
        day, mon, year = m.group(1), m.group(2), m.group(3)
    num = _SFCSB_MONTHS.get(mon.lower())
    if not num:
        return None
    return f"{year}-{num:02d}-{int(day):02d}T00:00:00+00:00"


def fetch_hurma(token):
    """Hurma — an HR platform serving careers sites at {tenant}.hurma.work.

    A Laravel app: the XSRF-TOKEN and hurma_session cookies give it away. The
    listing calls /api/v1/public-vacancies?page=N&per_page=M, paginated in the
    usual Laravel shape. Loading the careers page first establishes the session
    and issues the XSRF token, which is echoed back in the X-XSRF-TOKEN header.

    Its own session, not the shared one — the same reason SuccessFactors needed
    one: a jar carrying thirty other sites' cookies is asking for trouble.

    Token is the tenant: "spribe", or a full host."""
    from urllib.parse import unquote
    host = token.strip().replace("https://", "").rstrip("/")
    base = f"https://{host}" if "." in host else f"https://{host}.hurma.work"

    hs = requests.Session()
    listing = f"{base}/public-vacancies"
    try:
        hs.get(listing, headers=AGENCY_UA, timeout=TIMEOUT)
    except Exception as e:
        print(f"      hurma {token}: {type(e).__name__} on the careers page")
        return []
    xsrf = unquote(hs.cookies.get("XSRF-TOKEN") or "")

    hdr = {**AGENCY_UA, "Accept": "application/json, text/plain, */*",
           "X-Requested-With": "XMLHttpRequest", "Referer": listing}
    if xsrf:
        hdr["X-XSRF-TOKEN"] = xsrf

    out, seen, total = [], set(), None
    for page in range(1, 21):
        url = f"{base}/api/v1/public-vacancies?page={page}&per_page=50"
        try:
            r = hs.get(url, headers=hdr, timeout=TIMEOUT)
        except Exception as e:
            print(f"      hurma {token}: {type(e).__name__}")
            break
        if r.status_code != 200:
            print(f"      hurma {token}: HTTP {r.status_code} on page {page}")
            break
        try:
            d = r.json()
        except Exception:
            print(f"      hurma {token}: 200 but not JSON")
            break

        items = d
        if isinstance(d, dict):
            for k in ("data", "vacancies", "items", "results", "list"):
                if isinstance(d.get(k), list):
                    items = d[k]
                    break
            meta = d.get("meta") if isinstance(d.get("meta"), dict) else d
            if total is None and isinstance(meta, dict):
                total = meta.get("total") or meta.get("total_count")
                if total:
                    print(f"      hurma {token}: board reports {total} vacancies")
        if not isinstance(items, list):
            print(f"      hurma {token}: unexpected payload {type(items).__name__}")
            break
        if not items:
            break

        page_new = 0
        for j in items:
            if not isinstance(j, dict):
                continue
            title = html.unescape(str(_pick(j, "name", "title", "position",
                                            "vacancy_name") or "")).strip()
            if not title or _MARKUP_JUNK.search(title):
                continue
            jid = str(_pick(j, "id", "slug", "uuid", "vacancy_id") or "").strip()
            u = _pick(j, "url", "link", "public_url")
            if not u:
                u = f"{base}/public-vacancies/{jid}" if jid else listing
            if u in seen:
                continue
            seen.add(u)
            out.append({
                "title": title,
                "location": _flatten_loc(_pick(j, "city", "location", "cities",
                                               "locations", "country", "office")),
                "department": str(_pick(j, "department", "category", "unit") or ""),
                "url": u,
                "posted_at": _pick(j, "published_at", "created_at", "date_start"),
            })
            page_new += 1

        if not page_new:
            if page == 1:
                shape = (f"dict keys {list(d)[:10]}" if isinstance(d, dict)
                         else f"{type(d).__name__}")
                print(f"      hurma {token}: 200 but nothing extracted — {shape}")
                if isinstance(items, list) and items and isinstance(items[0], dict):
                    print(f"         first record keys: {sorted(items[0])[:14]}")
            break
        if total and len(out) >= int(total):
            break
        time.sleep(REQUEST_DELAY)

    print(f"      hurma {token}: {len(out)} roles")
    return out


def _csod_date(v):
    """Cornerstone hand back DD/MM/YYYY. Left raw, the browser reads it as the
    American MM/DD and 06/08/2026 lands in June rather than August — wrong by
    two months, which breaks both the sort and the 48-hour filter. 28/07/2026
    settles which way round it is: there is no month 28."""
    if not v:
        return None
    t = str(v).strip()
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$", t)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if mo > 12 and d <= 12:          # the other way round after all
            d, mo = mo, d
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return t


def fetch_csod(token):
    """Cornerstone OnDemand career sites at {tenant}.csod.com.

    The page is a React shell, so nothing is scrapeable — but everything needed
    is in its HTML anyway. csod.context carries a Bearer token AND the regional
    API host:

        csod.context = {"corp":"olympic", "endpoints":{"cloud":"https://eu-fra.api.csod.com/"},
                        "token":"eyJhbGciOi..."}

    The token is anonymous (sub -100) and expires two hours after issue, which
    is why it must be lifted fresh each run rather than pinned. The listing is
    one POST to rec-job-search/external/jobs — pageSize 9999, so no paging.

    Token is the tenant, optionally with a career site id: "olympic" or
    "olympic|1"."""
    tenant, _, site = token.partition("|")
    tenant = tenant.strip().replace(".csod.com", "")
    site = (site.strip() or "1")
    base = f"https://{tenant}.csod.com"
    home = f"{base}/ux/ats/careersite/{site}/home?c={tenant}"

    cs = requests.Session()
    try:
        r = cs.get(home, headers=AGENCY_UA, timeout=TIMEOUT)
    except Exception as e:
        print(f"      csod {tenant}: {type(e).__name__} on the career site")
        return []
    if r.status_code != 200:
        print(f"      csod {tenant}: HTTP {r.status_code} on the career site")
        return []

    mt = re.search(r"csod\.context\s*=\s*(\{.*?\})\s*;", r.text, re.S)
    ctx = {}
    if mt:
        try:
            ctx = json.loads(mt.group(1))
        except Exception:
            ctx = {}
    bearer = ctx.get("token") or ""
    if not bearer:
        m2 = re.search(r'"token"\s*:\s*"([A-Za-z0-9._-]{40,})"', r.text)
        bearer = m2.group(1) if m2 else ""
    if not bearer:
        print(f"      csod {tenant}: no token found in {len(r.text)} bytes")
        return []
    cloud = ((ctx.get("endpoints") or {}).get("cloud") or "https://api.csod.com/").rstrip("/")

    body = {"careerSiteId": site, "careerSitePageId": site, "pageNumber": 1,
            "pageSize": 9999, "cultureId": 2, "searchText": "",
            "cultureName": "English (UK)", "states": [], "countryCodes": [],
            "cities": [], "placeID": "", "radius": None, "postingsWithinDays": None,
            "customFieldCheckboxKeys": [], "customFieldDropdowns": [],
            "customFieldRadios": []}
    try:
        rp = cs.post(f"{cloud}/rec-job-search/external/jobs", json=body, timeout=TIMEOUT,
                     headers={**AGENCY_UA, "Accept": "*/*",
                              "Content-Type": "application/json",
                              "Authorization": f"Bearer {bearer}",
                              "Origin": base, "Referer": base + "/"})
    except Exception as e:
        print(f"      csod {tenant}: {type(e).__name__} on the search API")
        return []
    if rp.status_code != 200:
        print(f"      csod {tenant}: search API HTTP {rp.status_code}")
        return []
    try:
        d = rp.json()
    except Exception:
        print(f"      csod {tenant}: 200 but not JSON")
        return []

    # Their reply is {"status":..., "timestamp":..., "data":{...}} — the list is
    # nested inside data, so walk for the first list of dicts that looks like
    # postings rather than guessing at the key name.
    def _find_jobs(node, depth=0):
        if depth > 4:
            return None
        if isinstance(node, list):
            if node and isinstance(node[0], dict) and any(
                    k in node[0] for k in ("title", "jobTitle", "name",
                                           "requisitionTitle", "requisitionId")):
                return node
            return None
        if isinstance(node, dict):
            for k in ("data", "jobs", "results", "items", "requisitions",
                      "jobList", "searchResults"):
                found = _find_jobs(node.get(k), depth + 1)
                if found:
                    return found
            for v in node.values():
                found = _find_jobs(v, depth + 1)
                if found:
                    return found
        return None

    items = _find_jobs(d) or d
    if not isinstance(items, list) or not items:
        shape = f"dict keys {list(d)[:10]}" if isinstance(d, dict) else type(d).__name__
        inner = d.get("data") if isinstance(d, dict) else None
        if isinstance(inner, dict):
            shape += f" | data keys {list(inner)[:12]}"
        print(f"      csod {tenant}: 200 but nothing extracted — {shape}")
        return []

    out, seen = [], set()
    for j in items:
        if not isinstance(j, dict):
            continue
        # Cornerstone name it displayJobTitle. Their own diagnostic gave this
        # up: "27 records but none mapped — keys ['displayJobTitle', ...]".
        title = html.unescape(str(_pick(j, "displayJobTitle", "title", "jobTitle",
                                        "name", "requisitionTitle") or "")).strip()
        if not title or _MARKUP_JUNK.search(title):
            continue
        jid = str(_pick(j, "requisitionId", "id", "jobId", "reqId") or "").strip()
        u = _pick(j, "url", "applyUrl")
        if not u:
            u = (f"{base}/ux/ats/careersite/{site}/job/{jid}?c={tenant}" if jid
                 else home)
        if u in seen:
            continue
        seen.add(u)
        out.append({
            "title": title,
            "location": _flatten_loc(_pick(j, "location", "locations", "city",
                                           "displayLocation", "primaryLocation")),
            "department": str(_pick(j, "department", "division", "jobCategory") or ""),
            "url": u,
            "posted_at": _csod_date(_pick(j, "postingEffectiveDate", "postedDate",
                                          "publishedDate", "createdDate")),
        })
    if not out and items and isinstance(items[0], dict):
        print(f"      csod {tenant}: {len(items)} records but none mapped — "
              f"keys {sorted(items[0])[:14]}")
    else:
        print(f"      csod {tenant}: {len(out)} roles")
    return out


def fetch_join(token):
    """JOIN (join.com) company pages.

    A Next.js app, so the whole job list ships inside __NEXT_DATA__ as proper
    structured data — no markup parsing and no API call needed:

        "jobs":{"items":[{"title":"...", "idParam":"16548464-finance-...",
                          "createdAt":"2026-07-20T...",
                          "city":{"cityName":"Valletta","countryName":"Malta"},
                          "category":{"name":"Finance"},
                          "workplaceType":"ONSITE"}]}

    Note a fully remote role still carries a city — "Remote / United States" —
    so workplaceType and remoteType decide the location, not the city fields.

    Token is the company slug: "booming-games"."""
    slug = token.strip().strip("/").split("/")[-1]
    url = f"https://join.com/companies/{slug}"
    try:
        r = _request("GET", url, headers=AGENCY_UA)
    except Exception as e:
        print(f"      join {slug}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      join {slug}: HTTP {r.status_code}")
        return []

    mt = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if not mt:
        print(f"      join {slug}: no __NEXT_DATA__ in {len(r.text)} bytes")
        return []
    try:
        data = json.loads(mt.group(1))
    except Exception:
        print(f"      join {slug}: __NEXT_DATA__ did not parse")
        return []

    items = (((data.get("props") or {}).get("pageProps") or {})
             .get("initialState") or {}).get("jobs") or {}
    items = items.get("items") if isinstance(items, dict) else None
    if not isinstance(items, list) or not items:
        print(f"      join {slug}: no jobs in the page data")
        return []

    out = []
    for j in items:
        if not isinstance(j, dict):
            continue
        title = html.unescape(str(j.get("title") or "")).strip()
        if not title:
            continue
        city = j.get("city") if isinstance(j.get("city"), dict) else {}
        wp = str(j.get("workplaceType") or "").upper()
        if wp == "REMOTE" and str(j.get("remoteType") or "").upper() == "ANYWHERE":
            loc = "Remote"
        else:
            bits = [city.get("cityName"), city.get("countryName")]
            loc = ", ".join(str(b) for b in bits if b)
            if wp == "REMOTE" and "remote" not in loc.lower():
                loc = f"{loc} (Remote)".strip()
        cat = j.get("category") if isinstance(j.get("category"), dict) else {}
        out.append({
            "title": title,
            "location": loc,
            "department": str(cat.get("name") or ""),
            "url": f"{url}/{j.get('idParam') or j.get('id') or ''}",
            "posted_at": j.get("createdAt") or j.get("publishedAt"),
        })
    print(f"      join {slug}: {len(out)} roles")
    return out


def fetch_zoho(token):
    """Zoho Recruit career sites at {tenant}.zohorecruit.{tld}.

    The whole board ships inside a hidden input on the careers page — no API
    call, no paging, no SHOW MORE:

        <input type="hidden" value="[{...}]" id="jobs">

    The value is HTML-escaped JSON carrying every field worth having:
    Posting_Title, City, State, Country, Date_Opened, Job_Type, Industry and
    the record id. MaxBet's SHOW MORE button paginates the DISPLAY only; the
    data behind it is already on the page.

    Token is the host, optionally with the career page name:
    "maxbet.zohorecruit.eu" or "maxbet.zohorecruit.eu|Careers"."""
    host, _, page = token.partition("|")
    host = host.strip().replace("https://", "").rstrip("/")
    page = (page.strip() or "Careers")
    base = f"https://{host}"
    url = f"{base}/jobs/{page}"
    try:
        r = _request("GET", url, headers=AGENCY_UA)
    except Exception as e:
        print(f"      zoho {host}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      zoho {host}: HTTP {r.status_code}")
        return []

    # Isolate the TAG first, then read its value. Their page has four hidden
    # inputs — pageJson, moduleMeta, jobs, meta — and moduleMeta also starts
    # with "[", so matching value=... directly latched onto the wrong one and
    # captured across the tag boundary. Attribute values carry no literal " or
    # > (both are entity-escaped), so tag isolation is safe.
    raw = None
    for tm in re.finditer(r"<input\b[^>]*>", r.text):
        tag = tm.group(0)
        if not re.search(r'\bid="jobs"', tag):
            continue
        vm = re.search(r'\bvalue="([^"]*)"', tag)
        if vm:
            raw = vm.group(1)
            break
    if raw is None:
        ids = re.findall(r'<input[^>]*id="(\w+)"', r.text)[:10]
        print(f"      zoho {host}: no jobs input found — inputs present: {ids}")
        return []
    try:
        items = json.loads(html.unescape(raw))
    except Exception as e:
        head = html.unescape(raw)[:110].replace("\n", " ")
        print(f"      zoho {host}: jobs input did not parse — {type(e).__name__}: {head}")
        return []
    if not isinstance(items, list) or not items:
        print(f"      zoho {host}: jobs input empty")
        return []

    out = []
    for j in items:
        if not isinstance(j, dict):
            continue
        if j.get("Publish") is False:
            continue
        title = html.unescape(str(_pick(j, "Posting_Title", "Job_Opening_Name") or "")).strip()
        if not title:
            continue
        jid = str(j.get("id") or "").strip()
        slug = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-") or "job"
        bits = [str(j.get(k) or "").strip() for k in ("City", "Country")]
        loc = ", ".join(b for b in bits if b and b.lower() != "null")
        if not loc and j.get("Remote_Job"):
            loc = "Remote"
        out.append({
            "title": title,
            "location": loc[:80],
            "department": str(j.get("Industry") or "")[:60],
            "url": f"{base}/jobs/{page}/{jid}/{slug}" if jid else url,
            "posted_at": j.get("Date_Opened"),
        })
    print(f"      zoho {host}: {len(out)} roles")
    return out


def fetch_breezy(token):
    """Breezy HR public board: https://<token>.breezy.hr/json

    NOTE the slug is the account name, not the company name — Betclic Group are
    "betclic-group", with the hyphen. An earlier probe guessed "betclic" and
    found nothing, which is why they sat unresolved for weeks."""
    try:
        r = session.get(f"https://{token}.breezy.hr/json", timeout=TIMEOUT)
    except Exception as e:
        print(f"      breezy {token}: {type(e).__name__}")
        return []
    if r.status_code != 200:
        print(f"      breezy {token}: HTTP {r.status_code}")
        return []
    try:
        d = r.json()
    except Exception:
        print(f"      breezy {token}: 200 but not JSON")
        return []
    if not isinstance(d, list):
        print(f"      breezy {token}: unexpected payload {type(d).__name__}")
        return []
    out = []
    for j in d if isinstance(d, list) else []:
        if not isinstance(j, dict):
            continue
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
    d = _json_list(f"https://api.lever.co/v0/postings/{token}?mode=json",
                   f"lever {token}")
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
    d = {"jobs": _json_list(f"https://api.ashbyhq.com/posting-api/job-board/{token}",
                            f"ashby {token}", key="jobs")}
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
        batch = _json_list(
            f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
            f"?limit=100&offset={offset}", f"smartrecruiters {token}", key="content")
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
    d = {"offers": _json_list(f"https://{token}.recruitee.com/api/offers/",
                              f"recruitee {token}", key="offers")}
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
            r = _request("POST", f"https://apply.workable.com/api/v3/accounts/{token}/jobs",
                         json=payload,
                         headers={**AGENCY_UA, "Accept": "application/json",
                                  "Content-Type": "application/json",
                                  "Origin": "https://apply.workable.com",
                                  "Referer": f"https://apply.workable.com/{token}/"})
            if r.status_code == 429:
                print(f"      workable {token}: still rate limited, stopping "
                      f"({len(out)} roles so far)")
                break
            if r.status_code != 200:
                print(f"      workable {token}: HTTP {r.status_code}")
                break
            d = r.json()
        except Exception as e:
            print(f"      workable {token}: {type(e).__name__}")
            break
        if not isinstance(d, dict):
            print(f"      workable {token}: payload is {type(d).__name__}")
            break
        batch = d.get("results") or d.get("jobs") or []
        batch = [x for x in batch if isinstance(x, dict)]
        out.extend(batch)
        page_token = d.get("nextPage")
        if not page_token or not batch:
            break
        time.sleep(REQUEST_DELAY)

    time.sleep(REQUEST_DELAY * 2)      # be gentler on a host we share across companies

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
    Works for <token>.teamtailor.com and for boards on a customer's own domain.

    Each card reads "{Title} {Department} · {Location}", but the department and
    location sit in siblings AFTER the anchor rather than inside it. So the
    title comes from the link and the rest from the window up to the next link
    — the same approach the Avature fetcher uses. Without this every Teamtailor
    company arrived with no location at all: 94 roles across nine of them."""
    base = _tt_base(token)
    try:
        page = session.get(f"{base}/jobs", headers=AGENCY_UA, timeout=TIMEOUT).text
    except Exception:
        return []
    body = _STYLE_SCRIPT.sub(" ", page)
    link_rx = re.compile(r'href="([^"]*?/jobs/[^"?#]+)"', re.I)
    marks = [(mt.group(1), mt.end(), mt.start()) for mt in link_rx.finditer(body)]

    jobs, seen = [], set()
    for url, title in _links_with_titles(page, base, "/jobs/"):
        if url.rstrip("/").endswith("/jobs") or url in seen:
            continue
        seen.add(url)
        jobs.append({"title": title, "location": _tt_meta(body, marks, url, base),
                     "department": "", "url": url, "posted_at": None})
    return jobs


def _tt_meta(body, marks, url, base):
    """Pull the location out of the window following a job link. The card meta
    reads "Department · Location", occasionally with several locations."""
    path = url[len(base):] if url.startswith(base) else url
    span = None
    for i, (href, pos, _st) in enumerate(marks):
        if href == path or href == url:
            # bound at where the NEXT link BEGINS, not where it ends, or the
            # window swallows the following card's title and location too
            nxt = marks[i + 1][2] if i + 1 < len(marks) else min(pos + 600, len(body))
            span = (pos, max(nxt, pos))
            break
    if not span:
        return ""
    window = body[span[0]:span[1]][:600]
    txt = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", window))).strip()
    if "\u00b7" not in txt and "·" not in txt:
        return ""
    tail = re.split(r"\s*[\u00b7·]\s*", txt, maxsplit=1)[-1]
    # the window runs on into the next card, so stop at the following title
    tail = re.split(r"\s{3,}", tail)[0]
    tail = _strip_tag_debris(tail)[:80].strip(" ,;-")
    if not tail or len(tail) < 2 or _MARKUP_JUNK.search(tail):
        return ""
    return tail


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
    for _ in range(40):
        r = session.post(
            endpoint,
            # Workday's CXS endpoint rejects a limit above 20 with HTTP 422 — do not raise it
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
        offset += len(batch)
        total = d.get("total") or 0
        # a missing or wrong `total` must not halt paging — only an empty batch
        # or reaching a total we actually trust should stop it
        if not batch or (total and offset >= total):
            break
        time.sleep(REQUEST_DELAY)
    if len(out) and len(out) % 20 == 0:
        print(f"      workday {tenant}/{site}: {len(out)} roles (exact multiple of the "
              f"page size — check it isn't truncated)")
    return out


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "greenhouse_eu": fetch_greenhouse_eu,
    "bamboohr": fetch_bamboohr,
    "ultipro": fetch_ultipro,
    "icims": fetch_icims,
    "freshteam": fetch_freshteam,
    "wordpress": fetch_wordpress,
    "hibob": fetch_hibob,
    "pawatalent": fetch_pawatalent,
    "pinpoint": fetch_pinpoint,
    "avature": fetch_avature,
    "dayforce": fetch_dayforce,
    "orangehrm": fetch_orangehrm,
    "talos": fetch_talos,
    "hurma": fetch_hurma,
    "csod": fetch_csod,
    "join": fetch_join,
    "zoho": fetch_zoho,
    "jobvite": fetch_jobvite,
    "betterteam": fetch_betterteam,
    "rippling": fetch_rippling,
    "oracle": fetch_oracle,
    "successfactors": fetch_successfactors,
    "sfcsb": fetch_sfcsb,
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

# A fuller Chrome fingerprint. Some edges (Dayforce at Rank) answer 403 to the
# minimal header set above because the client-hint and Sec-Fetch headers a real
# browser always sends are missing. Only used where a plain request is refused.
_CH_UA = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
BROWSER_UA = {
    **AGENCY_UA,
    "sec-ch-ua": _CH_UA,
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Upgrade-Insecure-Requests": "1",
    # no "br": requests only decodes brotli when the brotli package is
    # installed, and a body we cannot decode fails the JSON parse.
    "Accept-Encoding": "gzip, deflate",
}
BROWSER_XHR = {
    **BROWSER_UA,
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}
BROWSER_XHR.pop("Sec-Fetch-User", None)
BROWSER_XHR.pop("Upgrade-Insecure-Requests", None)

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
    "Vat": "VAT", "Cs": "CS", "Ta": "TA", "Rg": "RG", "Cx": "CX", "Sre": "SRE",
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


def _href_shapes(html_text, limit=12):
    """Summarise the distinct URL path shapes on a page. When a marker finds
    nothing this says what the real pattern is, instead of another guess."""
    paths = []
    for href in re.findall(r'href="([^"#?]+)', html_text):
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        p = re.sub(r"^https?://[^/]+", "", href)
        seg = [x for x in p.split("/") if x]
        if not seg:
            continue
        # collapse the identifying part so shapes group: /jobs/foo-123 -> /jobs/*
        shape = f"/{seg[0]}/*" if len(seg) > 1 else f"/{seg[0]}"
        paths.append(shape)
    common = Counter(paths).most_common(limit)
    return ", ".join(f"{p} x{n}" for p, n in common)


# Titles that are navigation, not vacancies. Scrapers pick these up when a
# listing page links to itself or to a category index.
_JUNK_TITLE = re.compile(
    r"^(job\s*board|jobs?|vacanc(y|ies)|careers?|search|apply|home|all\s+jobs?|"
    r"view\s+all|more|see\s+all|browse|opportunities|open\s+roles?|back|next|"
    r"previous|contact|about|register|sign\s*in|log\s*in)$", re.I)


# A requisition reference glued to the end of a title. Emerald Zebra publish
# "Risk and Payments Agent-4200563" and "Head of Projects (FinTech), Limassol,
# Cyprus-4245817" — the number is in their post title, not something we added.
# Five digits minimum, so a genuine "Engineer II-2024" or "Casino 2000" is safe.
_TITLE_REF = re.compile(r"\s*[-–—_#]\s*\d{5,}\s*$")


# Places that turn up as a trailing clause on an agency's job titles. Not a
# world gazetteer — just enough to be confident a comma-tail is a LOCATION and
# not part of the role, so "Director, VIP Sports" is never mistaken for one.
_TITLE_PLACES = re.compile(
    r"^(remote|hybrid|onsite|on-site|"
    r"cyprus|limassol|nicosia|larnaca|paphos|famagusta|"
    r"malta|valletta|sliema|gzira|msida|"
    r"dubai|abu dhabi|uae|united arab emirates|qatar|doha|saudi\w*|riyadh|"
    r"london|manchester|leeds|gibraltar|isle of man|douglas|jersey|guernsey|uk|"
    r"united kingdom|england|scotland|ireland|dublin|"
    r"greece|athens|thessaloniki|bulgaria|sofia|romania|bucharest|"
    r"poland|warsaw|krakow|portugal|lisbon|porto|spain|madrid|barcelona|"
    r"germany|berlin|munich|netherlands|amsterdam|belgium|brussels|"
    r"estonia|tallinn|latvia|riga|lithuania|vilnius|ukraine|kyiv|kiev|"
    r"serbia|belgrade|croatia|zagreb|slovenia|ljubljana|austria|vienna|"
    r"italy|rome|milan|france|paris|switzerland|zurich|geneva|"
    r"sweden|stockholm|denmark|copenhagen|norway|oslo|finland|helsinki|"
    r"armenia|yerevan|georgia|tbilisi|batumi|israel|tel aviv|turkey|istanbul|"
    r"south africa|cape town|johannesburg|nigeria|lagos|kenya|nairobi|"
    r"usa|us|united states|new york|las vegas|new jersey|canada|toronto|"
    r"australia|sydney|melbourne|philippines|manila|singapore|japan|tokyo|"
    r"brazil|sao paulo|mexico|colombia|bogota|peru|lima|panama|costa rica"
    r")\b", re.I)


def _loc_from_title(title, current=""):
    """Some agencies bake the real location into the job title and leave the
    location FIELD holding their own office or a broad taxonomy. Emerald Zebra
    publish "Financial Analyst, Dubai, UAE" with a location of Limassol — wrong
    for 55 of their 69 roles.

    Only the trailing comma-separated segments are considered, and only when
    they are recognisable places, so "Director, VIP Sports" is left alone."""
    # clean first: the requisition reference is glued to the LAST segment, so
    # "…, Dubai, UAE-4244372" would otherwise yield a location of "UAE-4244372"
    parts = [p.strip() for p in _clean_title(title).split(",")]
    if len(parts) < 2:
        return current
    tail = []
    for p in reversed(parts[1:]):
        base = re.sub(r"\s*\(.*?\)\s*", " ", p).strip()
        if base and _TITLE_PLACES.match(base) and len(base) <= 30:
            tail.insert(0, base)
        else:
            break
    if not tail:
        return current
    return ", ".join(tail)[:80]


def _clean_title(t):
    t = _TITLE_REF.sub("", (t or "").strip())
    return re.sub(r"\s+", " ", t).strip(" ,;-–—")


def _drop_junk(jobs, source=""):
    """Strip navigation links that slipped through as vacancies, and trailing
    requisition references from the titles that survive."""
    out = [j for j in jobs if not _JUNK_TITLE.match((j.get("title") or "").strip())]
    if len(out) != len(jobs) and source:
        print(f"      {source}: dropped {len(jobs)-len(out)} navigation link(s)")
    fixed = 0
    for j in out:
        t = j.get("title") or ""
        c = _clean_title(t)
        if c and c != t:
            j["title"] = c
            fixed += 1
    if fixed and source:
        print(f"      {source}: stripped a trailing reference from {fixed} title(s)")
    return out


# CSS and JS can contain anything, including strings that look like hrefs.
# Apercon's stylesheet produced a "role" titled img:is([sizes=auto i],[sizes^=
_STYLE_SCRIPT = re.compile(r"<(style|script|noscript)\b.*?</\1>", re.S | re.I)

# a title that is really a page heading rather than a vacancy
_PAGE_TITLE = re.compile(
    r"^(jobs?|careers?|vacanc\w*|opportunit\w*|positions?)\s+(at|with|@|in)\b", re.I)

# leftovers that mean the parser grabbed markup, not a job
_MARKUP_JUNK = re.compile(r"[{}]|:is\(|\^=|@media|!important|</|\bdocument\.|\bfunction\s*\(")


def _links_first_element(html_text, base, path_marker):
    """(url, title) where the title is the FIRST block element inside the anchor.

    Some boards put the job title in a heading and then append location,
    department and work type as siblings, all inside the same <a>. Stripping all
    tags runs them together; taking the first block keeps just the title."""
    html_text = _STYLE_SCRIPT.sub(" ", html_text)
    rx = re.compile(
        r'href="([^"]*?' + re.escape(path_marker) + r'[^"?#]*)[^"]*"[^>]*>(.*?)</a>',
        re.S | re.I,
    )
    inner_block = re.compile(
        r"<(h[1-6]|p|div|span|strong|b)\b[^>]*>(.*?)</\1>", re.S | re.I)
    out, seen = [], set()
    for href, inner in rx.findall(html_text):
        blocks = [re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", b[1]))).strip()
                  for b in inner_block.findall(inner)]
        blocks = [b for b in blocks if b]
        if blocks:
            title = blocks[0]
            rest = blocks[1:]
        else:
            title = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", inner))).strip()
            rest = []
        if not title or len(title) < 3 or len(title) > 140:
            continue
        if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
            continue
        # the remaining blocks are location, department and work type in some
        # order; the one naming a place is whichever isn't an employment type
        loc = next((b for b in rest if not _EMPLOYMENT_TYPE.fullmatch(b)), "")
        # some boards run the three together in a single element rather than
        # as siblings — "Varna, Bulgaria Finance Full-time hybrid" invented
        # five different Slovakias. Trim the department and work type back off.
        loc = _LOC_TRAILING.sub("", loc).strip(" ,;-")
        url = href if href.startswith("http") else base.rstrip("/") + href
        if url in seen:
            continue
        seen.add(url)
        out.append((url, title, loc))
    return out


_LOC_TRAILING = re.compile(
    r"\s+(Analytics|Commercial|Compliance|Customer(?:\s+\w+)?|Delivery|Design|"
    r"Engineering|Finance|HR|Legal|Marketing|Operations|People|"
    r"Product(?:\s+Management)?|Risk|Sales|Support|Tech(?:nology)?|Trading)?"
    r"\s*(Full[- ]?time|Part[- ]?time|Contract|Permanent|Temporary|"
    r"Intern(?:ship)?|Freelance)?\s*(remote|hybrid|on[- ]?site)?\s*$", re.I)


_EMPLOYMENT_TYPE = re.compile(
    r"(full[- ]?time|part[- ]?time|contract|permanent|temporary|intern(ship)?|"
    r"freelance|remote|hybrid|on[- ]?site|casual)", re.I)


def _links_with_titles(html_text, base, path_marker):
    """(url, title) pairs for links whose path contains `path_marker`.
    Deliberately tolerant: hrefs may be relative or absolute, and the visible
    title is often wrapped in nested tags rather than sitting directly inside
    the anchor, so tags are stripped rather than excluded."""
    html_text = _STYLE_SCRIPT.sub(" ", html_text)
    out, seen = [], set()
    rx = re.compile(
        r'href="([^"]*?' + re.escape(path_marker) + r'[^"?#]*)[^"]*"[^>]*>(.*?)</a>',
        re.S | re.I,
    )
    for href, inner in rx.findall(html_text):
        title = html.unescape(re.sub(r"<[^>]+>", " ", inner))
        title = re.sub(r"\s+", " ", title).strip()
        # some boards run marketing copy straight into the link text, e.g.
        # 'Frontend web developer! (Gurugram) "Get paid to break things" READ MORE'
        title = re.split(r'\s*["\u201c\u201d]|\s+#|\s+READ MORE|\s+APPLY', title)[0].strip()
        title = re.sub(r"\s*\{.*$", "", title).strip()
        if not title or len(title) < 3 or len(title) > 140:
            continue
        # same guards the slug path uses — link text can carry a page heading
        # ("Jobs at Amusnet We use cookies...") or stray markup
        if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
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
        r = _request("GET", url, headers=AGENCY_UA)
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
                        str(addr.get(k)).strip() for k in
                        ("addressLocality", "addressRegion", "addressCountry")
                        if isinstance(addr.get(k), str) and str(addr.get(k)).strip()
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


_UUID_RX = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


def _titles_from_urls(urls, source, loc_index=None):
    """Derive a title from a job URL slug — last resort, gives title only."""
    out, seen = [], set()
    for u in urls:
        segs = [x for x in u.rstrip("/").split("/") if x]
        slug = segs[-1] if segs else ""
        # Bally's end their job URLs with a UUID and put the title one segment
        # back: /…/interactive/customer-service-representative/fad57a20-f86a-…
        if _UUID_RX.fullmatch(slug) and len(segs) > 1:
            slug = segs[-2]
        if slug.lower() in _NOT_A_JOB:
            continue
        # agency reference codes first, e.g. "-qvv8x9w6", "-es6649", "-5wryr53r".
        # Must run before the digit rules or those split the code and strand a
        # fragment ("...-es6649" would otherwise leave a trailing "Es").
        slug = re.sub(r"[-_](?=[a-z0-9]{4,10}$)(?=[a-z0-9]*\d)(?=[a-z0-9]*[a-z])[a-z0-9]+$",
                      "", slug, flags=re.I)
        slug = re.sub(r"[-_]?\d{4,}[-_]?\d*$", "", slug)
        slug = re.sub(r"[-_]\d{1,3}$", "", slug)
        slug = re.sub(r"^\d+[-_]+", "", slug)          # leading job id
        words = [w for w in re.split(r"[-_]+", slug) if w]
        if not words or len("".join(words)) < 3:
            continue
        title = " ".join(_FIX_CASE.get(w.title(), w.title()) for w in words)
        if title.lower() in seen or title.strip().lower() in ("null", "none", "undefined", "test"):
            continue
        if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
            continue
        seen.add(title.lower())
        loc = ""
        if loc_index is not None:
            try:
                raw = segs[loc_index]
                loc = " ".join(w.capitalize() for w in re.split(r"[-_]+", raw) if w)
                if len(loc) > 40 or _UUID_RX.fullmatch(raw):
                    loc = ""
            except Exception:
                loc = ""
        out.append({"title": title, "location": loc, "department": "",
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
    """bettingjobs.com — WordPress + Applyflow. Individual roles live under
    /jobview/, which is what the sector pages link out to. The board listing
    itself renders client-side, so the sector pages are the reliable source."""
    base = "https://www.bettingjobs.com"
    MARKERS = ("/jobview/", "/job/", "/jobs/", "/job-category/")

    # 1) sector and listing pages — server-rendered, each carries job links
    pages = [f"{base}/{p}/" for p in (
        "jobs", "job-seekers", "hr-finance", "marketing",
        "executive-senior-appointments", "it-technical", "analytics-bi",
        "commercial", "trading-sportsbook", "operations-payment",
        "compliance-legal", "product",
    )]
    out, seen = [], set()
    for url in pages:
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        except Exception as e:
            print(f"      BettingJobs {url}: error ({type(e).__name__})")
            continue
        if r.status_code != 200:
            print(f"      BettingJobs {url}: HTTP {r.status_code}")
            continue
        hits = []
        for marker in MARKERS:
            hits = _links_with_titles(r.text, base, marker)
            if hits:
                break
        if not hits:
            if url == pages[0]:
                print(f"      BettingJobs {url}: 200, {len(r.text)} bytes")
                print(f"         link shapes present: {_href_shapes(r.text)}")
            continue
        for u, t in hits:
            if u in seen or u.rstrip("/").endswith(("/jobview", "/jobs")):
                continue
            seen.add(u)
            out.append({"title": t, "location": "", "department": "",
                        "url": u, "posted_at": None})
        time.sleep(REQUEST_DELAY)
    if out:
        print(f"   BettingJobs: sector pages -> {len(out)} roles")
        return out

    # 2) sitemap, trying each marker
    for marker in MARKERS:
        urls = _sitemap_job_urls(base, marker)
        if urls:
            print(f"      BettingJobs: sitemap marker {marker} -> {len(urls)}")
            detailed = []
            for u in urls[:120]:
                detailed.extend(_jsonld_jobs(u, "BettingJobs"))
                time.sleep(REQUEST_DELAY)
            if detailed:
                return detailed
            derived = _titles_from_urls(urls, "BettingJobs")
            if derived:
                return derived

    # 3) the API the board calls
    print("   BettingJobs: endpoint discovery")
    jobs = _discover(base, BETTINGJOBS_CANDIDATES, "BettingJobs")
    if jobs:
        return jobs
    hits = _spa_api_hunt(f"{base}/jobs/", "BettingJobs")
    for u in hits:
        if re.search(r"(job|vacanc|search)", u, re.I):
            data = _try_json(u, u)
            if data:
                mapped = _normalise(data, base, "BettingJobs")
                if mapped:
                    print(f"   BettingJobs: bundle API {u} -> {len(mapped)}")
                    return mapped
            time.sleep(REQUEST_DELAY)
    return []


def _vankaizen_api():
    """Van Kaizen run a Nuxt site backed by a PUBLIC API — the app config names
    it outright (apiUrl: https://ats.vankaizen.com/api/public), and the page's
    __NUXT_DATA__ block carries every field as structured data:

        {"slug": "...", "name": "...", "remote": "Remote (regional)",
         "locations": [{"code": "CR", "name": "Costa Rica"}], ...}

    Far better than the markup: their rendered HTML wraps the location in Vue
    comment markers, and the sitemap route gave titles only. Their 74 roles had
    no location at all until this."""
    base = "https://ats.vankaizen.com/api/public"

    def _get(url):
        """One request, returning the list of records or None."""
        try:
            r = _request("GET", url, headers={**AGENCY_UA, "Accept": "application/json"})
        except Exception:
            return None
        if r.status_code != 200:
            return None
        try:
            d = r.json()
        except Exception:
            return None
        items = d
        if isinstance(d, dict):
            for k in ("data", "vacancies", "results", "items", "jobs", "records"):
                if isinstance(d.get(k), list):
                    items = d[k]
                    break
        return items if isinstance(items, list) else None

    def _all(path):
        """Their /roles endpoint returns exactly 12 — the site's own page size,
        not the total. Ask for everything at once first, since a single request
        beats eleven; only fall back to paging if the cap is enforced."""
        first = _get(base + path)
        if not first:
            return []
        for param in ("limit=200", "per_page=200", "pageSize=200", "take=200", "size=200"):
            big = _get(f"{base}{path}?{param}")
            if big and len(big) > len(first):
                print(f"   Van Kaizen: {path}?{param} -> {len(big)} in one request")
                return big
        # paging, then. Stop as soon as a page adds nothing new.
        seen = {json.dumps(x, sort_keys=True) for x in first if isinstance(x, dict)}
        out = list(first)
        for style in ("page={}", "offset={}", "skip={}"):
            got_any = False
            for n in range(2, 14):
                val = n if "page=" in style else (n - 1) * len(first)
                page = _get(f"{base}{path}?{style.format(val)}")
                if not page:
                    break
                fresh = [x for x in page if isinstance(x, dict)
                         and json.dumps(x, sort_keys=True) not in seen]
                if not fresh:
                    break
                for x in fresh:
                    seen.add(json.dumps(x, sort_keys=True))
                out.extend(fresh)
                got_any = True
                time.sleep(REQUEST_DELAY)
            if got_any:
                print(f"   Van Kaizen: {path} paged with {style.split('=')[0]} -> {len(out)}")
                return out
        return out

    # /roles answered with 12 while the site lists ~74, so first-wins settled
    # for a partial endpoint. Try them all and keep the fullest.
    best = []
    for path in ("/vacancies", "/vacancy", "/jobs", "/roles", "/positions"):
        items = _all(path)
        if not items:
            continue

        out = []
        for j in items:
            if not isinstance(j, dict):
                continue
            title = html.unescape(str(j.get("name") or j.get("title") or "")).strip()
            slug = str(j.get("slug") or "").strip()
            if not title or not slug:
                continue
            if j.get("active") is False:
                continue
            locs = j.get("locations")
            names = []
            if isinstance(locs, list):
                for L in locs:
                    n = L.get("name") if isinstance(L, dict) else L
                    if n and str(n).strip():
                        names.append(str(n).strip())
            loc = ", ".join(names)
            if not loc:
                loc = str(j.get("city") or j.get("remote") or "").strip()
            spec = j.get("specialisms")
            dept = ", ".join(str(x) for x in spec) if isinstance(spec, list) else str(spec or "")
            out.append({
                "title": title,
                "location": loc,
                "department": dept[:80],
                "url": f"https://vankaizen.com/vacancies/{slug}",
                "posted_at": j.get("published_at") or j.get("created_at"),
            })
        if len(out) > len(best):
            best = out
            print(f"   Van Kaizen: public API {path} -> {len(out)} roles")
        # once a path has given a real board there is nothing to gain from
        # trying the rest, and each one costs up to nine requests
        if len(best) >= 20:
            break
    return best


def scrape_vankaizen():
    """vankaizen.com — bespoke board with load-more pagination."""
    base = "https://www.vankaizen.com"
    api = _vankaizen_api()
    if api:
        return api
    print("   Van Kaizen: strategy 1 — JSON-LD")
    jobs = _jsonld_jobs(base + "/vacancies", "Van Kaizen")
    if jobs:
        return jobs
    print("   Van Kaizen: strategy 2 — sitemap")
    urls = _sitemap_job_urls(base, "/vacanc")
    if urls:
        detailed, misses = [], 0
        for u in urls[:120]:
            found = _jsonld_jobs(u, "Van Kaizen")
            detailed.extend(found)
            misses = 0 if found else misses + 1
            if misses >= 4 and not detailed:
                print("   Van Kaizen: no JSON-LD on their job pages, skipping the rest")
                break
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


def _spa_api_hunt(base, source, max_bundles=4, prefer=("main", "app", "index", "vendor",
                                                       "runtime", "config", "env")):
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
    # Some apps reference 150+ chunks. Downloading them all is wasteful, but only
    # sampling the first few misses the one holding the API base, so sort the
    # likely candidates to the front and widen the budget when there are many.
    srcs.sort(key=lambda u: (not any(k in u.lower() for k in prefer), len(u)))
    budget = max_bundles if len(srcs) <= 12 else min(30, max_bundles + len(srcs) // 6)
    print(f"      {source}: {len(srcs)} script bundle(s) referenced, checking {budget}")

    found = set()
    for u in srcs[:budget]:
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
    NOISE = ("reactjs.org", "whatwg.org", "w3.org", "electronjs.org", "github.com",
             "npmjs.com", "nodejs.org", "mozilla.org", "jquery.com", "angular.io",
             "vuejs.org", "schema.org", "example.com", "localhost")
    found = {u for u in found if not any(n in u for n in NOISE)}
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
                     "/api/v1/jobs", "/api/adverts", "/api/Job/GetAll",
                     "/api/Job/List", "/api/JobAds", "/api/jobads",
                     "/api/publication", "/api/publications", "/api/offers",
                     "/api/Recruitment/Jobs", "/api/job/search"):
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


def _pageup_style(name, base):
    """Tabcorp and Sportsbet run the same careers platform: a server-rendered
    listing at /jobs/search with individual roles at /jobs/{slug}."""
    out, seen = [], set()
    for page in range(1, 8):
        url = f"{base}/jobs/search?page={page}&query="
        try:
            r = session.get(url, headers=AGENCY_UA, timeout=TIMEOUT)
        except Exception as e:
            print(f"      {name} page {page}: error ({type(e).__name__})")
            break
        if r.status_code != 200:
            print(f"      {name} page {page}: HTTP {r.status_code}")
            break
        new = 0
        for u, t in _links_with_titles(r.text, base, "/jobs/"):
            if u in seen or u.rstrip("/").endswith(("/jobs", "/jobs/search")):
                continue
            seen.add(u)
            out.append({"title": t, "location": "", "department": "",
                        "url": u, "posted_at": None})
            new += 1
        if page == 1 and not new:
            print(f"      {name}: 200, {len(r.text)} bytes")
            print(f"         link shapes present: {_href_shapes(r.text)}")
        if not new:
            break
        time.sleep(REQUEST_DELAY)
    return out


def scrape_sportsbet():
    return _pageup_style("Sportsbet", "https://careers.sportsbet.com.au")


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
    "Codere Online":dict(base="https://codere.hiringroom.com", marker="/jobs/",
                         listing=["/jobs/"], extra=["https://codereargentina.hiringroom.com/jobs/"]),
    "Casumo":       dict(base="https://www.casumocareers.com", marker="/jobs/",
                         listing=["/jobs/"]),
    # their own careers site links "Vacancies" to /home/search — the
    # /JobApplications/ path we were trying appears nowhere on it
    "bet9ja":       dict(base="https://bet9jacareers.com", marker="/home/",
                         listing=["/home/search", "/Home/search", "/"]),
    "Exacta Solutions": dict(base="https://www.exactasolutions.com", marker="/vacancies/",
                         listing=["/vacancies/", "/vacancies/page/2/", "/vacancies/page/3/",
                                  "/vacancies/page/4/", "/vacancies/page/5/"]),
    # Their cards link out with the text "More Details", so link-text titles
    # gave 50 roles all called that. The URL slug carries the real title:
    # /jobs/head-of-acquisition, /jobs/senior-kyc-compliance.
    "TalentBet":    dict(base="https://www.talentbet.com", marker="/jobs/",
                         listing=["/jobs/", "/jobs/page/2/", "/jobs/page/3/", "/vacancies/"],
                         slug_titles=True),
    "iGaming Recruitment": dict(base="https://igamingrecruitment.io", marker="/job",
                         listing=["/jobs/", "/jobs/page/2/", "/vacancies/"]),
    # Flutter's Porto engineering hub. Note greenhouse:blip is a DIFFERENT Blip
    # (a US company) — their real board is on their own site.
    "Blip.pt":      dict(base="https://www.blip.pt", marker="/jobs/",
                         listing=["/jobs/", "/jobs/?page=2", "/jobs/?page=3"]),
    "Legend":       dict(base="https://l1.com", marker="/jobs/",
                         listing=["/jobs", "/jobs/"], first_element=True),
    # Isle of Man social casino studio. No ATS but fully server-rendered:
    # /careers lists each role as a link to /vacancy{N} with a clean title.
    # Employment Hero boards are server-rendered at /jobs/organisations/{org}/
    # with job links at /jobs/position/{slug}/. The link text runs title,
    # location, type and date together, so the slug gives the cleaner title.
    "Newfield":     dict(base="https://employmenthero.com", marker="/jobs/position/",
                         listing=["/jobs/organisations/newfield-ltd/"], slug_titles=True),
    # Their /wp-json/ endpoint hangs (511s of timeouts across ten post types)
    # while huddle.tech itself is fine, so read the careers page directly.
    # Roles live at /careers/{slug}/ and are mostly talent pools.
    "Huddle":       dict(base="https://huddle.tech", marker="/careers/",
                         listing=["/careers/", "/careers"], prefer_sitemap=True),
    # Server-rendered listing, roles at /careers/{slug}. The link text runs
    # title, employment type and seniority together ("Chief Commercial Officer
    # Full-time Senior"), so the slug gives the cleaner title.
    "BGaming":      dict(base="https://bgaming.com", marker="/careers/",
                         listing=["/careers"], slug_titles=True),
    # Their visible listing is client-rendered and yielded 5 roles, one of them
    # "All Offers". The application form's dropdown carries all 46, each with
    # its WordPress post id — /?p=ID resolves to the real permalink.
    "Wazdan":       dict(base="https://wazdan.com", marker="/career/",
                         listing=["/career/all-offers", "/career/", "/career",
                                  "/career/apply", "/kariera"], job_select=True),
    # 9 roles at /career/all, individual roles at /career/{slug}
    "Upgaming":     dict(base="https://lifeat.upgaming.com", marker="/career/",
                         listing=["/career/all", "/career"], slug_titles=True),
    # Yolo Group's B2B aggregator arm, and the only part of that group with a
    # findable board. Server-rendered; the link text is "View vacancy: X" so
    # the slug gives the cleaner title.
    "Hub88":        dict(base="https://hub88.io", marker="/careers/",
                         listing=["/careers/"], slug_titles=True),
    # Their full careers site is 445 roles of mostly land-based resort work.
    # This is the Interactive career area only — the digital business — which
    # is the part worth surfacing. Server-rendered; link text is "View Job" so
    # the title comes from the slug, and the country sits four segments from
    # the end of the URL.
    "Bally's Interactive": dict(base="https://careers.ballys.com", marker="/job/",
                         listing=["/career-areas/interactive"],
                         slug_titles=True, url_loc_index=-4, listing_only=True),
    # PARKED WRONGLY as "Softgarden, unsupported" — that is only where the
    # APPLY button goes. Their listing is fully server-rendered with data-*
    # attributes carrying title, city and full address.
    "MERKUR GROUP":  dict(base="https://merkur.group", marker="stellenanzeige",
                         listing=["/career/our-jobs/job-vacancies/",
                                  "/career/job-vacancies/find-a-job/",
                                  "/en/career/our-jobs/job-vacancies/"],
                         data_attrs=True),
    # PARKED WRONGLY as "PeopleHR, unsupported" — that is only the apply
    # destination. Their listing is server-rendered: title in the anchor,
    # location in a sibling <p class="locations">.
    "Sportingtech": dict(base="https://sportingtech.com", marker="/careers/roles/",
                         listing=["/resources/careers/roles/"], loc_class="locations"),
    # PARKED WRONGLY as "PeopleForce, unsupported" — that is only the apply
    # platform. Their listing is server-rendered: title in the anchor, location
    # in a sibling div carrying PeopleForce's own utility class.
    "NuxGame":      dict(base="https://careers.nuxgame.com", marker="/v/",
                         listing=["/", "/vacancies", "/careers"],
                         loc_class="tw-text-neutral-dark-80"),
    # PARKED WRONGLY as "Humi, unsupported". Server-rendered: title in the
    # anchor, then a sibling div reading "{Location} . {type} . {date}".
    "NSUS Group":   dict(base="https://nsusgroup.applytojobs.ca", marker="/",
                         listing=["/", "/jobs", "/careers"],
                         loc_class="text-xs", loc_split=".",
                         url_pattern=r"/[a-z-]+/\d{3,}$"),
    # PARKED WRONGLY as "TalentAppStore, unsupported". Server-rendered:
    # title in the anchor, location in a sibling div.location. ~32 roles.
    "SkyCity Entertainment Group": dict(base="https://www.skycitycareers.com",
                         marker="/jobdetails/", listing=["/search", "/"],
                         loc_class="location"),
    # PARKED WRONGLY as "PageUp, unsupported" — only the apply destination.
    # Server-rendered table: title in the anchor, location in span.location.
    # NOTE mostly brick-and-mortar venue roles, which the Shops/Casino Floor
    # exclusion hides — the value is the occasional corporate role.
    # NOTE their site answers HTTP 202 to every request — a bot-protection
    # challenge, not a bad path. Left configured in case it lifts, but it is
    # genuinely blocked rather than mis-pointed. Mostly venue roles anyway.
    "The Star Entertainment Group": dict(base="https://careers.star.com.au",
                         marker="/job/", listing=["/en/search", "/en", "/"],
                         loc_class="location"),
    # PARKED WRONGLY as "aptrack + Employment Hero, unsupported". Employment
    # Hero needs no fetcher at all — Newfield has been read off the same path
    # for weeks. Their aptrack side was never the listing.
    # Employment Hero org slugs carry a generated suffix — the real one is in
    # their own JSON-LD: .../organisations/pointsbet-australia-pty-limited-vo6ll/
    "PointsBet":    dict(base="https://employmenthero.com", marker="/jobs/position/",
                         listing=["/jobs/organisations/pointsbet-australia-pty-limited-vo6ll/"],
                         slug_titles=True),
    # Their hrefs are RELATIVE with no leading slash ("Details/7" from
    # /home/search), so URLs join against the listing page. Most of their board
    # is flagged "Application Closed" — skip_if drops those, leaving the live ones.
    "bet9ja":       dict(base="https://bet9jacareers.com", marker="Details/",
                         # ONE listing path only. "/home" has no trailing slash,
                         # so urljoin resolves "Details/12" against the parent and
                         # gives /Details/12 — the same role a second time under a
                         # broken URL.
                         listing=["/home/search"],
                         # NO skip_if. Every one of their 7 listings is flagged
                         # "Application Closed" and dropping them left the board
                         # empty — he wants those 7 carried regardless.
                         loc_icon="fa-map-marker"),
    "KamaGames":    dict(base="https://www.kamagames.com", marker="/vacancy",
                         listing=["/careers"]),
    # also PeopleForce, so the same sibling class should carry the location —
    # their 20 roles have arrived blank until now
    "SoftConstruct": dict(base="https://peopleforce.softconstruct.com", marker="/careers/v/",
                         listing=["/careers/v/", "/careers/", "/careers/v/?page=2"],
                         loc_class="tw-text-neutral-dark-80"),
    "Greentube (Novomatic)": dict(base="https://careers.greentube.com", marker="/job",
                         listing=["/", "/jobs/", "/vacancies/", "/en/jobs/"]),
    # careers.gamesglobal.com is the listing; careers-gamesglobal.icims.com is
    # only the apply gateway, which is why the iCIMS fetcher saw a 148-byte stub
    # The careers page lists only the CURRENT roles and links out to skillie.ai,
    # whose own sitemap carries every job ever posted (ids 2-14667). So use the
    # marketing site as the live filter, and take titles from the slugs since
    # the anchors there are empty.
    "EGT Digital":  dict(base="https://egt-digital.com", marker="skillie.ai/jobs/",
                         listing=["/careers/", "/careers"], slug_titles=True),
    "Games Global": dict(base="https://careers.gamesglobal.com", marker="/jobs/",
                         listing=["/jobs", "/jobs?lang=en-us", "/", "/search"]),
    "Play'n GO":    dict(base="https://talenthub.playngo.com", marker="/job",
                         listing=["/jobs/", "/jobs", "/"]),
    "EvenBet Gaming": dict(base="https://evenbetgaming.com", marker="/vacanc",
                         listing=["/vacancies/", "/vacancies"]),
    "Nolimit City®": dict(base="https://career.nolimitcity.com", marker="/career/",
                         listing=["/", "/career/"]),
    "ARRISE (global)": dict(base="https://arrise.com", marker="/job",
                         listing=["/careers", "/careers/", "/jobs", "/en/careers"]),
    # NOTE: Apercon is now pinned to wordpress:apercon.com in companies.csv,
    # because link-parsing their listing only reached the first 20 roles.
    # This config is kept as a fallback if the REST API is ever closed.
    "Apercon":      dict(base="https://apercon.com", marker="/job",
                         listing=["/jobs/", "/jobs/page/2/", "/jobs/page/3/",
                                  "/jobs/page/4/", "/jobs/page/5/", "/jobs/page/6/",
                                  "/vacancies/", "/"]),
    # Allwyn sit on Recruitis, a Czech ATS. Commercially tied to OPAP, but a
    # completely separate system — the SuccessFactors work doesn't reach them.
    # Recruitis puts each job at /<tenant>/<id>, not /job/<id> — the run log's
    # link-shape report showed "/allwyn/* x10" while "/job" matched nothing
    "Allwyn":       dict(base="https://jobs.recruitis.io", marker="/allwyn/",
                         listing=["/allwyn", "/allwyn/"],
                         extra=["https://www.allwyn.co.uk/job-board"]),
}


# Country names as some boards write them in their own language. MERKUR's
# German addresses end "Österreich" or "Deutschland"; without this the country
# never resolves and the role lands in the catch-all bucket.
_COUNTRY_NATIVE = {
    "osterreich": "Austria", "österreich": "Austria",
    "deutschland": "Germany", "schweiz": "Switzerland",
    "belgien": "Belgium", "niederlande": "Netherlands", "nederland": "Netherlands",
    "spanien": "Spain", "espana": "Spain", "españa": "Spain",
    "italien": "Italy", "italia": "Italy", "frankreich": "France",
    "polen": "Poland", "polska": "Poland", "tschechien": "Czechia",
    "danemark": "Denmark", "dänemark": "Denmark", "danmark": "Denmark",
    "schweden": "Sweden", "sverige": "Sweden", "norwegen": "Norway",
    "grossbritannien": "United Kingdom", "großbritannien": "United Kingdom",
    "vereinigtes konigreich": "United Kingdom", "kroatien": "Croatia",
    "rumanien": "Romania", "rumänien": "Romania", "ungarn": "Hungary",
    "griechenland": "Greece", "turkei": "Turkey", "türkei": "Turkey",
    "brasil": "Brazil", "brasilien": "Brazil", "mexiko": "Mexico",
}


def _native_country(v):
    return _COUNTRY_NATIVE.get((v or "").strip().lower(), (v or "").strip())


def _links_from_data_attrs(html_text, base, cfg):
    """Some boards render each job as a div carrying data-* attributes rather
    than putting anything useful in the anchor. MERKUR do exactly this, and it
    is richer than their markup: title, city and full address all present.

        <div class="job-item" data-title="..." data-city="Raaba"
             data-geo_name="…, 8074 Raaba, Österreich" data-joburl="...">
    """
    out, seen = [], set()
    block_rx = re.compile(r"<div\b[^>]*\bdata-title=[^>]*>", re.I)
    def attr(block, name):
        m = re.search(rf'data-{name}\s*=\s*"([^"]*)"', block, re.I)
        return html.unescape(m.group(1)).strip() if m else ""

    for mt in block_rx.finditer(html_text):
        block = mt.group(0)
        title = attr(block, "title")
        if not title or len(title) < 3 or _MARKUP_JUNK.search(title):
            continue
        href = attr(block, cfg.get("url_attr", "joburl"))
        if href.startswith("http"):
            u = href
        else:
            u = base.rstrip("/") + "/" + re.sub(r"^(\.\./)+", "", href).lstrip("/")
        if u in seen:
            continue
        seen.add(u)
        city = attr(block, "city")
        geo = attr(block, "geo_name")
        country = ""
        if geo and "," in geo:
            country = _native_country(geo.rsplit(",", 1)[-1])
        loc = ", ".join(x for x in (city, country) if x) or city or country
        out.append({"title": title, "location": loc,
                    "department": attr(block, "jobCategories"),
                    "url": u, "posted_at": None})
    return out


def _links_with_loc_class(html_text, base, marker, loc_class,
                          loc_split=None, url_pattern=None):
    """(url, title, location) where the location sits in a sibling element
    carrying a known class, after the job link.

        <h4><a href="/careers/roles/data-engineer/">Data Engineer</a></h4>
        <p class="locations">Sofia</p>

    Sportingtech do this, and it is the third distinct shape after Teamtailor's
    bullet-separated meta and MERKUR's data-* attributes. Bounded at the next
    job link so a window never swallows the following card."""
    body = _STYLE_SCRIPT.sub(" ", html_text)
    link_rx = re.compile(
        r'href="([^"]*?' + re.escape(marker) + r'[^"?#]*)"[^>]*>(.*?)</a>', re.S | re.I)
    loc_rx = re.compile(
        r'<[a-z]+[^>]*class="[^"]*\b' + re.escape(loc_class) + r'\b[^"]*"[^>]*>(.*?)</[a-z]+>',
        re.S | re.I)
    marks = list(link_rx.finditer(body))
    out, seen = [], set()
    for i, mt in enumerate(marks):
        href, inner = mt.group(1), mt.group(2)
        title = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", inner))).strip()
        if not title or len(title) < 3 or len(title) > 140:
            continue
        if _MARKUP_JUNK.search(title) or _PAGE_TITLE.match(title):
            continue
        u = href if href.startswith("http") else base.rstrip("/") + href
        if u in seen or u.rstrip("/") == base.rstrip("/"):
            continue
        # NSUS link their jobs as /{department}/{id}, sharing no path segment,
        # so a fixed marker cannot isolate them — match the shape instead
        if url_pattern and not re.search(url_pattern, u):
            continue
        seen.add(u)
        nxt = marks[i + 1].start() if i + 1 < len(marks) else min(mt.end() + 800, len(body))
        window = body[mt.end():max(nxt, mt.end())][:800]
        lm = loc_rx.search(window)
        loc, posted = "", None
        if lm:
            loc = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", lm.group(1)))).strip()
            loc = _strip_tag_debris(loc)
            if loc_split:
                # "North YorK, Ontario, Canada . full-time . May 7, 2026" —
                # place first, then contract type, then the posting date
                bits = [b.strip() for b in
                        re.split(r"\s+" + re.escape(loc_split) + r"\s+", loc) if b.strip()]
                if bits:
                    loc = bits[0]
                    for b in bits[1:]:
                        d = _sfcsb_date(b)
                        if d:
                            posted = d
                            break
            loc = loc[:60].strip(" ,;.-")
        out.append({"title": title, "location": loc, "department": "",
                    "url": u, "posted_at": posted})
    return out


def _jobs_from_select(html_text, base, cfg):
    """Some sites publish their whole board in a <select> on the application
    form, even when the visible listing is client-rendered and gives up almost
    nothing. Wazdan showed 5 roles to a scraper and 46 in this dropdown:

        <option value="46053">2D Animator (After Effects, Spine 2D)</option>

    The value is the WordPress post ID, and WordPress always resolves /?p=ID
    to the real permalink, so a usable link comes free.

    The right select is found by shape rather than by id — the one carrying the
    most numeric-valued options — because form field ids are generated and
    change whenever the form is edited."""
    best, best_n = None, 0
    for m in re.finditer(r"<select\b[^>]*>(.*?)</select>", html_text, re.S | re.I):
        n = len(re.findall(r'<option[^>]+value="\d+"', m.group(1), re.I))
        if n > best_n:
            best, best_n = m.group(1), n
    if not best or best_n < 3:
        return []

    out, seen = [], set()
    for om in re.finditer(r'<option[^>]*\bvalue="(\d+)"[^>]*>(.*?)</option>',
                          best, re.S | re.I):
        pid = om.group(1)
        title = html.unescape(re.sub(r"<[^>]+>", " ", om.group(2)))
        title = re.sub(r"\s+", " ", title).strip()
        if not title or len(title) < 3 or title.lower() in _NOT_A_JOB:
            continue
        if _MARKUP_JUNK.search(title):
            continue
        key = title.lower()
        if key in seen:          # two "Frontend Developer" rows is one listing
            continue
        seen.add(key)
        out.append({"title": title, "location": "", "department": "",
                    "url": f"{base.rstrip('/')}/?p={pid}", "posted_at": None})
    return out


def _links_with_loc_icon(html_text, listing_url, base, marker, cfg):
    """(url, title, location, date) for cards that mark their location with an
    ICON rather than a class — Font Awesome map pins are everywhere:

        <h5><a href="Details/7">Senior Tax Officer 1</a></h5>
        <li><p><i class="fas fa-map-marker-alt"></i> Head Office</p></li>
        <span class="time">Posted on 3/12/2026</span>

    Two things bet9ja forced. Their hrefs are RELATIVE with no leading slash
    ("Details/7" from /home/search), so URLs are joined against the listing
    page rather than the domain — concatenating onto the base gave
    "bet9jacareers.comDetails/7". And a `skip_if` pattern drops cards flagged
    "Application Closed", which is most of their board."""
    from urllib.parse import urljoin
    body = _STYLE_SCRIPT.sub(" ", html_text)
    icon = cfg.get("loc_icon", "fa-map-marker")
    skip_rx = re.compile(cfg["skip_if"], re.I) if cfg.get("skip_if") else None
    link_rx = re.compile(
        r'href="([^"]*?' + re.escape(marker) + r'[^"?#]*)"[^>]*>(.*?)</a>', re.S | re.I)
    icon_rx = re.compile(r'<i[^>]*class="[^"]*' + re.escape(icon) +
                         r'[^"]*"[^>]*>\s*</i>\s*([^<]{2,60})', re.I)
    date_rx = re.compile(r'Posted on\s*(\d{1,2}/\d{1,2}/\d{4})', re.I)

    marks = list(link_rx.finditer(body))
    out, seen, skipped = [], set(), 0
    for i, mt in enumerate(marks):
        href, inner = mt.group(1), mt.group(2)
        title = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", inner))).strip()
        if not title or len(title) < 3 or _MARKUP_JUNK.search(title):
            continue
        u = urljoin(listing_url, href)
        if u in seen:
            continue
        nxt = marks[i + 1].start() if i + 1 < len(marks) else min(mt.end() + 1400, len(body))
        window = body[mt.end():max(nxt, mt.end())][:1400]
        if skip_rx and skip_rx.search(window):
            skipped += 1
            continue
        seen.add(u)
        lm = icon_rx.search(window)
        loc = html.unescape(lm.group(1)).strip(" \t\n:-") if lm else ""
        dm = date_rx.search(window)
        posted = None
        if dm:
            try:
                d, mo, y = [int(x) for x in dm.group(1).split("/")]
                if mo > 12 and d <= 12:
                    d, mo = mo, d
                posted = f"{y:04d}-{mo:02d}-{d:02d}"
                # A posting date in the future means we read it the wrong way
                # round: bet9ja's "3/12/2026" is 12 March, not 3 December, and
                # left alone it would sort to the top as the newest role on the
                # board. Only swap when the alternative is actually plausible.
                from datetime import date as _date
                today = _date.today()
                if _date(y, mo, d) > today and d <= 12 and mo <= 31:
                    alt = f"{y:04d}-{d:02d}-{mo:02d}"
                    try:
                        if _date(y, d, mo) <= today:
                            posted = alt
                    except ValueError:
                        pass
            except Exception:
                posted = None
        out.append({"title": title, "location": _strip_tag_debris(loc)[:60],
                    "department": "", "url": u, "posted_at": posted})
    if skipped:
        print(f"      dropped {skipped} closed listings")
    return out


def scrape_custom(name):
    """Try, in order: JSON-LD on the listing, the XML sitemap, the SPA bundle's
    API, then tolerant link parsing. Logs which rung worked."""
    cfg = CUSTOM_BOARDS[name]
    base, marker = cfg["base"], cfg["marker"]
    pages = [base.rstrip("/") + p for p in cfg["listing"]] + cfg.get("extra", [])

    if not cfg.get("listing_only"):
        for url in pages:
            ld = _jsonld_jobs(url, name)
            if ld:
                print(f"   {name}: JSON-LD on {url}")
                return ld

    # Bally's sitemap lists all 446 jobs across the group, so the sitemap rung
    # quietly overrode a listing config that was deliberately scoped to one
    # career area. Where the config says so, the listing page is the only
    # source that should be trusted.
    urls = [] if cfg.get("listing_only") else _sitemap_job_urls(base, marker)
    # Boards whose listing renders client-side expose only a stray link or two
    # in the static HTML, so link parsing "succeeds" with a fraction of the
    # roles and the sitemap fallback never runs. Huddle returned 1 of 6 that
    # way. Where the config says so, take the sitemap as the fuller source.
    if urls and cfg.get("prefer_sitemap"):
        derived = _titles_from_urls(urls, name)
        if derived:
            print(f"   {name}: sitemap preferred -> {len(derived)} roles "
                  f"(listing is client-rendered)")
            return derived
    if urls:
        detailed, misses = [], 0
        for u in urls[:80]:
            found = _jsonld_jobs(u, name)
            detailed.extend(found)
            # a site either publishes JobPosting markup or it does not — if the
            # first few pages have none, the other 75 won't either. Van Kaizen
            # spent ~144s a run proving that.
            misses = 0 if found else misses + 1
            if misses >= 4 and not detailed:
                print(f"      {name}: no JSON-LD after {misses} pages, skipping the rest")
                break
            time.sleep(REQUEST_DELAY)
            if len(detailed) >= 80:
                break
        if detailed:
            print(f"   {name}: sitemap + JSON-LD -> {len(detailed)}")
            return detailed
        # deliberately NOT returning derived slugs here — see below

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
            if cfg.get("first_element"):
                found = _links_first_element(r.text, base, marker)
                new_n = 0
                for u, t, loc in found:
                    if u in seen:
                        continue
                    seen.add(u)
                    out.append({"title": t, "location": loc, "department": "",
                                "url": u, "posted_at": None})
                    new_n += 1
                if new_n:
                    print(f"   {name}: first-element titles -> {new_n}")
                continue
            if cfg.get("loc_icon"):
                found = _links_with_loc_icon(r.text, url, base, marker, cfg)
                new_n = 0
                for j in found:
                    if j["url"] in seen:
                        continue
                    seen.add(j["url"])
                    out.append(j)
                    new_n += 1
                if new_n:
                    got = sum(1 for j in out if j["location"])
                    print(f"   {name}: link + icon -> {new_n} roles ({got} with a location)")
                continue
            if cfg.get("job_select"):
                found = _jobs_from_select(r.text, base, cfg)
                new_n = 0
                for j in found:
                    if j["url"] in seen:
                        continue
                    seen.add(j["url"])
                    out.append(j)
                    new_n += 1
                if new_n:
                    print(f"   {name}: application-form select -> {new_n} roles")
                continue
            if cfg.get("loc_class"):
                found = _links_with_loc_class(r.text, base, marker, cfg["loc_class"],
                                              loc_split=cfg.get("loc_split"),
                                              url_pattern=cfg.get("url_pattern"))
                new_n = 0
                for j in found:
                    if j["url"] in seen:
                        continue
                    seen.add(j["url"])
                    out.append(j)
                    new_n += 1
                if new_n:
                    got = sum(1 for j in out if j["location"])
                    print(f"   {name}: link + .{cfg['loc_class']} -> {new_n} roles "
                          f"({got} with a location)")
                continue
            if cfg.get("data_attrs"):
                found = _links_from_data_attrs(r.text, base, cfg)
                new_n = 0
                for j in found:
                    if j["url"] in seen:
                        continue
                    seen.add(j["url"])
                    out.append(j)
                    new_n += 1
                if new_n:
                    got = sum(1 for j in out if j["location"])
                    print(f"   {name}: data attributes -> {new_n} roles "
                          f"({got} with a location)")
                continue
            if cfg.get("slug_titles"):
                # collect the links only, then name them from their slugs
                links = re.findall(r'href="([^"]*' + re.escape(marker) + r'[^"?#]*)', r.text)
                links = [u if u.startswith("http") else base.rstrip("/") + u for u in links]
                derived = _titles_from_urls(sorted(set(links)), name,
                                            loc_index=cfg.get("url_loc_index"))
                for j in derived:
                    if j["url"] in seen:
                        continue
                    seen.add(j["url"])
                    out.append(j)
                if derived:
                    print(f"   {name}: listing page -> {len(derived)} current roles")
                continue
            found = _links_with_titles(r.text, base, marker)
            if not found:
                print(f"      {name} {url}: 200, {len(r.text)} bytes, "
                      f"{r.text.count(marker)} '{marker}' refs")
                print(f"         link shapes present: {_href_shapes(r.text)}")
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

    # last resort: titles derived from sitemap URLs. Unverified, and a sitemap
    # often lists closed roles too, so this only runs when nothing else worked.
    if urls:
        derived = _titles_from_urls(urls, name)
        if derived:
            print(f"   {name}: sitemap slugs -> {len(derived)} (unverified, may include closed roles)")
            return derived
    return out


AGENCY_BOARDS = {
    "Fortuna Entertainment Group": scrape_fortuna,
    **{n: (lambda n=n: scrape_custom(n)) for n in CUSTOM_BOARDS},
    "Tabcorp": scrape_tabcorp,
    "Sportsbet": scrape_sportsbet,
    "Pentasia": scrape_pentasia,
    "BettingJobs": scrape_bettingjobs,
    "Van Kaizen": scrape_vankaizen,
}


# Physical racing roles — stable, yard, raceday and track staff. These are
# dropped at collection rather than being made hideable in the app: the board
# exists to surface digital and commercial roles, and no user of it wants a
# stablehand vacancy. Same call as removing land-based casino resorts.
# Deliberately NOT the bare word "racing", which would catch Head of Racing.
_RACING_PHYSICAL = re.compile(
    r"\b(stable\s*hand|stablehand|stable\s*staff|stud\s*(hand|groom|farm)|groom|"
    r"head\s*(lad|girl)|work\s*rider|exercise\s*rider|jockey|farrier|trackwork|"
    r"track\s*work|stalls\s*handler|yard\s*(person|staff|hand)|raceday|race\s*day|"
    r"turnstile|groundstaff|groundsperson|racecourse\s*(steward|attendant)|"
    r"horsebox|equine\s*(care|therapist)|riding\s*out|point[- ]to[- ]point|"
    # the gender-neutral yard titles the first pass missed: a "Head Person" or
    # "Travelling Head Person" is a stable role, same as head lad
    r"head\s*person|second\s*person|travelling\s*head|gallops?|"
    r"gardener|caretaker|groundsman|groundskeeper|stalls|"
    r"racing\s*secretary|yard\s*manager)\b", re.I)

# a senior seat that merely names the function is not a yard job
_RACING_KEEP = re.compile(
    r"\b(chief|c[eofmpir]o|managing director|general manager|vice president|\bvp\b|"
    r"head of|director of|senior director|head,)\b", re.I)


def _is_physical_racing(title):
    t = title or ""
    return bool(_RACING_PHYSICAL.search(t)) and not _RACING_KEEP.search(t)


# A company that errored this run should not silently vanish from the feed.
# Workable throttling wiped seven companies and ~90 roles in one go; those roles
# hadn't been withdrawn, we just couldn't reach them. Carry the previous run's
# roles forward for a few days, then let them go.
_CARRY_DAYS = 3


# Boards keep postings up long after they're filled. The oldest in the feed was
# dated Sept 2015 — nearly eleven years. His call: drop anything over two years.
# Only ever applies to roles that carry a date; undated postings are untouched,
# since an absent date says nothing about whether the role is live.
_MAX_AGE_DAYS = 730


# Expression-of-interest listings are evergreen by design — a company leaves one
# up for years on purpose, so an old date says nothing about whether it's live.
# They stay in the feed regardless of age, and carry an "open application" badge.
_OPEN_APPLICATION = re.compile(
    r"\btalent\s*(pool|community|network|bank)\b|\bopen\s+application\b"
    r"|\bspeculative\b|\bgeneral\s+interest\b|\bexpression\s+of\s+interest\b"
    r"|\bfuture\s+opportunit\w*\b|\bregister\s+your\s+interest\b"
    r"|\bjoin\s+our\s+talent\b", re.I)


def _too_old(job):
    if _OPEN_APPLICATION.search(job.get("title") or ""):
        return False
    p = job.get("posted_at")
    if not p:
        return False
    try:
        d = datetime.fromisoformat(str(p).replace("Z", "+00:00"))
    except Exception:
        return False
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - d).days > _MAX_AGE_DAYS


def _carry_forward(all_jobs, feed_path, skipped=(), known=None):
    """Re-add roles for companies that returned nothing this run but had roles
    in the last feed. Only applies where the company produced before, so a board
    that is genuinely empty still shows as empty.

    Companies deliberately set to skip are excluded: they return nothing by
    design, and carrying them forward would resurrect rows that were removed
    on purpose.

    So are companies that no longer exist in companies.csv. Renaming a row —
    "Caesars Digital" became "Caesars Entertainment" — left the old name with
    no CSV entry, so it looked exactly like a board that had gone down and
    1,421 roles were carried forward under BOTH names. A company that has gone
    from the list has not gone unreachable; it has gone."""
    try:
        with open(feed_path, encoding="utf-8") as f:
            prev = json.load(f).get("jobs") or []
    except Exception:
        return all_jobs
    if not prev:
        return all_jobs

    now = datetime.now(timezone.utc)
    have = {j["company"] for j in all_jobs}
    by_co = {}
    for j in prev:
        by_co.setdefault(j["company"], []).append(j)

    skipped = set(skipped)
    known = set(known) if known else None
    carried = 0
    for co, jobs in by_co.items():
        if co in skipped:
            print(f"   not carrying {co} forward — set to skip")
            continue
        if known is not None and co not in known:
            print(f"   not carrying {co} forward — no longer in companies.csv "
                  f"(renamed or removed)")
            continue
        if co in have:
            continue
        kept = []
        for j in jobs:
            first = j.get("carried_since") or now.isoformat()
            try:
                age = (now - datetime.fromisoformat(first)).days
            except Exception:
                age = 0
            if age < _CARRY_DAYS:
                j = dict(j)
                j["carried_since"] = first
                kept.append(j)
        if kept:
            all_jobs.extend(kept)
            carried += len(kept)
            print(f"   carried forward {len(kept)} roles for {co} (unreachable this run)")
    if carried:
        print(f"\ncarried {carried} roles forward from the previous feed")
    return all_jobs


def _stamp_first_seen(all_jobs, feed_path):
    """Record the date Role Radar FIRST saw each role, and carry it forward.

    This is a better basis for "what's new" than posted_at, which only 65% of
    roles carry — several platforms publish no date at all. first_seen exists
    for every role and answers the question a returning visitor actually has:
    what has appeared since I last looked.

    Date only, not a timestamp: the app needs day granularity, and 7,000 full
    ISO strings would add a quarter of a megabyte to every commit.

    Only works forward. On the first run with this every role looks new, so
    let it accumulate for a few days before exposing a filter on it."""
    prev = {}
    try:
        with open(feed_path, encoding="utf-8") as f:
            for j in json.load(f).get("jobs") or []:
                u, fs = j.get("url"), j.get("first_seen")
                if u and fs:
                    prev[u] = fs
    except Exception:
        pass

    today = datetime.now(timezone.utc).date().isoformat()
    fresh = 0
    for j in all_jobs:
        seen = prev.get(j.get("url"))
        if seen:
            j["first_seen"] = seen
        else:
            j["first_seen"] = today
            fresh += 1

    if not prev:
        print(f"\nfirst_seen: stamped all {len(all_jobs)} roles with {today} — "
              f"no prior dates, so this is the first run carrying it")
    else:
        print(f"\nfirst_seen: {fresh} roles new since the last run, "
              f"{len(all_jobs) - fresh} carried an existing date")
    return all_jobs


def _write_company_status(companies, all_jobs, cache):
    """Refresh the returning / roles / status columns in companies.csv so the
    file always shows which companies actually produced roles on the last run.
    Written via a temp file and renamed, so a failure can't leave the collector's
    own input truncated."""
    counts = {}
    for j in all_jobs:
        counts[j["company"]] = counts.get(j["company"], 0) + 1

    fields = ["company", "category", "returning", "roles", "status",
              "ats_hint", "ats_token", "notes"]
    extra = [k for k in (companies[0].keys() if companies else []) if k not in fields]

    for c in companies:
        name = c["company"].strip()
        n = counts.get(name, 0)
        ats = (cache.get(name) or {}).get("ats")
        c["roles"] = str(n)
        c["returning"] = "yes" if n else "no"
        if n:
            c["status"] = "producing"
        elif (c.get("ats_token") or "").strip().lower() in ("skip", "none", "-") or ats == "skip":
            c["status"] = "skipped on purpose"
        elif ats == "workday_pending":
            c["status"] = "needs Workday URL"
        elif ats == "unknown":
            c["status"] = "no board found"
        elif ats is None:
            c["status"] = "not probed yet"
        else:
            c["status"] = f"{ats} board found but empty"

    rows = sorted(companies, key=lambda r: (r["returning"] == "yes",
                                            r["status"], r["company"].lower()))
    tmp = COMPANIES_CSV.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields + extra, lineterminator="\r\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: (r.get(k) or "") for k in fields + extra})
    tmp.replace(COMPANIES_CSV)
    yes = sum(1 for r in rows if r["returning"] == "yes")
    print(f"\ncompanies.csv refreshed: {yes} returning, {len(rows)-yes} not")


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
        t0 = time.time()
        try:
            result = detect(name, hint)
        except Exception as e:
            print(f"  ! detect failed for {name}: {type(e).__name__}: {e}")
            result = None
        took = time.time() - t0
        if took > 20:
            print(f"  .. {name} took {took:.0f}s to detect")
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
        try:
            result = detect(name, hints.get(name, ""))
        except Exception as e:
            print(f"  ! re-probe failed for {name}: {type(e).__name__}: {e}")
            result = None
        cache[name] = result or {"ats": "unknown", "checked": now}

    CACHE_FILE.write_text(json.dumps(cache, indent=1))

    # --- fetch pass ---
    # Detection is speculative and may have tripped the circuit breaker on a
    # host. The pins that follow are known-good, so give them a clean slate.
    _host_429.clear()
    global _rate_limit_spent
    _rate_limit_spent = 0.0

    all_jobs = []
    for c in companies:
        name = c["company"].strip()
        info = cache.get(name) or {}
        ats = info.get("ats")
        _t0 = time.time()
        try:
            if ats == "workday":
                jobs = fetch_workday(info["url"])
            elif ats in FETCHERS:
                jobs = FETCHERS[ats](info["token"])
            else:
                continue
            lb = (c.get("business_model") or "").strip().lower() == "land-based"
            for j in jobs:
                j.update(company=name, ats=ats, source="collector")
                # only stamped when true, so the flag costs nothing on the 96%
                # of companies that are digital
                if lb:
                    j["land_based"] = True
            all_jobs.extend(jobs)
            took = time.time() - _t0
            slow = f"  [{took:.0f}s]" if took > 20 else ""
            print(f"{name}: {len(jobs)} roles ({ats}){slow}")
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"{name}: FAILED ({e})")

    # --- agency boards (recruiter sites, no public ATS) ---
    for name, fn in AGENCY_BOARDS.items():
        try:
            jobs = _drop_junk(fn(), name)
            lb = next((( r.get("business_model") or "").strip().lower() == "land-based"
                       for r in companies if r["company"].strip() == name), False)
            for j in jobs:
                j.update(company=name, ats="agency", source="agency")
                if lb:
                    j["land_based"] = True
            all_jobs.extend(jobs)
            print(f"{name}: {len(jobs)} roles (agency board)"
                  + ("  <-- CHECK: returned nothing" if not jobs else ""))
        except Exception as e:
            print(f"{name}: FAILED ({e})")

    # Warn when several company rows resolve to the same board — that's either a
    # duplicate entry or a slug that matched somebody else's careers site.
    # Include the first path segment: apply.workable.com and greenhouse are
    # multi-tenant by design, so the host alone gives constant false alarms.
    # It's the host+tenant pair that signals a duplicate or wrong match.
    boards = {}
    for j in all_jobs:
        m = re.match(r"https?://([^/]+)((?:/[^/?#]*){1,2})?", j.get("url") or "")
        if m:
            # two path segments, not one: ats.rippling.com/en-GB is a locale,
            # so the tenant only appears in the segment after it. Comparing on
            # one segment flagged BetMakers and White Hat every run as sharing
            # a board when their tenants are betmakers and whitehatgaming.
            key = m.group(1) + (m.group(2) or "")
            boards.setdefault(key, set()).add(j["company"])
    for host, cos in boards.items():
        if len(cos) > 1:
            print(f"!! {host} is serving {len(cos)} company rows: {', '.join(sorted(cos))}")

    # Clean every title HERE, not in _drop_junk. _drop_junk is only called on
    # the custom-board and agency path, so the reference-stripping added for
    # Emerald Zebra never ran on them — they come through the WordPress TOKEN
    # fetcher. One central pass catches every source, whichever route it took.
    fixed_t = fixed_l = 0
    for j in all_jobs:
        t = j.get("title") or ""
        c = _clean_title(t)
        if c and c != t:
            j["title"] = c
            fixed_t += 1
        # and the same for locations baked into a title clause
        loc = (j.get("location") or "").strip()
        nl = _loc_from_title(j.get("title") or "", loc)
        if nl and nl != loc:
            j["location"] = nl
            fixed_l += 1
    if fixed_t or fixed_l:
        print(f"\ntitles cleaned: {fixed_t} trailing references stripped, "
              f"{fixed_l} locations taken from the title")

    # "skip" means two different things in the CSV: a company we do not want to
    # fetch, and one whose listing is read by a custom board instead of a token.
    # Only the first should be barred from the carry-forward. Huddle and iGaming
    # Recruitment both timed out one morning and vanished from the feed entirely,
    # because their custom-board rows read as deliberately skipped.
    deliberately_skipped = {
        (c.get("company") or "").strip() for c in companies
        if (c.get("ats_token") or "").strip().lower() in ("skip", "none", "-")
        and (c.get("company") or "").strip() not in CUSTOM_BOARDS
        and (c.get("company") or "").strip() not in AGENCY_BOARDS}
    all_jobs = _carry_forward(all_jobs, FEED_FILE, deliberately_skipped,
                              known={c["company"].strip() for c in companies})
    all_jobs = _stamp_first_seen(all_jobs, FEED_FILE)

    before = len(all_jobs)
    all_jobs = [j for j in all_jobs if not _too_old(j)]
    if before != len(all_jobs):
        print(f"\ndropped {before - len(all_jobs)} postings older than "
              f"{_MAX_AGE_DAYS // 365} years")

    before = len(all_jobs)
    all_jobs = [j for j in all_jobs if not _is_physical_racing(j.get("title"))]
    if before != len(all_jobs):
        print(f"\ndropped {before - len(all_jobs)} physical racing roles "
              f"(stable, yard, raceday, track)")

    try:
        _write_company_status(companies, all_jobs, cache)
    except Exception as e:
        print(f"!! company status write failed: {type(e).__name__}: {e}")

    feed = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "job_count": len(all_jobs),
        "jobs": all_jobs,
    }
    # written compact, not pretty-printed. Nothing reads this by eye, and at
    # ~7,000 roles the indentation alone was 320KB of every Pages deployment
    # and of every commit in the repository's history.
    FEED_FILE.write_text(json.dumps(feed, separators=(",", ":")))
    unknown = [n for n, v in cache.items() if v.get("ats") in ("unknown", "workday_pending")]
    print(f"\nFeed written: {len(all_jobs)} jobs")
    print(f"Unresolved companies ({len(unknown)}): {', '.join(unknown[:30])}")


class _Tee:
    """Write to several streams at once."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for st in self.streams:
            st.write(data)

    def flush(self):
        for st in self.streams:
            st.flush()


if __name__ == "__main__":
    # Mirror the whole run into docs/run-log.txt. The workflow commits docs/,
    # so the diagnostics end up published alongside the feed instead of being
    # stranded in the Actions console.
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(_Tee(sys.stdout, buf)):
            main()
    finally:
        stamp = datetime.now(timezone.utc).isoformat()
        (DOCS / "run-log.txt").write_text(
            f"role-radar collector run: {stamp}\n{'=' * 60}\n\n" + buf.getvalue(),
            encoding="utf-8",
        )
