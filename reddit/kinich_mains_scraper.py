import argparse, base64, csv, json, os, re, sys, time
import urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

SUB = "kinich_mains"
DEFAULT_UA = "script:kinich_mains_census:v2.0 (by /u/USERNAME)"
UA = os.environ.get("REDDIT_USER_AGENT", DEFAULT_UA)
PULLPUSH = "https://api.pullpush.io/reddit/search/submission/"

FLAIR_GROUPS = {
    "art | oc": "Fan Art", "art | non-oc": "Fan Art", "art | non oc": "Fan Art",
    "fluff | memes": "Meme", "fluff/memes": "Meme", "art": "Fan Art", "fan art": "Fan Art",
    "cosplay": "Cosplay", "meme": "Meme", "memes": "Meme", "humor": "Meme",
}
HEURISTICS = [
    ("Cosplay", re.compile(r"\bcosplay(s|ed|ing)?\b", re.I)),
    ("Fan Art", re.compile(r"\b(fan ?art|art|drawing|drew|sketch|painting|illustration|oc)\b", re.I)),
    ("Meme", re.compile(r"\b(memes?|shitpost)\b", re.I)),
    ("Questions | Help", re.compile(r"\b(help|question|how (do|to)|should i|build)\b|\?", re.I)),
]


def ts_str(ts) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError):
        return "N/A"


def http_json(url: str, headers=None, data: Optional[bytes] = None, tries: int = 5,
              timeout: int = 20) -> Tuple[Optional[Any], Any]:
    h = {"User-Agent": UA}
    h.update(headers or {})
    delay = 2.0
    for attempt in range(1, tries + 1):
        try:
            req = urllib.request.Request(url, headers=h, data=data)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8")), r.headers
        except urllib.error.HTTPError as e:
            if e.code not in (422, 429) and e.code < 500:
                print(f"    [-] HTTP {e.code} (no retry): {url[:90]}")
                return None, None
            msg = f"HTTP {e.code}"
        except Exception as e:
            msg = repr(e)
        if attempt < tries:
            print(f"    [!] {msg}; retry {attempt}/{tries - 1} in {delay:.0f}s")
            time.sleep(delay)
            delay *= 2
        else:
            print(f"    [-] {msg}; giving up after {tries} tries")
    return None, None


def fetch_pullpush(sub: str = SUB) -> Tuple[Dict[str, dict], bool]:
    print(f"[*] Stage 1a: Pullpush archive for r/{sub}")
    posts: Dict[str, dict] = {}
    before = int(time.time()) + 1
    page = 0
    while True:
        q = urllib.parse.urlencode({"subreddit": sub, "size": 100, "before": before,
                                    "sort": "desc", "sort_type": "created_utc"})
        data, _ = http_json(f"{PULLPUSH}?{q}")
        if data is None:
            print("[!] Pullpush failed mid-way -> dataset marked INCOMPLETE")
            return posts, False
        batch = data.get("data", [])
        new = [p for p in batch if p.get("id") and p["id"] not in posts]
        for p in new:
            posts[p["id"]] = p
        page += 1
        if not new:
            break
        oldest = min(int(p.get("created_utc", before)) for p in batch)
        print(f"    page {page}: +{len(new)} (total {len(posts)}, oldest {ts_str(oldest)})")
        nb = oldest + 1
        if nb >= before:
            nb = oldest
        before = nb
        time.sleep(1.0)
    print(f"[+] Pullpush done: {len(posts)} unique IDs")
    return posts, True


ARCTIC = "https://arctic-shift.photon-reddit.com/api/posts/search"


def fetch_arctic(sub: str = SUB) -> Tuple[Dict[str, dict], bool]:
    print(f"[*] Stage 1a: Arctic Shift archive for r/{sub}")
    posts: Dict[str, dict] = {}
    before = int(time.time()) + 1
    page = 0
    while True:
        q = urllib.parse.urlencode({"subreddit": sub, "limit": 100, "before": before, "sort": "desc"})
        data, _ = http_json(f"{ARCTIC}?{q}")
        if data is None or data.get("error"):
            print(f"[!] Arctic Shift failed mid-way -> INCOMPLETE ({data and data.get('error')})")
            return posts, False
        batch = data.get("data") or []
        new = [p for p in batch if p.get("id") and p["id"] not in posts]
        for p in new:
            posts[p["id"]] = p
        page += 1
        if not new:
            break
        oldest = min(int(p.get("created_utc", before)) for p in batch)
        print(f"    page {page}: +{len(new)} (total {len(posts)}, oldest {ts_str(oldest)})")
        nb = oldest + 1
        before = oldest if nb >= before else nb
        time.sleep(0.5)
    print(f"[+] Arctic Shift done: {len(posts)} unique IDs")
    return posts, True


class Reddit:
    def __init__(self, cid: str, secret: str):
        self.cid, self.secret, self.token, self.exp = cid, secret, None, 0

    def auth(self) -> bool:
        cred = base64.b64encode(f"{self.cid}:{self.secret}".encode()).decode()
        data, _ = http_json("https://www.reddit.com/api/v1/access_token",
                            {"Authorization": f"Basic {cred}"},
                            data=b"grant_type=client_credentials", tries=3)
        if not data or "access_token" not in data:
            print(f"[-] Reddit OAuth failed: {data}")
            return False
        self.token, self.exp = data["access_token"], time.time() + data.get("expires_in", 3600) - 60
        return True

    def get(self, path: str):
        if time.time() > self.exp and not self.auth():
            return None
        data, hdr = http_json("https://oauth.reddit.com" + path,
                              {"Authorization": f"bearer {self.token}"})
        if hdr is not None:
            try:
                rem = float(hdr.get("X-Ratelimit-Remaining", 100))
                reset = float(hdr.get("X-Ratelimit-Reset", 0))
                if rem < 2:
                    print(f"    [~] rate limit low, sleeping {reset:.0f}s")
                    time.sleep(reset + 1)
            except ValueError:
                pass
        return data

    def listing_new(self, sub: str = SUB) -> Tuple[List[str], bool]:
        print("[*] Stage 1b: Reddit official /new listing (max ~1000)")
        ids, after = [], None
        while True:
            q = {"limit": 100, "raw_json": 1}
            if after:
                q["after"] = after
            d = self.get(f"/r/{sub}/new?{urllib.parse.urlencode(q)}")
            if d is None:
                return ids, False
            ch = d.get("data", {}).get("children", [])
            ids += [c["data"]["id"] for c in ch]
            after = d.get("data", {}).get("after")
            if not after or not ch:
                break
        print(f"[+] Listing: {len(ids)} IDs")
        return ids, True

    def hydrate(self, ids: List[str]) -> Tuple[Dict[str, dict], bool]:
        print(f"[*] Stage 2: hydrating {len(ids)} IDs via /api/info")
        out, ok = {}, True
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            d = self.get("/api/info?raw_json=1&id=" + ",".join("t3_" + x for x in chunk))
            if d is None:
                ok = False
                continue
            for c in d.get("data", {}).get("children", []):
                out[c["data"]["id"]] = c["data"]
            print(f"    {min(i + 100, len(ids))}/{len(ids)}")
        return out, ok


def removal_status(p: dict) -> str:
    if p.get("removed_by_category"):
        return str(p["removed_by_category"])
    if p.get("selftext") in ("[removed]", "[deleted]") or p.get("author") == "[deleted]":
        return "deleted_or_removed"
    return ""


def classify(p: dict) -> Tuple[str, str, bool]:
    flair = (p.get("link_flair_text") or "").strip()
    if flair:
        return flair, FLAIR_GROUPS.get(flair.lower(), "Other"), False
    title = p.get("title") or ""
    for grp, rx in HEURISTICS:
        if rx.search(title):
            return f"(Unflaired, inferred: {grp})", grp if grp in ("Fan Art", "Cosplay", "Meme") else "Other", True
    return "(Unflaired)", "Other", True


def analyze_posts(posts: List[dict], exclude_removed: bool = False) -> Dict[str, Any]:
    removed = [p for p in posts if removal_status(p)]
    use = [p for p in posts if not (exclude_removed and removal_status(p))]
    agg = defaultdict(lambda: {"count": 0, "up": 0, "com": 0, "removed": 0, "group": "", "inferred": False, "top": []})
    groups = defaultdict(int)
    for p in use:
        label, grp, inf = classify(p)
        a = agg[label]
        a["count"] += 1; a["group"] = grp; a["inferred"] = inf
        a["up"] += int(p.get("score") or 0); a["com"] += int(p.get("num_comments") or 0)
        a["removed"] += bool(removal_status(p))
        a["top"].append(p)
        groups[grp] += 1
    n = len(use)
    rows = []
    for label, a in sorted(agg.items(), key=lambda kv: -kv[1]["count"]):
        top = sorted(a["top"], key=lambda p: -(p.get("score") or 0))[:3]
        rows.append({
            "category": label, "group": a["group"], "inferred": a["inferred"], "count": a["count"],
            "removed": a["removed"], "share_percentage": round(a["count"] / n * 100, 2) if n else 0.0,
            "total_upvotes": a["up"], "avg_upvotes": round(a["up"] / a["count"], 1),
            "total_comments": a["com"], "avg_comments": round(a["com"] / a["count"], 1),
            "top_posts": [{"title": t.get("title"), "author": t.get("author"), "score": t.get("score"),
                           "url": "https://www.reddit.com" + (t.get("permalink") or f"/comments/{t.get('id')}")}
                          for t in top],
        })
    ts = [float(p["created_utc"]) for p in use if p.get("created_utc")]
    return {"total_posts": n, "total_including_removed": len(posts), "removed_count": len(removed),
            "exclude_removed": exclude_removed,
            "earliest_post_date": ts_str(min(ts)) if ts else "N/A",
            "latest_post_date": ts_str(max(ts)) if ts else "N/A",
            "group_counts": dict(groups), "category_stats": rows}


def print_report(stats: Dict[str, Any], meta: Dict[str, Any]):
    W = 100
    print("\n" + "=" * W)
    print(f" r/{SUB} POST & FLAIR STATISTICS (best-effort; see metadata)")
    print(f" Posts analyzed: {stats['total_posts']} | removed/deleted flagged: {stats['removed_count']}"
          f" ({'excluded' if stats['exclude_removed'] else 'included'})")
    print(f" Span: {stats['earliest_post_date']} -> {stats['latest_post_date']}")
    if not meta.get("complete", False):
        print(" !!! DATASET INCOMPLETE: " + "; ".join(meta.get("warnings", [])))
    elif meta.get("warnings"):
        print(" Notes: " + "; ".join(meta["warnings"]))
    print("=" * W)
    print(f"{'Flair':<34} | {'Group':<8} | {'Count':>6} | {'Share%':>7} | {'AvgUp':>8} | {'AvgCom':>7} | {'Rm':>3}")
    print("-" * W)
    for r in stats["category_stats"]:
        print(f"{r['category'][:34]:<34} | {r['group']:<8} | {r['count']:>6} | {r['share_percentage']:>7.2f} | "
              f"{r['avg_upvotes']:>8.1f} | {r['avg_comments']:>7.1f} | {r['removed']:>3}")
    print("-" * W)
    n = stats["total_posts"] or 1
    g = stats["group_counts"]
    fan = sum(g.get(k, 0) for k in ("Fan Art", "Cosplay", "Meme"))
    print(f"[*] Fan works (Fan Art + Cosplay + Meme): {fan} ({fan / n * 100:.2f}%)")
    for k in ("Fan Art", "Cosplay", "Meme", "Other"):
        print(f"    - {k}: {g.get(k, 0)}")
    print("    (rows labelled 'inferred' come from title keywords, not real flairs)")
    print("=" * W + "\n")


def export_csv(stats: Dict[str, Any], filename: str):
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Flair", "Group", "Inferred (unflaired)", "Total Posts", "Share (%)", "Removed/Deleted",
                    "Total Upvotes", "Avg Upvotes/Post", "Total Comments", "Avg Comments/Post", "Top post URL"])
        for r in stats["category_stats"]:
            w.writerow([r["category"], r["group"], r["inferred"], r["count"], r["share_percentage"], r["removed"],
                        r["total_upvotes"], r["avg_upvotes"], r["total_comments"], r["avg_comments"],
                        r["top_posts"][0]["url"] if r["top_posts"] else ""])
    print(f"[+] CSV -> {filename}")


def main():
    ap = argparse.ArgumentParser(description="r/kinich_mains post & flair statistics (Pullpush IDs + Reddit API)")
    ap.add_argument("--exclude-removed", action="store_true", help="exclude removed/deleted posts from main table")
    ap.add_argument("--no-listing", action="store_true", help="don't merge Reddit /new listing IDs")
    ap.add_argument("--skip-hydrate", "--dry-run", dest="skip", action="store_true",
                    help="no Reddit credentials; use archive data (scores not live)")
    ap.add_argument("--source", choices=["arctic", "pullpush"], default="arctic", help="ID/archive source")
    ap.add_argument("--output-csv", default="kinich_mains_flair_stats.csv")
    ap.add_argument("--save-json", default="kinich_mains_posts_raw.json")
    ap.add_argument("--save-meta", default="kinich_mains_metadata.json")
    a = ap.parse_args()

    warnings, complete = [], True
    meta: Dict[str, Any] = {"fetched_at": datetime.now(timezone.utc).isoformat(), "subreddit": SUB,
                            "sources": ["pullpush"], "user_agent": UA}
    have_cred = bool(os.environ.get("REDDIT_CLIENT_ID") and os.environ.get("REDDIT_CLIENT_SECRET"))
    if not have_cred:
        a.skip = True
    src = a.source
    meta["sources"] = [src]
    pp, ok = (fetch_arctic if src == "arctic" else fetch_pullpush)()
    meta[f"{src}_ids"] = len(pp)
    if not ok:
        complete = False; warnings.append(f"{src} fetch failed/interrupted")

    reddit = None
    if not a.skip:
        cid, sec = os.environ.get("REDDIT_CLIENT_ID"), os.environ.get("REDDIT_CLIENT_SECRET")
        if not (cid and sec):
            sys.exit("[-] Set REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET, or use --skip-hydrate.")
        if UA == DEFAULT_UA:
            print("[!] Please set REDDIT_USER_AGENT with your Reddit username (Reddit API rules).")
        reddit = Reddit(cid, sec)
        if not reddit.auth():
            sys.exit("[-] OAuth failed; check credentials.")

    ids = list(pp)
    if reddit and not a.no_listing:
        lid, lok = reddit.listing_new()
        meta["sources"].append("reddit_listing_new")
        meta["listing_ids"] = len(lid)
        extra = [i for i in lid if i not in pp]
        meta["listing_only_ids"] = len(extra)
        ids += extra
        if not lok:
            warnings.append("Reddit /new listing failed")

    if not ids:
        meta.update(complete=False, warnings=warnings + ["no IDs retrieved"])
        with open(a.save_meta, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        sys.exit("[-] No posts retrieved (Pullpush down and no listing). Metadata saved.")

    if reddit:
        meta["sources"].append("reddit_api_info")
        hyd, hok = reddit.hydrate(ids)
        missing = [i for i in ids if i not in hyd]
        meta["hydrated"] = len(hyd); meta["missing_after_hydrate"] = len(missing)
        if not hok:
            complete = False; warnings.append("some /api/info batches failed")
        if missing:
            warnings.append(f"{len(missing)} IDs not returned by Reddit (using Pullpush snapshot for them)")
        posts = [hyd.get(i) or dict(pp.get(i, {}), _snapshot_only=True) for i in ids if (i in hyd or i in pp)]
        for p in posts:
            p.setdefault("_snapshot_only", False)
    else:
        posts = list(pp.values())
        meta["hydrated"] = 0
        refetched = sum(1 for p in posts if (p.get("_meta") or {}).get("retrieved_2nd_on"))
        meta["score_refetched_count"] = refetched
        meta["score_refetched_share_pct"] = round(refetched / len(posts) * 100, 2) if posts else 0
        meta["score_source_per_post"] = {p["id"]: ("refetch" if (p.get("_meta") or {}).get("retrieved_2nd_on") else "first_ingest") for p in posts}
        warnings.append(f"no Reddit API hydration: score/num_comments are archive values, not live "
                        f"({meta['score_refetched_share_pct']}% from Arctic Shift re-fetch)")
    posts.sort(key=lambda p: -float(p.get("created_utc") or 0))
    meta.update(complete=complete, warnings=warnings, total_posts=len(posts))

    stats = analyze_posts(posts, a.exclude_removed)
    stats_other = analyze_posts(posts, not a.exclude_removed)
    meta["count_including_removed"] = stats["total_including_removed"]
    meta["count_excluding_removed"] = stats["total_including_removed"] - stats["removed_count"]
    print_report(stats, meta)
    print(f"[i] Posts incl. removed: {meta['count_including_removed']} | excl. removed: {meta['count_excluding_removed']}")
    export_csv(stats, a.output_csv)
    with open(a.save_json, "w", encoding="utf-8") as f:
        json.dump({"metadata": meta, "stats": stats, "stats_alt_removed_setting": stats_other, "posts": posts},
                  f, ensure_ascii=False, indent=2)
    with open(a.save_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"[+] Raw JSON -> {a.save_json}; metadata -> {a.save_meta}")
    if not complete:
        print("[!!!] WARNING: dataset INCOMPLETE — do not report these numbers as a full count.")


if __name__ == "__main__":
    main()
