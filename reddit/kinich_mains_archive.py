import argparse, csv, json, os, random, sys, threading, time, urllib.error, urllib.parse, urllib.request
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

SUB = "kinich_mains"
API = "https://arctic-shift.photon-reddit.com/api/comments/search"
UA = "kinich_mains_archive/2.0 (academic research; stdlib urllib)"
START = 1720137600
FIELDS = "id,link_id,parent_id,author,body,created_utc,score,distinguished"
P = "kinich_mains_comments"
RAW, FULL, CK, CSVF, IDX, META = (f"{P}_raw.jsonl", f"{P}_full.jsonl", f"{P}.checkpoint.json",
                                  f"{P}.csv", f"{P}_by_post.json", f"{P}_metadata.json")
KEYS = ["id", "link_id", "parent_id", "author", "body", "created_utc", "score", "distinguished"]
MIN_WINDOW = 3600

lock = threading.Lock()
cool = {"until": 0.0}
stats = Counter()


def ts(t):
    return datetime.fromtimestamp(float(t), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def cooldown(sec):
    with lock:
        cool["until"] = max(cool["until"], time.time() + sec)


def wait_cool():
    while True:
        w = cool["until"] - time.time()
        if w <= 0: return
        time.sleep(min(w, 5) + random.uniform(0, 0.5))


def get(params, full):
    delay, n422 = 2.0, 0
    for i in range(1, 11):
        wait_cool()
        q = dict(params)
        if not full: q["fields"] = FIELDS
        if n422 >= 2: q["limit"] = 50 if n422 < 4 else 25
        stats["requests"] += 1
        try:
            url = f"{API}?{urllib.parse.urlencode(q)}"
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=60) as r:
                d = json.loads(r.read().decode("utf-8"))
            if d.get("error"):
                raise RuntimeError(d["error"])
            return d.get("data") or []
        except urllib.error.HTTPError as e:
            stats[f"http_{e.code}"] += 1
            if e.code not in (422, 429) and e.code < 500:
                return None
            if e.code == 422: n422 += 1
            if e.code in (422, 429): cooldown(delay / 2)
        except Exception as e:
            stats["other_err"] += 1
            if "Timeout" in str(e): n422 += 1
        if i == 6 and n422 < 3: break
        time.sleep(delay + random.uniform(0, delay / 2))
        delay = min(delay * 2, 60)
    return None


class Writer:
    def __init__(self, seen, full):
        self.seen, self.raw = seen, open(RAW, "a", encoding="utf-8")
        self.full = open(FULL, "a", encoding="utf-8") if full else None

    def write(self, batch):
        n = 0
        with lock:
            for c in batch:
                if not c.get("id") or c["id"] in self.seen: continue
                self.seen.add(c["id"]); n += 1
                self.raw.write(json.dumps(slim(c), ensure_ascii=False) + "\n")
                if self.full: self.full.write(json.dumps(c, ensure_ascii=False) + "\n")
            self.raw.flush(); self.full and self.full.flush()
        return n

    def close(self):
        self.raw.close(); self.full and self.full.close()


def slim(c):
    o = {k: c.get(k) for k in KEYS}
    body = c.get("body") or ""
    o["deleted"] = c.get("author") == "[deleted]" and body == "[deleted]"
    o["removed"] = body == "[removed]"
    return o


def run_window(w, ck, wr, full, total):
    a, b = w
    before, got = b, 0
    while True:
        data = get({"subreddit": SUB, "after": a, "before": before, "limit": 100, "sort": "desc"}, full)
        if data is None:
            if b - a > MIN_WINDOW:
                m = (a + before) // 2
                print(f"  [~] window {ts(a)}..{ts(before)} keeps failing -> split")
                with lock:
                    ck["pending"].extend([[a, m], [m, before]])
                    save(ck)
                return "split"
            with lock:
                ck["failed"].append([a, before]); save(ck)
            print(f"  [-] window {ts(a)}..{ts(before)} FAILED permanently (will retry on rerun)")
            return "failed"
        new = wr.write(data)
        got += new
        if not data or (not new and len(data) < 25): break
        oldest = min(int(c["created_utc"]) for c in data)
        if not new:
            nb = oldest
        else:
            nb = oldest + 1 if oldest + 1 < before else oldest
        if nb <= a: break
        before = nb
        time.sleep(0.3)
    with lock:
        ck["done"].append([a, b]); save(ck)
        d = len(ck["done"])
    print(f"  [+] {ts(a)[:10]}..{ts(b)[:10]}: +{got} new | windows {d}/{total[0]} | total {len(wr.seen)}")
    return "done"


def save(ck):
    tmp = CK + ".tmp"
    with open(tmp, "w") as f: json.dump(ck, f)
    os.replace(tmp, CK)


def main():
    ap = argparse.ArgumentParser(description="Archive all r/kinich_mains comments (Arctic Shift), sharded & resumable")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--window-days", type=float, default=7)
    ap.add_argument("--full-objects", action="store_true")
    ap.add_argument("--restart", action="store_true")
    ap.add_argument("--no-csv", action="store_true")
    ap.add_argument("--max-windows", type=int, default=0, help="process at most N windows (testing)")
    ap.add_argument("--newest-first", action="store_true", default=True)
    ap.add_argument("--posts-json", default="kinich_mains_posts_raw.json")
    a = ap.parse_args()
    if a.workers > 4: print("[!] workers capped at 4"); a.workers = 4
    if a.workers > 3: print("[!] >3 workers adds server load and usually causes more 422s")
    if a.restart:
        for f in (RAW, FULL, CK):
            if os.path.exists(f): os.remove(f)

    now = int(time.time()) + 1
    ck = json.load(open(CK)) if os.path.exists(CK) else {}
    if "done" not in ck or not isinstance(ck.get("done"), list):
        v1_before = ck.get("before") if isinstance(ck.get("before"), int) else None
        step = int(a.window_days * 86400)
        wins, t = [], START
        while t < now: wins.append([t, min(t + step, now)]); t += step
        done = []
        if v1_before:
            done = [w for w in wins if w[0] >= v1_before]
            print(f"[*] v1 checkpoint found: {len(done)} windows already covered (>= {ts(v1_before)})")
        ck = {"version": 2, "started": ck.get("started") or datetime.now(timezone.utc).isoformat(),
              "now": now, "done": done, "failed": [], "pending": [w for w in wins if w not in done]}
        save(ck)
    else:
        ck["pending"] = ck.get("pending", []) + ck.get("failed", [])
        ck["failed"] = []
        if now - ck["now"] > 600: ck["pending"].append([ck["now"], now]); ck["now"] = now
        save(ck)
    seen = set()
    if os.path.exists(RAW):
        good = []
        with open(RAW, encoding="utf-8") as f:
            for line in f:
                try:
                    i = json.loads(line)["id"]
                    if i not in seen: seen.add(i); good.append(line)
                except Exception: pass
        with open(RAW, "w", encoding="utf-8") as f: f.writelines(good)
        print(f"[*] Loaded {len(seen)} existing comments (kept, deduped)")

    wr = Writer(seen, a.full_objects)
    t0, n0 = time.time(), len(seen)
    total = [len(ck["done"]) + len(ck["pending"])]
    print(f"[*] {len(ck['pending'])} windows to fetch, {a.workers} workers, fields={'ALL' if a.full_objects else 'slim'}")
    processed = 0
    try:
        with ThreadPoolExecutor(a.workers) as ex:
            while True:
                with lock:
                    batch = sorted(ck["pending"], key=lambda w: -w[0])
                    if a.max_windows: batch = batch[:max(0, a.max_windows - processed)]
                    for w in batch: ck["pending"].remove(w)
                    save(ck)
                if not batch: break
                processed += len(batch)
                total[0] = len(ck["done"]) + len(batch) + len(ck["pending"]) + len(ck["failed"])
                futs = [ex.submit(run_window, w, ck, wr, a.full_objects, total) for w in batch]
                try:
                    for f in futs: f.result()
                except KeyboardInterrupt:
                    raise
    except KeyboardInterrupt:
        print("\n[!] Interrupted — rerun to resume."); os._exit(130)
    finally:
        wr.close()
        with lock:
            dn = {tuple(x) for x in ck["done"]}
    el = time.time() - t0
    complete = not ck["pending"] and not ck["failed"]
    print(f"[{'+' if complete else '!'}] {'COMPLETE' if complete else 'INCOMPLETE'}: {len(seen)} comments "
          f"(+{len(seen) - n0} this run, {el:.0f}s). requests={stats['requests']} 422={stats['http_422']} "
          f"429={stats['http_429']} 5xx={sum(v for k, v in stats.items() if k.startswith('http_5'))}")
    if ck["failed"]:
        print("[!] Failed windows (rerun to retry):")
        for w in ck["failed"]: print(f"    {ts(w[0])} .. {ts(w[1])}")
    finalize(ck, complete, a, el)
    sys.exit(0 if complete else 2)


def finalize(ck, complete, a, el):
    comments = [json.loads(l) for l in open(RAW, encoding="utf-8")]
    by_post = defaultdict(list)
    for c in comments: by_post[(c.get("link_id") or "").replace("t3_", "")].append(c["id"])
    json.dump(by_post, open(IDX, "w"), separators=(",", ":"))
    if not a.no_csv:
        with open(CSVF, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f); cols = KEYS + ["deleted", "removed"]; w.writerow(cols + ["created_iso"])
            for c in comments:
                w.writerow([c.get(k) for k in cols] + [ts(c["created_utc"]) if c.get("created_utc") else ""])
    recon = {}
    if os.path.exists(a.posts_json):
        posts = json.load(open(a.posts_json, encoding="utf-8"))["posts"]
        pid = {p["id"]: int(p.get("num_comments") or 0) for p in posts}
        exp = sum(pid.values()); got = sum(len(by_post.get(i, [])) for i in pid)
        diffs = [len(by_post.get(i, [])) - n for i, n in pid.items()]
        recon = {"posts": len(pid), "sum_post_num_comments": exp, "archived_comments_on_known_posts": got,
                 "coverage_pct": round(got / exp * 100, 2) if exp else None,
                 "posts_exact_match": sum(x == 0 for x in diffs), "posts_fewer_archived": sum(x < 0 for x in diffs),
                 "posts_more_archived": sum(x > 0 for x in diffs),
                 "comments_on_posts_not_in_post_file": sum(len(v) for k, v in by_post.items() if k not in pid)}
    t = [c["created_utc"] for c in comments if c.get("created_utc")]
    meta = {"source": API, "subreddit": SUB, "user_agent": UA, "fetched_at": datetime.now(timezone.utc).isoformat(),
            "started": ck["started"], "complete": complete, "windows_done": len(ck["done"]),
            "windows_pending": ck["pending"], "windows_failed": ck["failed"], "total_comments": len(comments),
            "earliest": ts(min(t)) if t else None, "latest": ts(max(t)) if t else None,
            "deleted": sum(bool(c.get("deleted")) for c in comments), "removed": sum(bool(c.get("removed")) for c in comments),
            "last_run": {"seconds": round(el), **stats}, "reconciliation": recon,
            "notes": "Scores are archive values, not live. num_comments includes removed/unarchived comments."}
    json.dump(meta, open(META, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
