# BMS353-Research

## Reddit: r/kinich_mains

Scripts in `reddit/` use the Python 3.8+ standard library only. Run them from that directory so output files land next to the scripts. Generated `kinich_mains_*` CSV, JSON, and JSONL files are gitignored.

### Post and flair statistics

`kinich_mains_scraper.py` collects post IDs from Arctic Shift (default) or Pullpush, optionally merges the Reddit `/new` listing, and fills in current score, comment count, flair, and removal state with the Reddit API. Coverage is best-effort. An interrupted fetch is marked INCOMPLETE in the metadata.

Create a free script app at <https://www.reddit.com/prefs/apps> (redirect URI `http://localhost:8080`). The string under the app name is the client id; `secret` is the client secret. Reddit may ask you to accept the Data API terms at <https://support.reddithelp.com> first.

```
export REDDIT_CLIENT_ID=xxx
export REDDIT_CLIENT_SECRET=yyy
export REDDIT_USER_AGENT="script:kinich_mains_census:v2.0 (by /u/your_username)"
```

```
python kinich_mains_scraper.py
python kinich_mains_scraper.py --exclude-removed
python kinich_mains_scraper.py --no-listing
python kinich_mains_scraper.py --skip-hydrate
python kinich_mains_scraper.py --source pullpush
```

`--exclude-removed` drops removed or deleted posts from the main table. `--no-listing` skips the Reddit `/new` ID merge. `--skip-hydrate` (also `--dry-run`) skips the Reddit API and uses archive scores, which are not live. Without `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET`, the script takes that path on its own.

Outputs: `kinich_mains_flair_stats.csv`, `kinich_mains_posts_raw.json`, `kinich_mains_metadata.json`.

### Comment archive

`kinich_mains_archive.py` archives every r/kinich_mains comment from Arctic Shift. It shards history into time windows, requests only the fields it stores, and shrinks the window or page size when Arctic Shift returns HTTP 422 (a server query timeout under load). The run is resumable: interrupt it and run the same command again. Run the post scraper first if you want comment counts reconciled against `kinich_mains_posts_raw.json`.

```
python kinich_mains_archive.py
python kinich_mains_archive.py --workers 3
python kinich_mains_archive.py --window-days 3
python kinich_mains_archive.py --full-objects
python kinich_mains_archive.py --restart
```

`--workers` defaults to 2 and is capped at 4. `--window-days` defaults to 7. `--full-objects` also writes the full comment objects. `--restart` deletes the checkpoint and raw files and starts over. `--no-csv` skips the CSV. `--max-windows N` stops after N windows. `--posts-json` points at the scraper JSON used for reconciliation.

Outputs: `kinich_mains_comments_raw.jsonl`, `kinich_mains_comments_full.jsonl` (with `--full-objects`), `kinich_mains_comments.csv`, `kinich_mains_comments_by_post.json`, `kinich_mains_comments_metadata.json`, and `kinich_mains_comments.checkpoint.json`.
