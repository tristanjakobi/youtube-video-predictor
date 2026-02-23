# YouTube Query-Driven Research Ingestion (Fly.io + Supabase)

This worker runs a query-driven ingestion workflow for your research strategy:

1. Pick the current search query from `youtube_queries`.
2. Use `search.list` to discover videos + creators for that query.
3. Add creators to crawl queue for that query.
4. Crawl creator uploads playlists page-by-page until all discovered creators are fully crawled.
5. Only then move to the next query.
6. Stop when daily API unit budget is exhausted; resume next day.

No manual channel-ID input is required.

## Data model

Schema file: `/Users/developer/youtube-research/sql/schema.sql`

Main tables:

- `public.youtube_queries` - query queue and progress
- `public.youtube_query_channels` - per-query creator crawl state
- `public.youtube_channels` - creator metadata + global crawl metadata
- `public.youtube_videos` - video metadata/stats (title, thumbnail URL, views, etc.)
- `public.youtube_ingest_runs` - run logs + API units consumed

## 1) Supabase setup (migrations)

Migrations are checked in under `/Users/developer/youtube-research/supabase/migrations`:

- `20260221230000_init_schema.sql`
- `20260221230100_seed_youtube_queries.sql`

Apply them in order in Supabase SQL Editor, or run `supabase db push` if your local Supabase CLI project is already initialized and linked.

The seed source file under `supabase/seed_creative_queries.sql` grows as you add theme-focused batches (about 5891 topics after this update). The migration file mirrors that seed at migration creation time.

If you want to append more queries later, add to `supabase/seed_creative_queries.sql` and generate a new migration file rather than editing an applied migration.

## 2) Local run

```bash
cd /Users/developer/youtube-research
cp .env.example .env
npm install
npm run ingest:once
```

Required env vars:

- `YOUTUBE_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Key runtime env vars:

- `MAX_DAILY_QUOTA_UNITS` (default `9500`)
- `INTERVAL_MINUTES` (default `1440`, used only in loop mode)
- `SEARCH_PAGE_SIZE` (default `50`)
- `CRAWL_PAGE_SIZE` (default `50`)
- `SEARCH_REGION_CODE` (default `US`)
- `SEARCH_LANGUAGE` (default `en`)

## 3) Deploy to Fly.io (daily scheduled machine)

```bash
cd /Users/developer/youtube-research
fly launch --no-deploy
fly secrets set YOUTUBE_API_KEY=... SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=...
npm run fly:deploy:scheduled
fly machine list --app youtube-research-ingest
```

`scripts/fly-deploy-scheduled.sh` creates or updates one Fly Machine named `youtube-research-daily` with `--schedule daily` and `RUN_ONCE=true`.
The machine starts on schedule, runs one ingestion pass, exits, and stays stopped between runs.

If you previously deployed the always-on loop worker, stop or destroy that older machine to avoid duplicate runs and cost.

## Query lifecycle

- `pending`: queued, not started
- `in_progress`: active query being discovered/crawled
- `completed`: discovery done and all discovered creators fully crawled
- `paused`: skipped until you unpause

## Useful operations

Pause a query:

```sql
update public.youtube_queries
set status = 'paused', is_active = false
where query_text = 'ai productivity apps';
```

Re-run a completed query from scratch:

```sql
update public.youtube_queries
set
  status = 'pending',
  is_active = true,
  discovery_complete = false,
  next_search_page_token = null,
  completed_at = null
where query_text = 'ai productivity apps';

delete from public.youtube_query_channels
where query_id in (
  select id from public.youtube_queries where query_text = 'ai productivity apps'
);
```

## Notes on quota economics

- `search.list` = 100 units per call
- `channels.list` / `videos.list` / `playlistItems.list` = 1 unit each

This pipeline deliberately spends units to discover creators, then spends low-cost units crawling creator uploads in depth.
