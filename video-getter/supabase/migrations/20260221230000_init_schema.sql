create table if not exists public.youtube_videos (
  video_id text primary key,
  channel_id text not null,
  channel_title text,
  title text,
  published_at timestamptz,
  thumbnail_url text,
  view_count bigint not null default 0,
  like_count bigint not null default 0,
  comment_count bigint not null default 0,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists youtube_videos_channel_id_idx
  on public.youtube_videos (channel_id);

create index if not exists youtube_videos_published_at_idx
  on public.youtube_videos (published_at desc);

create index if not exists youtube_videos_view_count_idx
  on public.youtube_videos (view_count desc);

create table if not exists public.youtube_ingest_runs (
  id bigint generated always as identity primary key,
  started_at timestamptz not null,
  finished_at timestamptz not null,
  status text not null,
  total_fetched integer not null default 0,
  total_written integer not null default 0,
  error_message text,
  created_at timestamptz not null default now()
);

create index if not exists youtube_ingest_runs_created_at_idx
  on public.youtube_ingest_runs (created_at desc);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists set_youtube_videos_updated_at on public.youtube_videos;

create trigger set_youtube_videos_updated_at
before update on public.youtube_videos
for each row
execute function public.set_updated_at();

create table if not exists public.youtube_channels (
  channel_id text primary key,
  channel_title text,
  niche text,
  notes text,
  source text not null default 'manual',
  custom_url text,
  country text,
  subscriber_count bigint,
  video_count bigint,
  total_view_count bigint,
  trending_hits integer not null default 0,
  regions_seen text,
  last_discovered_at timestamptz,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.youtube_channels add column if not exists source text not null default 'manual';
alter table public.youtube_channels add column if not exists custom_url text;
alter table public.youtube_channels add column if not exists country text;
alter table public.youtube_channels add column if not exists subscriber_count bigint;
alter table public.youtube_channels add column if not exists video_count bigint;
alter table public.youtube_channels add column if not exists total_view_count bigint;
alter table public.youtube_channels add column if not exists trending_hits integer not null default 0;
alter table public.youtube_channels add column if not exists regions_seen text;
alter table public.youtube_channels add column if not exists last_discovered_at timestamptz;
alter table public.youtube_channels add column if not exists uploads_playlist_id text;
alter table public.youtube_channels add column if not exists crawl_next_page_token text;
alter table public.youtube_channels add column if not exists crawl_complete boolean not null default false;
alter table public.youtube_channels add column if not exists crawl_started_at timestamptz;
alter table public.youtube_channels add column if not exists crawl_finished_at timestamptz;
alter table public.youtube_channels add column if not exists last_crawled_at timestamptz;

create index if not exists youtube_channels_is_active_idx
  on public.youtube_channels (is_active);

create index if not exists youtube_channels_subscriber_count_idx
  on public.youtube_channels (subscriber_count desc);

create index if not exists youtube_channels_last_discovered_at_idx
  on public.youtube_channels (last_discovered_at desc);

create index if not exists youtube_channels_crawl_complete_idx
  on public.youtube_channels (crawl_complete);

drop trigger if exists set_youtube_channels_updated_at on public.youtube_channels;

create trigger set_youtube_channels_updated_at
before update on public.youtube_channels
for each row
execute function public.set_updated_at();

create table if not exists public.youtube_queries (
  id bigint generated always as identity primary key,
  query_text text not null unique,
  status text not null default 'pending',
  is_active boolean not null default true,
  priority integer not null default 100,
  discovery_complete boolean not null default false,
  next_search_page_token text,
  started_at timestamptz,
  completed_at timestamptz,
  last_run_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint youtube_queries_status_check
    check (status in ('pending', 'in_progress', 'completed', 'paused'))
);

create index if not exists youtube_queries_active_priority_idx
  on public.youtube_queries (is_active, status, priority, id);

drop trigger if exists set_youtube_queries_updated_at on public.youtube_queries;

create trigger set_youtube_queries_updated_at
before update on public.youtube_queries
for each row
execute function public.set_updated_at();

create table if not exists public.youtube_query_channels (
  query_id bigint not null references public.youtube_queries(id) on delete cascade,
  channel_id text not null references public.youtube_channels(channel_id) on delete cascade,
  discovered_at timestamptz not null default now(),
  first_video_id text,
  uploads_playlist_id text,
  next_page_token text,
  crawl_complete boolean not null default false,
  crawl_started_at timestamptz,
  crawl_finished_at timestamptz,
  last_crawled_at timestamptz,
  primary key (query_id, channel_id)
);

alter table public.youtube_query_channels add column if not exists uploads_playlist_id text;
alter table public.youtube_query_channels add column if not exists next_page_token text;
alter table public.youtube_query_channels add column if not exists crawl_complete boolean not null default false;
alter table public.youtube_query_channels add column if not exists crawl_started_at timestamptz;
alter table public.youtube_query_channels add column if not exists crawl_finished_at timestamptz;
alter table public.youtube_query_channels add column if not exists last_crawled_at timestamptz;

create index if not exists youtube_query_channels_query_id_idx
  on public.youtube_query_channels (query_id);

create index if not exists youtube_query_channels_channel_id_idx
  on public.youtube_query_channels (channel_id);

create index if not exists youtube_query_channels_crawl_complete_idx
  on public.youtube_query_channels (query_id, crawl_complete, last_crawled_at);

alter table public.youtube_ingest_runs add column if not exists total_api_units integer not null default 0;
alter table public.youtube_ingest_runs add column if not exists active_query_id bigint references public.youtube_queries(id);
