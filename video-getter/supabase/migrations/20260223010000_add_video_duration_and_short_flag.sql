alter table public.youtube_videos
  add column if not exists duration_seconds integer;

alter table public.youtube_videos
  add column if not exists is_short boolean;

update public.youtube_videos
set is_short = true
where is_short is distinct from true
  and coalesce(title, '') ~* '(^|\\W)#?shorts(\\W|$)';

create index if not exists youtube_videos_is_short_idx
  on public.youtube_videos (is_short);

create index if not exists youtube_videos_duration_seconds_idx
  on public.youtube_videos (duration_seconds);
