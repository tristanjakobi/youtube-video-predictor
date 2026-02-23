import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";

const SEARCH_LIST_COST = 100;
const SIMPLE_LIST_COST = 1;
const MIN_UNITS_FOR_SEARCH_STEP = 102;
const MIN_UNITS_FOR_CRAWL_STEP = 3;
const DEFAULT_API_MAX_ATTEMPTS = 5;
const DEFAULT_API_RETRY_BASE_MS = 800;

class BudgetLimitReachedError extends Error {
  constructor(message) {
    super(message);
    this.name = "BudgetLimitReachedError";
  }
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function parseBoolean(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function parseInteger(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed)) {
    return fallback;
  }
  return parsed;
}

function chunkArray(items, chunkSize) {
  const chunks = [];
  for (let index = 0; index < items.length; index += chunkSize) {
    chunks.push(items.slice(index, index + chunkSize));
  }
  return chunks;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function pickThumbnail(thumbnails) {
  if (!thumbnails) {
    return null;
  }
  return (
    thumbnails.maxres?.url ||
    thumbnails.standard?.url ||
    thumbnails.high?.url ||
    thumbnails.medium?.url ||
    thumbnails.default?.url ||
    null
  );
}

function parseCount(value) {
  if (value === null || value === undefined) {
    return 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function remainingBudget(budget) {
  return Math.max(0, budget.limit - budget.used);
}

function consumeBudget(budget, units, label) {
  const remaining = remainingBudget(budget);
  if (remaining < units) {
    throw new BudgetLimitReachedError(
      `Daily budget exhausted before ${label}. required=${units} remaining=${remaining}`,
    );
  }
  budget.used += units;
}

function getGoogleErrorMeta(error) {
  const responseStatus = error?.response?.status;
  const codeAsNumber =
    typeof error?.code === "number" ? error.code : Number.parseInt(error?.code || "", 10);
  const status = Number.isFinite(responseStatus)
    ? responseStatus
    : Number.isFinite(codeAsNumber)
      ? codeAsNumber
      : null;

  const message =
    error?.response?.data?.error?.message ||
    error?.message ||
    (error === null || error === undefined ? "Unknown error" : String(error));

  const reasons = (error?.response?.data?.error?.errors || [])
    .map((entry) => entry?.reason)
    .filter(Boolean);

  const codeAsString = typeof error?.code === "string" ? error.code : null;
  const quotaReasons = new Set([
    "quotaExceeded",
    "dailyLimitExceeded",
    "dailyLimitExceededUnreg",
    "rateLimitExceeded",
    "userRateLimitExceeded",
  ]);
  const retryableReasons = new Set([
    "backendError",
    "internalError",
    "rateLimitExceeded",
    "userRateLimitExceeded",
  ]);
  const authReasons = new Set([
    "keyInvalid",
    "accessNotConfigured",
    "forbidden",
    "insufficientPermissions",
  ]);
  const retryableNetworkCodes = new Set([
    "ECONNRESET",
    "ECONNREFUSED",
    "ETIMEDOUT",
    "EAI_AGAIN",
    "ENOTFOUND",
    "EPIPE",
    "ERR_NETWORK",
  ]);

  const quotaExceeded =
    reasons.some((reason) => quotaReasons.has(reason)) ||
    (status === 403 && /quota/i.test(message));
  const authError = status === 401 || reasons.some((reason) => authReasons.has(reason));
  const retryableStatus = [429, 500, 502, 503, 504].includes(status);
  const retryableReason = reasons.some((reason) => retryableReasons.has(reason));
  const retryableNetwork = codeAsString ? retryableNetworkCodes.has(codeAsString) : false;
  const retryable = !authError && !quotaExceeded && (retryableStatus || retryableReason || retryableNetwork);

  return {
    status,
    message,
    reasons,
    retryable,
    quotaExceeded,
  };
}

function formatErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

async function callYouTubeApi({ budget, units, label, request }) {
  for (let attempt = 1; attempt <= DEFAULT_API_MAX_ATTEMPTS; attempt += 1) {
    consumeBudget(budget, units, label);

    try {
      return await request();
    } catch (error) {
      const meta = getGoogleErrorMeta(error);
      if (meta.quotaExceeded) {
        throw new BudgetLimitReachedError(`YouTube quota exceeded during ${label}: ${meta.message}`);
      }

      if (meta.retryable && attempt < DEFAULT_API_MAX_ATTEMPTS) {
        const jitterMs = Math.floor(Math.random() * 250);
        const waitMs = DEFAULT_API_RETRY_BASE_MS * 2 ** (attempt - 1) + jitterMs;
        const statusText = meta.status ?? "unknown";
        const reasonText = meta.reasons.length > 0 ? meta.reasons.join("|") : "n/a";
        console.warn(
          `${label} failed (attempt ${attempt}/${DEFAULT_API_MAX_ATTEMPTS}). ` +
            `status=${statusText} reasons=${reasonText}. retrying in ${waitMs}ms`,
        );
        await sleep(waitMs);
        continue;
      }

      const statusText = meta.status ?? "unknown";
      const reasonText = meta.reasons.length > 0 ? meta.reasons.join("|") : "n/a";
      throw new Error(
        `${label} failed after ${attempt} attempt(s). ` +
          `status=${statusText} reasons=${reasonText} message=${meta.message}`,
      );
    }
  }

  throw new Error(`Unexpected retry loop exit for ${label}`);
}

async function upsertVideos(supabase, rows) {
  if (rows.length === 0) {
    return;
  }

  for (const rowChunk of chunkArray(rows, 500)) {
    const { error } = await supabase
      .from("youtube_videos")
      .upsert(rowChunk, { onConflict: "video_id" });

    if (error) {
      throw new Error(`Supabase video upsert failed: ${error.message}`);
    }
  }
}

async function upsertChannels(supabase, rows) {
  if (rows.length === 0) {
    return;
  }

  for (const rowChunk of chunkArray(rows, 500)) {
    const { error } = await supabase
      .from("youtube_channels")
      .upsert(rowChunk, { onConflict: "channel_id" });

    if (error) {
      throw new Error(`Supabase channel upsert failed: ${error.message}`);
    }
  }
}

async function upsertQueryChannels(supabase, rows) {
  if (rows.length === 0) {
    return;
  }

  for (const rowChunk of chunkArray(rows, 500)) {
    const { error } = await supabase
      .from("youtube_query_channels")
      .upsert(rowChunk, { onConflict: "query_id,channel_id" });

    if (error) {
      throw new Error(`Supabase query-channel upsert failed: ${error.message}`);
    }
  }
}

async function recordRun(supabase, run) {
  const { error } = await supabase.from("youtube_ingest_runs").insert(run);
  if (error) {
    console.error("Failed to record run:", error.message);
  }
}

async function deferFailedQuery(supabase, queryId) {
  const nowIso = new Date().toISOString();
  const { error } = await supabase
    .from("youtube_queries")
    .update({
      status: "pending",
      last_run_at: nowIso,
    })
    .eq("id", queryId)
    .eq("status", "in_progress");

  if (error) {
    throw new Error(`Failed deferring query ${queryId}: ${error.message}`);
  }
}

async function fetchVideoDetails(youtube, videoIds, budget) {
  const detailMap = new Map();

  for (const idChunk of chunkArray(videoIds, 50)) {
    const response = await callYouTubeApi({
      budget,
      units: SIMPLE_LIST_COST,
      label: "videos.list",
      request: () =>
        youtube.videos.list({
          part: ["snippet", "statistics"],
          id: idChunk,
          maxResults: 50,
        }),
    });

    for (const item of response.data.items || []) {
      detailMap.set(item.id, {
        videoId: item.id,
        title: item.snippet?.title || null,
        publishedAt: item.snippet?.publishedAt || null,
        channelId: item.snippet?.channelId || null,
        channelTitle: item.snippet?.channelTitle || null,
        thumbnailUrl: pickThumbnail(item.snippet?.thumbnails),
        viewCount: parseCount(item.statistics?.viewCount),
        likeCount: parseCount(item.statistics?.likeCount),
        commentCount: parseCount(item.statistics?.commentCount),
      });
    }
  }

  return detailMap;
}

async function fetchChannelDetails(youtube, channelIds, budget) {
  const detailMap = new Map();

  for (const idChunk of chunkArray(channelIds, 50)) {
    const response = await callYouTubeApi({
      budget,
      units: SIMPLE_LIST_COST,
      label: "channels.list",
      request: () =>
        youtube.channels.list({
          part: ["contentDetails", "snippet", "statistics"],
          id: idChunk,
          maxResults: 50,
        }),
    });

    for (const item of response.data.items || []) {
      detailMap.set(item.id, {
        channelId: item.id,
        channelTitle: item.snippet?.title || null,
        customUrl: item.snippet?.customUrl || null,
        country: item.snippet?.country || null,
        uploadsPlaylistId: item.contentDetails?.relatedPlaylists?.uploads || null,
        subscriberCount: parseCount(item.statistics?.subscriberCount),
        videoCount: parseCount(item.statistics?.videoCount),
        viewCount: parseCount(item.statistics?.viewCount),
      });
    }
  }

  return detailMap;
}

function buildVideoRows(videoIds, detailMap, fallbackMap, fetchedAt) {
  const rows = [];

  for (const videoId of videoIds) {
    const details = detailMap.get(videoId);
    const fallback = fallbackMap.get(videoId) || {};

    rows.push({
      video_id: videoId,
      channel_id: details?.channelId || fallback.channelId || null,
      channel_title: details?.channelTitle || fallback.channelTitle || null,
      title: details?.title || fallback.title || null,
      published_at: details?.publishedAt || fallback.publishedAt || null,
      thumbnail_url: details?.thumbnailUrl || fallback.thumbnailUrl || null,
      view_count: details?.viewCount || 0,
      like_count: details?.likeCount || 0,
      comment_count: details?.commentCount || 0,
      fetched_at: fetchedAt,
    });
  }

  return rows.filter((row) => row.video_id && row.channel_id);
}

async function getOrStartQuery(supabase, excludedQueryIds = []) {
  let inProgressQuery = supabase
    .from("youtube_queries")
    .select("id,query_text,status,is_active,priority,discovery_complete,next_search_page_token,started_at")
    .eq("is_active", true)
    .eq("status", "in_progress")
    .order("priority", { ascending: true })
    .order("id", { ascending: true })
    .limit(1);

  if (excludedQueryIds.length > 0) {
    inProgressQuery = inProgressQuery.not("id", "in", `(${excludedQueryIds.join(",")})`);
  }

  const inProgress = await inProgressQuery.maybeSingle();

  if (inProgress.error) {
    throw new Error(`Failed to load in-progress query: ${inProgress.error.message}`);
  }

  if (inProgress.data) {
    return inProgress.data;
  }

  let pendingQuery = supabase
    .from("youtube_queries")
    .select("id,query_text,status,is_active,priority,discovery_complete,next_search_page_token,started_at")
    .eq("is_active", true)
    .eq("status", "pending")
    .order("priority", { ascending: true })
    .order("id", { ascending: true })
    .limit(1);

  if (excludedQueryIds.length > 0) {
    pendingQuery = pendingQuery.not("id", "in", `(${excludedQueryIds.join(",")})`);
  }

  const pending = await pendingQuery.maybeSingle();

  if (pending.error) {
    throw new Error(`Failed to load pending query: ${pending.error.message}`);
  }

  if (!pending.data) {
    return null;
  }

  const nowIso = new Date().toISOString();
  const { data, error } = await supabase
    .from("youtube_queries")
    .update({
      status: "in_progress",
      started_at: pending.data.started_at || nowIso,
      last_run_at: nowIso,
    })
    .eq("id", pending.data.id)
    .select("id,query_text,status,is_active,priority,discovery_complete,next_search_page_token,started_at")
    .single();

  if (error) {
    throw new Error(`Failed to start query ${pending.data.id}: ${error.message}`);
  }

  return data;
}

async function getChannelStateMap(supabase, channelIds) {
  if (channelIds.length === 0) {
    return new Map();
  }

  const { data, error } = await supabase
    .from("youtube_channels")
    .select("channel_id,uploads_playlist_id,crawl_next_page_token,crawl_complete")
    .in("channel_id", channelIds);

  if (error) {
    throw new Error(`Failed to read channel crawl state: ${error.message}`);
  }

  return new Map((data || []).map((row) => [row.channel_id, row]));
}

async function discoverOneSearchPage(config, youtube, supabase, query, budget) {
  if (remainingBudget(budget) < MIN_UNITS_FOR_SEARCH_STEP) {
    return {
      madeProgress: false,
      query,
      fetched: 0,
      written: 0,
    };
  }

  const response = await callYouTubeApi({
    budget,
    units: SEARCH_LIST_COST,
    label: "search.list",
    request: () =>
      youtube.search.list({
        part: ["snippet"],
        q: query.query_text,
        type: ["video"],
        maxResults: Math.min(50, config.searchPageSize),
        pageToken: query.next_search_page_token || undefined,
        regionCode: config.searchRegionCode,
        relevanceLanguage: config.searchLanguage,
      }),
  });

  const items = response.data.items || [];
  const videoIds = [];
  const channelIds = [];
  const seenVideoIds = new Set();
  const seenChannelIds = new Set();
  const fallbackByVideoId = new Map();
  const firstVideoByChannel = new Map();

  for (const item of items) {
    const videoId = item.id?.videoId;
    const channelId = item.snippet?.channelId;

    if (videoId && !seenVideoIds.has(videoId)) {
      seenVideoIds.add(videoId);
      videoIds.push(videoId);
      fallbackByVideoId.set(videoId, {
        videoId,
        channelId: channelId || null,
        channelTitle: item.snippet?.channelTitle || null,
        title: item.snippet?.title || null,
        publishedAt: item.snippet?.publishedAt || null,
        thumbnailUrl: pickThumbnail(item.snippet?.thumbnails),
      });
    }

    if (channelId && !seenChannelIds.has(channelId)) {
      seenChannelIds.add(channelId);
      channelIds.push(channelId);
    }

    if (channelId && videoId && !firstVideoByChannel.has(channelId)) {
      firstVideoByChannel.set(channelId, videoId);
    }
  }

  const fetchedAt = new Date().toISOString();
  const videoDetails = await fetchVideoDetails(youtube, videoIds, budget);
  const videoRows = buildVideoRows(videoIds, videoDetails, fallbackByVideoId, fetchedAt);
  await upsertVideos(supabase, videoRows);

  const channelDetails = await fetchChannelDetails(youtube, channelIds, budget);
  const channelRows = channelIds.map((channelId) => {
    const details = channelDetails.get(channelId);
    return {
      channel_id: channelId,
      channel_title: details?.channelTitle || null,
      source: "query_search",
      custom_url: details?.customUrl || null,
      country: details?.country || null,
      subscriber_count: details?.subscriberCount || 0,
      video_count: details?.videoCount || 0,
      total_view_count: details?.viewCount || 0,
      uploads_playlist_id: details?.uploadsPlaylistId || null,
      last_discovered_at: fetchedAt,
    };
  });
  await upsertChannels(supabase, channelRows);

  const channelStateMap = await getChannelStateMap(supabase, channelIds);
  const queryChannelRows = channelIds.map((channelId) => {
    const channelState = channelStateMap.get(channelId);
    const apiChannel = channelDetails.get(channelId);

    return {
      query_id: query.id,
      channel_id: channelId,
      first_video_id: firstVideoByChannel.get(channelId) || null,
      uploads_playlist_id:
        channelState?.uploads_playlist_id || apiChannel?.uploadsPlaylistId || null,
      next_page_token: channelState?.crawl_next_page_token || null,
      crawl_complete: Boolean(channelState?.crawl_complete),
    };
  });
  await upsertQueryChannels(supabase, queryChannelRows);

  const nextSearchPageToken = response.data.nextPageToken || null;
  const discoveryComplete = !nextSearchPageToken || items.length === 0;

  const { data: updatedQuery, error: queryUpdateError } = await supabase
    .from("youtube_queries")
    .update({
      discovery_complete: discoveryComplete,
      next_search_page_token: nextSearchPageToken,
      last_run_at: fetchedAt,
    })
    .eq("id", query.id)
    .select("id,query_text,status,is_active,priority,discovery_complete,next_search_page_token,started_at")
    .single();

  if (queryUpdateError) {
    throw new Error(`Failed to update query ${query.id}: ${queryUpdateError.message}`);
  }

  return {
    madeProgress: true,
    query: updatedQuery,
    fetched: videoRows.length,
    written: videoRows.length,
  };
}

async function resolveUploadsPlaylistId(youtube, supabase, channelId, budget) {
  const existing = await supabase
    .from("youtube_channels")
    .select("uploads_playlist_id")
    .eq("channel_id", channelId)
    .maybeSingle();

  if (existing.error) {
    throw new Error(`Failed to read channel ${channelId}: ${existing.error.message}`);
  }

  if (existing.data?.uploads_playlist_id) {
    return existing.data.uploads_playlist_id;
  }

  const response = await callYouTubeApi({
    budget,
    units: SIMPLE_LIST_COST,
    label: "channels.list (resolve uploads playlist)",
    request: () =>
      youtube.channels.list({
        part: ["contentDetails", "snippet", "statistics"],
        id: [channelId],
        maxResults: 1,
      }),
  });

  const channel = response.data.items?.[0];
  if (!channel) {
    return null;
  }

  const uploadsPlaylistId = channel.contentDetails?.relatedPlaylists?.uploads || null;
  const nowIso = new Date().toISOString();

  await upsertChannels(supabase, [
    {
      channel_id: channelId,
      channel_title: channel.snippet?.title || null,
      source: "query_search",
      custom_url: channel.snippet?.customUrl || null,
      country: channel.snippet?.country || null,
      subscriber_count: parseCount(channel.statistics?.subscriberCount),
      video_count: parseCount(channel.statistics?.videoCount),
      total_view_count: parseCount(channel.statistics?.viewCount),
      uploads_playlist_id: uploadsPlaylistId,
      last_discovered_at: nowIso,
    },
  ]);

  return uploadsPlaylistId;
}

async function crawlOneChannelPage(config, youtube, supabase, queryId, budget) {
  if (remainingBudget(budget) < MIN_UNITS_FOR_CRAWL_STEP) {
    return { madeProgress: false, fetched: 0, written: 0 };
  }

  const pending = await supabase
    .from("youtube_query_channels")
    .select("channel_id,uploads_playlist_id,next_page_token,crawl_started_at")
    .eq("query_id", queryId)
    .eq("crawl_complete", false)
    .order("last_crawled_at", { ascending: true, nullsFirst: true })
    .order("discovered_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (pending.error) {
    throw new Error(`Failed to load crawl queue for query ${queryId}: ${pending.error.message}`);
  }

  if (!pending.data) {
    return { madeProgress: false, fetched: 0, written: 0 };
  }

  const channelId = pending.data.channel_id;
  let uploadsPlaylistId = pending.data.uploads_playlist_id || null;

  if (!uploadsPlaylistId) {
    uploadsPlaylistId = await resolveUploadsPlaylistId(youtube, supabase, channelId, budget);

    if (!uploadsPlaylistId) {
      const nowIso = new Date().toISOString();
      const { error: markError } = await supabase
        .from("youtube_query_channels")
        .update({
          crawl_complete: true,
          crawl_started_at: pending.data.crawl_started_at || nowIso,
          crawl_finished_at: nowIso,
          last_crawled_at: nowIso,
        })
        .eq("query_id", queryId)
        .eq("channel_id", channelId);

      if (markError) {
        throw new Error(`Failed marking channel ${channelId} complete: ${markError.message}`);
      }

      return { madeProgress: true, fetched: 0, written: 0 };
    }

    const { error: setPlaylistError } = await supabase
      .from("youtube_query_channels")
      .update({ uploads_playlist_id: uploadsPlaylistId })
      .eq("query_id", queryId)
      .eq("channel_id", channelId);

    if (setPlaylistError) {
      throw new Error(
        `Failed to set uploads playlist for query ${queryId}, channel ${channelId}: ${setPlaylistError.message}`,
      );
    }
  }

  const pageResponse = await callYouTubeApi({
    budget,
    units: SIMPLE_LIST_COST,
    label: "playlistItems.list",
    request: () =>
      youtube.playlistItems.list({
        part: ["snippet", "contentDetails"],
        playlistId: uploadsPlaylistId,
        pageToken: pending.data.next_page_token || undefined,
        maxResults: Math.min(50, config.crawlPageSize),
      }),
  });

  const items = pageResponse.data.items || [];
  const videoIds = [];
  const fallbackByVideoId = new Map();
  const seenVideoIds = new Set();

  for (const item of items) {
    const videoId = item.contentDetails?.videoId;
    if (!videoId || seenVideoIds.has(videoId)) {
      continue;
    }

    seenVideoIds.add(videoId);
    videoIds.push(videoId);
    fallbackByVideoId.set(videoId, {
      videoId,
      channelId,
      channelTitle: item.snippet?.channelTitle || null,
      title: item.snippet?.title || null,
      publishedAt: item.contentDetails?.videoPublishedAt || item.snippet?.publishedAt || null,
      thumbnailUrl: pickThumbnail(item.snippet?.thumbnails),
    });
  }

  const fetchedAt = new Date().toISOString();
  const videoDetails = await fetchVideoDetails(youtube, videoIds, budget);
  const videoRows = buildVideoRows(videoIds, videoDetails, fallbackByVideoId, fetchedAt);
  await upsertVideos(supabase, videoRows);

  const nextPageToken = pageResponse.data.nextPageToken || null;
  const crawlComplete = !nextPageToken || items.length === 0;

  const queryChannelUpdates = {
    uploads_playlist_id: uploadsPlaylistId,
    next_page_token: nextPageToken,
    crawl_complete: crawlComplete,
    crawl_started_at: pending.data.crawl_started_at || fetchedAt,
    last_crawled_at: fetchedAt,
  };
  if (crawlComplete) {
    queryChannelUpdates.crawl_finished_at = fetchedAt;
  }

  const { error: queryChannelUpdateError } = await supabase
    .from("youtube_query_channels")
    .update(queryChannelUpdates)
    .eq("query_id", queryId)
    .eq("channel_id", channelId);

  if (queryChannelUpdateError) {
    throw new Error(
      `Failed updating crawl state for query ${queryId}, channel ${channelId}: ${queryChannelUpdateError.message}`,
    );
  }

  const globalChannelUpdate = {
    channel_id: channelId,
    uploads_playlist_id: uploadsPlaylistId,
    crawl_next_page_token: nextPageToken,
    crawl_complete: crawlComplete,
    last_crawled_at: fetchedAt,
  };
  if (crawlComplete) {
    globalChannelUpdate.crawl_finished_at = fetchedAt;
  }

  await upsertChannels(supabase, [globalChannelUpdate]);

  return {
    madeProgress: true,
    fetched: videoRows.length,
    written: videoRows.length,
  };
}

async function completeQueryIfDone(supabase, query) {
  if (!query.discovery_complete) {
    return false;
  }

  const pending = await supabase
    .from("youtube_query_channels")
    .select("channel_id", { count: "exact", head: true })
    .eq("query_id", query.id)
    .eq("crawl_complete", false);

  if (pending.error) {
    throw new Error(`Failed checking query completion for ${query.id}: ${pending.error.message}`);
  }

  const pendingCount = pending.count || 0;
  if (pendingCount > 0) {
    return false;
  }

  const nowIso = new Date().toISOString();
  const { error } = await supabase
    .from("youtube_queries")
    .update({
      status: "completed",
      completed_at: nowIso,
      last_run_at: nowIso,
    })
    .eq("id", query.id);

  if (error) {
    throw new Error(`Failed marking query ${query.id} completed: ${error.message}`);
  }

  return true;
}

async function runOnce(config, youtube, supabase) {
  const startedAt = new Date();
  const budget = { limit: config.maxDailyQuotaUnits, used: 0 };
  const excludedQueryIds = new Set();

  let status = "success";
  let errorMessage = null;
  let totalFetched = 0;
  let totalWritten = 0;
  let activeQueryId = null;

  try {
    while (remainingBudget(budget) > 0) {
      let query = null;
      try {
        query = await getOrStartQuery(supabase, [...excludedQueryIds]);
        if (!query) {
          console.log("No active or pending queries. Worker is idle.");
          break;
        }

        activeQueryId = query.id;
        let progressed = false;

        if (!query.discovery_complete) {
          const discovery = await discoverOneSearchPage(
            config,
            youtube,
            supabase,
            query,
            budget,
          );
          if (discovery.madeProgress) {
            query = discovery.query;
            totalFetched += discovery.fetched;
            totalWritten += discovery.written;
            progressed = true;
            console.log(
              `Query ${query.id} search page processed. used_units=${budget.used} remaining=${remainingBudget(
                budget,
              )}`,
            );
          }
        }

        const crawl = await crawlOneChannelPage(config, youtube, supabase, query.id, budget);
        if (crawl.madeProgress) {
          totalFetched += crawl.fetched;
          totalWritten += crawl.written;
          progressed = true;
          console.log(
            `Query ${query.id} channel page crawled. used_units=${budget.used} remaining=${remainingBudget(
              budget,
            )}`,
          );
        }

        const completed = await completeQueryIfDone(supabase, query);
        if (completed) {
          console.log(`Query ${query.id} complete. Moving to next query.`);
          activeQueryId = null;
          continue;
        }

        if (!progressed) {
          console.log("No further progress possible in this run (likely budget constraints).");
          break;
        }

        const { error: touchError } = await supabase
          .from("youtube_queries")
          .update({ last_run_at: new Date().toISOString() })
          .eq("id", query.id);

        if (touchError) {
          throw new Error(`Failed to touch query ${query.id}: ${touchError.message}`);
        }
      } catch (error) {
        if (error instanceof BudgetLimitReachedError) {
          throw error;
        }

        if (!query?.id) {
          throw error;
        }

        const queryErrorMessage = formatErrorMessage(error);
        console.error(
          `Query ${query.id} failed and will be retried later. error=${queryErrorMessage}`,
        );

        await deferFailedQuery(supabase, query.id);
        excludedQueryIds.add(query.id);
        activeQueryId = null;

        if (status === "success") {
          status = "partial_error";
        }
        if (!errorMessage) {
          errorMessage = queryErrorMessage;
        }
      }
    }
  } catch (error) {
    if (error instanceof BudgetLimitReachedError) {
      status = status === "partial_error" ? "partial_error_quota_exhausted" : "quota_exhausted";
      errorMessage = error.message;
    } else {
      status = "error";
      errorMessage = formatErrorMessage(error);
      throw error;
    }
  } finally {
    await recordRun(supabase, {
      started_at: startedAt.toISOString(),
      finished_at: new Date().toISOString(),
      status,
      total_fetched: totalFetched,
      total_written: totalWritten,
      total_api_units: budget.used,
      active_query_id: activeQueryId,
      error_message: errorMessage,
    });
  }
}

async function main() {
  const config = {
    youtubeApiKey: requireEnv("YOUTUBE_API_KEY"),
    supabaseUrl: requireEnv("SUPABASE_URL"),
    supabaseServiceRoleKey: requireEnv("SUPABASE_SERVICE_ROLE_KEY"),
    searchPageSize: parseInteger(process.env.SEARCH_PAGE_SIZE, 50),
    crawlPageSize: parseInteger(process.env.CRAWL_PAGE_SIZE, 50),
    maxDailyQuotaUnits: parseInteger(process.env.MAX_DAILY_QUOTA_UNITS, 9500),
    searchRegionCode: process.env.SEARCH_REGION_CODE || "US",
    searchLanguage: process.env.SEARCH_LANGUAGE || "en",
    runOnce: parseBoolean(process.env.RUN_ONCE, true),
    intervalMinutes: parseInteger(process.env.INTERVAL_MINUTES, 1440),
  };

  const youtube = google.youtube({
    version: "v3",
    auth: config.youtubeApiKey,
  });

  const supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });

  if (config.runOnce) {
    await runOnce(config, youtube, supabase);
    console.log("Run complete.");
    return;
  }

  console.log(`Starting loop mode. Interval: ${config.intervalMinutes} minutes`);
  while (true) {
    const loopStarted = Date.now();
    try {
      await runOnce(config, youtube, supabase);
      console.log("Loop run complete.");
    } catch (error) {
      const message = error instanceof Error ? error.stack || error.message : String(error);
      console.error("Loop run failed:", message);
    }

    const elapsedMs = Date.now() - loopStarted;
    const intervalMs = Math.max(1, config.intervalMinutes) * 60 * 1000;
    const waitMs = Math.max(5_000, intervalMs - elapsedMs);
    console.log(`Sleeping ${Math.round(waitMs / 1000)}s before next run...`);
    await sleep(waitMs);
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  console.error(message);
  process.exit(1);
});
