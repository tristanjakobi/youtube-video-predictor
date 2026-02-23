import fs from "node:fs/promises";
import path from "node:path";
import { google } from "googleapis";

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function parseInteger(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function parseList(value, fallback) {
  if (!value) {
    return fallback;
  }
  const parsed = value
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
  return parsed.length > 0 ? parsed : fallback;
}

function chunkArray(items, chunkSize) {
  const chunks = [];
  for (let index = 0; index < items.length; index += chunkSize) {
    chunks.push(items.slice(index, index + chunkSize));
  }
  return chunks;
}

function csvCell(value) {
  if (value === null || value === undefined) {
    return "";
  }
  const stringValue = String(value);
  if (!/[",\n]/.test(stringValue)) {
    return stringValue;
  }
  return `"${stringValue.replace(/"/g, "\"\"")}"`;
}

function toCsv(rows) {
  if (rows.length === 0) {
    return "";
  }

  const header = Object.keys(rows[0]);
  const lines = [header.join(",")];

  for (const row of rows) {
    lines.push(header.map((key) => csvCell(row[key])).join(","));
  }

  return `${lines.join("\n")}\n`;
}

async function fetchPopularChannelCandidates(youtube, regions, pagesPerRegion) {
  const candidates = new Map();

  for (const regionCode of regions) {
    let pageToken = undefined;
    let page = 0;

    while (page < pagesPerRegion) {
      const response = await youtube.videos.list({
        part: ["snippet"],
        chart: "mostPopular",
        regionCode,
        maxResults: 50,
        pageToken,
      });

      const items = response.data.items || [];
      for (const item of items) {
        const channelId = item.snippet?.channelId;
        if (!channelId) {
          continue;
        }

        const existing = candidates.get(channelId) || {
          channelId,
          trendingHits: 0,
          regions: new Set(),
        };
        existing.trendingHits += 1;
        existing.regions.add(regionCode);
        candidates.set(channelId, existing);
      }

      page += 1;
      pageToken = response.data.nextPageToken;
      if (!pageToken || items.length === 0) {
        break;
      }
    }
  }

  return candidates;
}

function parseCount(value) {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function fetchChannelDetails(youtube, candidates) {
  const channelIds = [...candidates.keys()];
  const detailedRows = [];

  for (const idChunk of chunkArray(channelIds, 50)) {
    const response = await youtube.channels.list({
      part: ["snippet", "statistics"],
      id: idChunk,
      maxResults: 50,
    });

    const items = response.data.items || [];
    for (const item of items) {
      const channelId = item.id;
      const candidate = candidates.get(channelId);
      if (!candidate) {
        continue;
      }

      const subscriberCount = parseCount(item.statistics?.subscriberCount);
      const hiddenSubscriberCount = Boolean(item.statistics?.hiddenSubscriberCount);
      detailedRows.push({
        channel_id: channelId,
        channel_title: item.snippet?.title || null,
        custom_url: item.snippet?.customUrl || null,
        country: item.snippet?.country || null,
        subscriber_count: subscriberCount,
        video_count: parseCount(item.statistics?.videoCount),
        view_count: parseCount(item.statistics?.viewCount),
        hidden_subscriber_count: hiddenSubscriberCount,
        trending_hits: candidate.trendingHits,
        regions_seen: [...candidate.regions].sort().join("|"),
      });
    }
  }

  return detailedRows;
}

function rankRows(rows, maxChannelsOut, minSubscribers) {
  return rows
    .filter((row) => {
      if (row.hidden_subscriber_count) {
        return false;
      }
      return (row.subscriber_count || 0) >= minSubscribers;
    })
    .sort((left, right) => {
      const bySubs = (right.subscriber_count || 0) - (left.subscriber_count || 0);
      if (bySubs !== 0) {
        return bySubs;
      }
      const byHits = (right.trending_hits || 0) - (left.trending_hits || 0);
      if (byHits !== 0) {
        return byHits;
      }
      return (left.channel_title || "").localeCompare(right.channel_title || "");
    })
    .slice(0, maxChannelsOut);
}

async function writeCsv(outputPath, rows) {
  const absolutePath = path.resolve(outputPath);
  await fs.mkdir(path.dirname(absolutePath), { recursive: true });
  await fs.writeFile(absolutePath, toCsv(rows), "utf8");
  return absolutePath;
}

async function main() {
  const config = {
    youtubeApiKey: requireEnv("YOUTUBE_API_KEY"),
    regions: parseList(
      process.env.DISCOVERY_REGIONS,
      ["US", "IN", "BR", "GB", "JP", "DE", "FR", "MX", "KR", "CA", "AU", "ES"],
    ),
    pagesPerRegion: parseInteger(process.env.DISCOVERY_PAGES_PER_REGION, 5),
    maxChannelsOut: parseInteger(process.env.DISCOVERY_MAX_CHANNELS_OUT, 2000),
    minSubscribers: parseInteger(process.env.DISCOVERY_MIN_SUBSCRIBERS, 100000),
    outputCsv: process.env.DISCOVERY_OUTPUT_CSV || "./data/big_channels.csv",
  };

  const youtube = google.youtube({
    version: "v3",
    auth: config.youtubeApiKey,
  });

  console.log(
    `Discovering channels from ${config.regions.length} regions x ${config.pagesPerRegion} pages...`,
  );
  const candidates = await fetchPopularChannelCandidates(
    youtube,
    config.regions,
    config.pagesPerRegion,
  );
  console.log(`Collected ${candidates.size} unique channel candidates.`);

  const detailedRows = await fetchChannelDetails(youtube, candidates);
  const rankedRows = rankRows(
    detailedRows,
    config.maxChannelsOut,
    config.minSubscribers,
  );

  const outputPath = await writeCsv(config.outputCsv, rankedRows);
  console.log(`Wrote ${rankedRows.length} channels to ${outputPath}`);
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  console.error(message);
  process.exit(1);
});
