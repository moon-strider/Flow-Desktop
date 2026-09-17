import type { FeedQuotas } from "./api/recommendation";
import type { VideoSummary } from "../types/video";
import type { WatchHistoryRecord } from "../types/db";

const CACHE_TTL_MS = 30 * 60 * 1000;
const PENDING_TTL_MS = 2 * 60 * 1000;
const MAX_ENTRIES = 8;

export interface HomeFeedSources {
  queryCandidates: string[];
  feedQuotas: FeedQuotas;
  starterRotationPool: Promise<VideoSummary[]>;
  starterDiscoveryPool: Promise<VideoSummary[]>;
  subscriptionPool: Promise<VideoSummary[]>;
  discoveryPool: Promise<VideoSummary[]>;
  relatedPool: Promise<VideoSummary[]>;
  personalizedMusicPool: Promise<VideoSummary[]>;
  freshSubs: Promise<VideoSummary[]>;
  getSubscriptionRotationPool: () => Promise<VideoSummary[]>;
}

export interface HomeFeedLoad {
  key: string;
  startedAt: number;
  sources: Promise<HomeFeedSources>;
  videos: VideoSummary[] | null;
}

const loads = new Map<string, HomeFeedLoad>();
const listeners = new Set<() => void>();
let revision = 0;

export function getHomeRelatedSeedIds(history: WatchHistoryRecord[], limit = 4) {
  const eligible = history.filter((record) => {
    if (record.isMusic) return false;
    const total = record.totalDurationSeconds ?? 0;
    return record.watchDurationSeconds >= 180 || total > 0 && record.watchDurationSeconds / total >= 0.35;
  });
  const ids: string[] = [];
  const channels = new Set<string>();
  const seen = new Set<string>();
  for (const record of eligible) {
    if (ids.length >= limit) break;
    const channel = record.channelName || record.videoId;
    if (channels.has(channel) || seen.has(record.videoId)) continue;
    channels.add(channel);
    seen.add(record.videoId);
    ids.push(record.videoId);
  }
  for (const record of eligible) {
    if (ids.length >= limit) break;
    if (seen.has(record.videoId)) continue;
    seen.add(record.videoId);
    ids.push(record.videoId);
  }
  return ids;
}

export const getHomeFeedRevision = () => revision;

export function subscribeHomeFeedRevision(listener: () => void) {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}

export function invalidateHomeFeedCache() {
  revision += 1;
  loads.clear();
  for (const listener of listeners) listener();
}

export function getHomeFeedLoad(
  key: string,
  createSources: () => Promise<HomeFeedSources>,
  refresh = false,
): HomeFeedLoad {
  const now = Date.now();
  for (const [entryKey, entry] of loads) {
    if (now - entry.startedAt >= (entry.videos ? CACHE_TTL_MS : PENDING_TTL_MS)) loads.delete(entryKey);
  }
  const existing = loads.get(key);
  if (!refresh && existing) return existing;

  const load: HomeFeedLoad = {
    key,
    startedAt: now,
    sources: Promise.resolve().then(createSources),
    videos: null,
  };
  loads.delete(key);
  loads.set(key, load);
  while (loads.size > MAX_ENTRIES) loads.delete(loads.keys().next().value!);
  void load.sources.catch(() => {
    if (loads.get(key) === load) loads.delete(key);
  });
  return load;
}

export function cacheHomeFeed(load: HomeFeedLoad, videos: VideoSummary[]) {
  if (loads.get(load.key) !== load || videos.length === 0) return;
  load.videos = [...videos];
}
