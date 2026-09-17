import { useEffect, useState } from "react";

import { proxyImageUrl } from "./api/images";

type CachedProxyUrl = { url: string; refreshAt: number; failures: number };
type ImageSubscriber = {
  apply: (entry: CachedProxyUrl) => void;
  refresh: () => void;
};
type ActiveImage = {
  entry: CachedProxyUrl | undefined;
  listeners: Set<ImageSubscriber>;
};

const MAX_CACHED_URLS = 512;
const REFRESH_MARGIN_MS = 5 * 60 * 1000;
const RETRY_DELAY_MS = 60 * 1000;
const proxyCache = new Map<string, CachedProxyUrl>();
const pending = new Map<string, Promise<CachedProxyUrl>>();
const subscribers = new Map<string, ActiveImage>();

function isLoopbackUrl(url: string): boolean {
  return /^https?:\/\/(?:127\.0\.0\.1|localhost)(?::\d+)?\//i.test(url);
}

function cacheUrl(source: string, entry: CachedProxyUrl): CachedProxyUrl {
  proxyCache.delete(source);
  const active = subscribers.get(source);
  if (active) {
    active.entry = entry;
    active.listeners.forEach((subscriber) => subscriber.apply(entry));
  } else {
    proxyCache.set(source, entry);
    while (proxyCache.size > MAX_CACHED_URLS) {
      const oldest = proxyCache.keys().next().value;
      if (oldest === undefined) break;
      proxyCache.delete(oldest);
    }
  }
  return entry;
}

function cachedUrl(source: string): CachedProxyUrl | undefined {
  return subscribers.get(source)?.entry ?? proxyCache.get(source);
}

function sourceForProxyUrl(url: string): string | undefined {
  for (const [source, { entry }] of subscribers) {
    if (entry?.url === url && entry.url !== source) return source;
  }
  for (const [source, entry] of proxyCache) {
    if (entry.url === url && entry.url !== source) return source;
  }
  return undefined;
}

function fallbackUrl(source: string): CachedProxyUrl {
  const failures = (cachedUrl(source)?.failures ?? 0) + 1;
  return cacheUrl(source, {
    url: source,
    refreshAt: failures < 2 ? Date.now() + RETRY_DELAY_MS : Number.POSITIVE_INFINITY,
    failures,
  });
}

function resolveProxyUrl(source: string): Promise<CachedProxyUrl> {
  const cached = cachedUrl(source);
  if (cached && cached.refreshAt > Date.now()) return Promise.resolve(cached);
  const existing = pending.get(source);
  if (existing) return existing;
  const request = proxyImageUrl(source)
    .then(({ url, expiresAt }) => cacheUrl(source, {
      url,
      refreshAt: expiresAt * 1000 - REFRESH_MARGIN_MS,
      failures: cachedUrl(source)?.failures ?? 0,
    }))
    .catch(() => fallbackUrl(source))
    .finally(() => {
      if (pending.get(source) === request) pending.delete(source);
    });
  pending.set(source, request);
  return request;
}

function refreshVisibleImages(): void {
  if (document.visibilityState === "hidden") return;
  subscribers.forEach(({ listeners }) => listeners.forEach((subscriber) => subscriber.refresh()));
}

function handleImageError(event: Event): void {
  if (!(event.target instanceof HTMLImageElement)) return;
  const failedUrl = event.target.currentSrc || event.target.src;
  const source = sourceForProxyUrl(failedUrl);
  if (source !== undefined) {
    event.stopImmediatePropagation();
    fallbackUrl(source);
  }
}

function handleImageLoad(event: Event): void {
  if (!(event.target instanceof HTMLImageElement)) return;
  const loadedUrl = event.target.currentSrc || event.target.src;
  const source = sourceForProxyUrl(loadedUrl);
  const entry = source === undefined ? undefined : cachedUrl(source);
  if (entry) entry.failures = 0;
}

function subscribeImage(source: string, subscriber: ImageSubscriber): () => void {
  if (subscribers.size === 0) {
    window.addEventListener("focus", refreshVisibleImages);
    window.addEventListener("pageshow", refreshVisibleImages);
    window.addEventListener("error", handleImageError, true);
    document.addEventListener("load", handleImageLoad, true);
    document.addEventListener("visibilitychange", refreshVisibleImages);
  }
  const active = subscribers.get(source) ?? {
    entry: proxyCache.get(source),
    listeners: new Set<ImageSubscriber>(),
  };
  proxyCache.delete(source);
  active.listeners.add(subscriber);
  subscribers.set(source, active);
  return () => {
    active.listeners.delete(subscriber);
    if (active.listeners.size === 0) {
      subscribers.delete(source);
      if (active.entry) cacheUrl(source, active.entry);
    }
    if (subscribers.size === 0) {
      window.removeEventListener("focus", refreshVisibleImages);
      window.removeEventListener("pageshow", refreshVisibleImages);
      window.removeEventListener("error", handleImageError, true);
      document.removeEventListener("load", handleImageLoad, true);
      document.removeEventListener("visibilitychange", refreshVisibleImages);
    }
  };
}

export function useProxiedImageUrl(src: string | null | undefined): string | undefined {
  const normalized = src?.trim();
  const passthrough = !!normalized && (isLoopbackUrl(normalized) || !/^https?:\/\//i.test(normalized));
  const [resolved, setResolved] = useState<{ source: string; url: string } | null>(null);

  useEffect(() => {
    if (!normalized || passthrough) return;

    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const apply = (entry: CachedProxyUrl) => {
      if (!active) return;
      setResolved((previous) => previous?.source === normalized && previous.url === entry.url
        ? previous
        : { source: normalized, url: entry.url });
      clearTimeout(timer);
      if (Number.isFinite(entry.refreshAt)) {
        timer = setTimeout(refresh, Math.max(1000, entry.refreshAt - Date.now()));
      }
    };
    const refresh = () => {
      if (active) void resolveProxyUrl(normalized).then(apply);
    };
    const unsubscribe = subscribeImage(normalized, { apply, refresh });
    refresh();

    return () => {
      active = false;
      clearTimeout(timer);
      unsubscribe();
    };
  }, [normalized, passthrough]);

  if (!normalized) return undefined;
  if (passthrough) return normalized;
  if (resolved?.source === normalized) return resolved.url;
  const cached = cachedUrl(normalized);
  return cached && cached.refreshAt > Date.now() ? cached.url : undefined;
}
