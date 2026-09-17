import { useEffect, useState } from "react";

import { proxyImageUrl } from "./api/images";

const proxyCache = new Map<string, string>();
const pending = new Map<string, Promise<string>>();

function isLoopbackUrl(url: string): boolean {
  return /^https?:\/\/(?:127\.0\.0\.1|localhost)(?::\d+)?\//i.test(url);
}

export function useProxiedImageUrl(src: string | null | undefined): string | undefined {
  const normalized = src?.trim();
  const passthrough = !!normalized && (isLoopbackUrl(normalized) || !/^https?:\/\//i.test(normalized));
  const [resolved, setResolved] = useState<{ source: string; url: string } | null>(null);

  useEffect(() => {
    if (!normalized || passthrough) return;

    const cached = proxyCache.get(normalized);
    if (cached) {
      return;
    }

    let active = true;
    const request = pending.get(normalized) ?? proxyImageUrl(normalized);
    pending.set(normalized, request);
    request
      .then((url) => {
        proxyCache.set(normalized, url);
        if (active) setResolved({ source: normalized, url });
      })
      .catch(() => {
        if (active) setResolved({ source: normalized, url: normalized });
      })
      .finally(() => {
        if (pending.get(normalized) === request) {
          pending.delete(normalized);
        }
      });

    return () => {
      active = false;
    };
  }, [normalized, passthrough]);

  if (!normalized) return undefined;
  if (passthrough) return normalized;
  return proxyCache.get(normalized) ?? (resolved?.source === normalized ? resolved.url : undefined);
}
