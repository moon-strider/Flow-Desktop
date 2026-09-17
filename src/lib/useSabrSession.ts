import { useEffect, useRef, useState } from "react";
import { acquireSabrSession, releaseSabrSession, touchSabrSession } from "./api/sabr";

function sessionFromManifest(manifestUrl: string | null | undefined) {
  if (!manifestUrl) return null;
  try {
    const url = new URL(manifestUrl);
    if (url.hostname !== "127.0.0.1") return null;
    return /^\/sabr\/(s\d+)\/manifest\.mpd$/.exec(url.pathname)?.[1] ?? null;
  } catch {
    return null;
  }
}

export function useSabrSession(
  manifestUrl: string | null | undefined,
  onError: (error: unknown) => void,
) {
  const sessionId = sessionFromManifest(manifestUrl);
  const [readyManifest, setReadyManifest] = useState<string | null>(null);
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  useEffect(() => {
    if (!sessionId) {
      setReadyManifest(null);
      return;
    }
    const leaseId = crypto.randomUUID();
    let disposed = false;
    let renewing = false;
    let acquired = false;
    setReadyManifest(null);

    const release = () => releaseSabrSession(sessionId, leaseId).catch(() => {});
    const acquire = async () => {
      await acquireSabrSession(sessionId, leaseId);
      acquired = true;
      if (disposed) await release();
      else setReadyManifest(manifestUrl ?? null);
    };
    const renew = async () => {
      if (disposed || !acquired || renewing) return;
      renewing = true;
      try {
        if (!(await touchSabrSession(sessionId, leaseId)) && !disposed) await acquire();
      } catch (error) {
        if (!disposed) onErrorRef.current(error);
      } finally {
        renewing = false;
      }
    };
    void acquire().catch((error) => {
      if (!disposed) onErrorRef.current(error);
    });
    const interval = window.setInterval(() => { void renew(); }, 30_000);
    const onFocus = () => { void renew(); };
    window.addEventListener("focus", onFocus);
    return () => {
      disposed = true;
      window.clearInterval(interval);
      window.removeEventListener("focus", onFocus);
      if (acquired) void release();
    };
  }, [sessionId, manifestUrl]);

  return !sessionId || readyManifest === manifestUrl;
}
