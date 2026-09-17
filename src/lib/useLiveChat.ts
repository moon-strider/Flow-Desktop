import { useCallback, useEffect, useRef, useState } from "react";
import { getLiveChat } from "./api/youtube";
import { useVisibleWork } from "./useVisibleWork";
import type { LiveChatMessage } from "../types/video";

const MAX_MESSAGES = 200;
const MAX_SEEN_IDS = 1500;
const RETRY_MS = 3000;
const MAX_RETRY_MS = 30000;
const MAX_FAILURES = 6;
const MAX_RESEEDS = 3;
const MIN_POLL_MS = 800;

export interface LiveChatState {
  messages: LiveChatMessage[];
  loading: boolean;
  // Chat is unavailable for this video, or its stream has closed.
  ended: boolean;
  reconnect: () => void;
}

interface ChatSession {
  videoId: string;
  reconnectNonce: number;
  continuation: string | null;
  seen: Set<string>;
  failures: number;
  reseeds: number;
  delay: number;
  nextPollAt: number;
  ended: boolean;
  pending: Promise<void> | null;
}

/**
 * Polls YouTube's native live chat for `videoId` while `enabled`. Seeds the continuation token
 * on the first call, then walks the continuation chain at the server-recommended cadence,
 * de-duplicating by message id and capping the in-memory backlog.
 *
 * A chain that runs out of continuations is re-seeded rather than treated as the end of chat:
 * YouTube drops the chain on its own often enough that giving up on the first gap leaves a live
 * stream with a dead panel. Only a chain that will not re-seed is reported as ended.
 */
export function useLiveChat(videoId: string | undefined, enabled: boolean): LiveChatState {
  const [messages, setMessages] = useState<LiveChatMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const [ended, setEnded] = useState(false);
  const [reconnectNonce, setReconnectNonce] = useState(0);
  const visible = useVisibleWork();
  const sessionRef = useRef<ChatSession | null>(null);
  const reconnect = useCallback(() => setReconnectNonce((value) => value + 1), []);

  useEffect(() => {
    if (!videoId) {
      sessionRef.current = null;
      setMessages([]);
      setLoading(false);
      setEnded(false);
      return;
    }

    const previous = sessionRef.current;
    if (previous?.videoId !== videoId || previous.reconnectNonce !== reconnectNonce) {
      const sameVideo = previous?.videoId === videoId;
      sessionRef.current = {
        videoId,
        reconnectNonce,
        continuation: null,
        seen: sameVideo ? previous.seen : new Set(),
        failures: 0,
        reseeds: 0,
        delay: RETRY_MS,
        nextPollAt: 0,
        ended: false,
        pending: null,
      };
      if (!sameVideo) setMessages([]);
      setEnded(false);
    }
    const session = sessionRef.current!;
    if (!enabled || !visible || session.ended) {
      setLoading(false);
      return;
    }

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    setLoading(true);

    const fetchPage = async () => {
      try {
        const page = await getLiveChat(videoId, session.continuation);
        if (sessionRef.current !== session) return;
        setLoading(false);
        const fresh = page.messages.filter((message) => !session.seen.has(message.id));
        if (fresh.length > 0) {
          for (const message of fresh) session.seen.add(message.id);
          if (session.seen.size > MAX_SEEN_IDS) {
            session.seen = new Set(Array.from(session.seen).slice(-MAX_SEEN_IDS));
          }
          setMessages((previousMessages) => [...previousMessages, ...fresh].slice(-MAX_MESSAGES));
        }
        session.continuation = page.continuation ?? null;
        if (!page.continuation) {
          if (session.reseeds >= MAX_RESEEDS) {
            session.ended = true;
            setEnded(true);
            return;
          }
          session.reseeds += 1;
          session.delay = RETRY_MS;
          return;
        }
        session.failures = 0;
        session.reseeds = 0;
        session.delay = Math.max(MIN_POLL_MS, page.pollingIntervalMs || 2000);
      } catch (error) {
        if (sessionRef.current !== session) return;
        session.failures += 1;
        session.continuation = null;
        console.warn("Live chat poll failed", error);
        if (session.failures >= MAX_FAILURES) {
          session.ended = true;
          setLoading(false);
          setEnded(true);
          return;
        }
        session.delay = Math.min(MAX_RETRY_MS, RETRY_MS * 2 ** (session.failures - 1));
      } finally {
        session.nextPollAt = Date.now() + session.delay;
        session.pending = null;
      }
    };

    const poll = async () => {
      if (cancelled) return;
      if (!session.pending && session.nextPollAt > Date.now()) {
        setLoading(false);
        timer = setTimeout(() => void poll(), session.nextPollAt - Date.now());
        return;
      }
      session.pending ??= fetchPage();
      await session.pending;
      if (cancelled || sessionRef.current !== session || session.ended) return;
      setLoading(false);
      timer = setTimeout(() => void poll(), Math.max(0, session.nextPollAt - Date.now()));
    };
    void poll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [videoId, enabled, visible, reconnectNonce]);

  return { messages, loading, ended, reconnect };
}
