import { useTabContext } from "../../lib/tabContext";
import { useTabsStore } from "../../store/useTabsStore";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Maximize2, X } from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";

import { usePlayerStore } from "../../store/usePlayerStore";
import { FlowPlayerCore } from "./FlowPlayerCore";
import { useMediaSessionMetadata } from "../../lib/useMediaSessionMetadata";

type PlayerBounds = {
  top: number;
  left: number;
  width: number;
  height: number;
};

const WATCH_ROUTE_RE = /^\/watch\/([^/?#]+)/;

function watchVideoIdFromPath(pathname: string) {
  const match = pathname.match(WATCH_ROUTE_RE);
  return match?.[1] ? decodeURIComponent(match[1]) : null;
}

function readSlotBounds(root: ParentNode): PlayerBounds | null {
  const slot = root.querySelector<HTMLElement>("[data-flow-watch-player-slot='true']");
  if (!slot) return null;
  const rect = slot.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) return null;
  return {
    top: rect.top,
    left: rect.left,
    width: rect.width,
    height: rect.height,
  };
}

function boundsEqual(a: PlayerBounds | null, b: PlayerBounds | null) {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.top === b.top && a.left === b.left && a.width === b.width && a.height === b.height;
}

export function GlobalVideoPlayer() {
  const tab = useTabContext();
  const navigate = useNavigate();
  const location = useLocation();
  const currentVideo = usePlayerStore((s) => s.currentVideo);
  const videoPlayerMode = usePlayerStore((s) => s.videoPlayerMode);
  const isVideoFullscreen = usePlayerStore((s) => s.isVideoFullscreen);
  const isVideoFullscreenTransitioning = usePlayerStore((s) => s.isVideoFullscreenTransitioning);
  const watchPageCache = usePlayerStore((s) => s.watchPageCache);
  const expandVideoPlayer = usePlayerStore((s) => s.expandVideoPlayer);
  const dismissVideoPlayer = usePlayerStore((s) => s.dismissVideoPlayer);
  const setIsPlaying = usePlayerStore((s) => s.setIsPlaying);

  const [slotBounds, setSlotBounds] = useState<PlayerBounds | null>(null);
  const frameRef = useRef<HTMLDivElement | null>(null);
  const lastSlotBoundsRef = useRef<PlayerBounds | null>(null);
  const previousVideoIdRef = useRef(currentVideo?.id ?? null);

  const inWatchPage = watchVideoIdFromPath(location.pathname) === currentVideo?.id;
  const isFloating = tab.id
    ? tab.ownsPip && videoPlayerMode === "pip"
    : videoPlayerMode === "pip";
  const visible = (tab.active && inWatchPage) || isFloating;
  // The pop-out window owns playback in this mode, so this window renders no
  // media element — two decoding the same video would double audio and CPU.
  const isPoppedOut = videoPlayerMode === "window";
  const cachedDetails =
    currentVideo && watchPageCache?.videoId === currentVideo.id
      ? watchPageCache.videoDetails
      : null;

  useEffect(() => {
    const previousVideoId = previousVideoIdRef.current;
    const nextVideoId = currentVideo?.id ?? null;
    previousVideoIdRef.current = nextVideoId;

    if (!nextVideoId || previousVideoId === nextVideoId) return;
    const routeVideoId = watchVideoIdFromPath(location.pathname);
    if (routeVideoId && routeVideoId !== nextVideoId) {
      navigate(`/watch/${nextVideoId}`, { replace: true });
    }
  }, [currentVideo?.id, location.pathname, navigate]);

  // While the pop-out owns playback it also owns the OS transport controls.
  const manualPipId = useTabsStore((state) => state.manualPipId);
  useMediaSessionMetadata(!isPoppedOut && (manualPipId ? manualPipId === tab.id : tab.active));

  useEffect(() => {
    const frame = frameRef.current;
    if (!frame || isFloating || isPoppedOut || isVideoFullscreen || !currentVideo) return;

    const handleWheel = (event: WheelEvent) => {
      if (event.defaultPrevented || event.ctrlKey || event.metaKey) return;

      for (
        let target = event.target instanceof Element ? event.target : null;
        target && target !== frame;
        target = target.parentElement
      ) {
        const style = window.getComputedStyle(target);
        if (
          (/^(auto|scroll)$/.test(style.overflowY) && target.scrollHeight > target.clientHeight)
          || (/^(auto|scroll)$/.test(style.overflowX) && target.scrollWidth > target.clientWidth)
        ) return;
      }

      const slot = (tab.rootRef?.current ?? document).querySelector<HTMLElement>("[data-flow-watch-player-slot='true']");
      const scrollContainer = slot?.closest("main");
      if (!scrollContainer) return;

      const lineHeight = Number.parseFloat(window.getComputedStyle(scrollContainer).lineHeight) || 16;
      const scaleY = event.deltaMode === 1 ? lineHeight : event.deltaMode === 2 ? scrollContainer.clientHeight : 1;
      const scaleX = event.deltaMode === 1 ? lineHeight : event.deltaMode === 2 ? scrollContainer.clientWidth : 1;
      event.preventDefault();
      scrollContainer.scrollBy({
        top: event.deltaY * scaleY,
        left: event.deltaX * scaleX,
        behavior: "instant",
      });
    };

    frame.addEventListener("wheel", handleWheel, { passive: false });
    return () => frame.removeEventListener("wheel", handleWheel);
  }, [currentVideo, isFloating, isPoppedOut, isVideoFullscreen, location.pathname]);

  useEffect(() => {
    if (!tab.active || isFloating || isPoppedOut || isVideoFullscreen || !currentVideo) return;

    const writeFrameBounds = (bounds: PlayerBounds) => {
      const frame = frameRef.current;
      if (!frame) return;
      frame.style.top = `${bounds.top}px`;
      frame.style.left = `${bounds.left}px`;
      frame.style.width = `${bounds.width}px`;
      frame.style.height = `${bounds.height}px`;
    };

    const sync = () => {
      const next = readSlotBounds(tab.rootRef?.current ?? document);
      if (!next) return;
      const prev = lastSlotBoundsRef.current;
      lastSlotBoundsRef.current = next;
      writeFrameBounds(next);
      if (!prev || prev.width !== next.width || prev.height !== next.height) {
        setSlotBounds((old) => (boundsEqual(old, next) ? old : next));
      }
    };

    sync();
    const slot = (tab.rootRef?.current ?? document).querySelector<HTMLElement>("[data-flow-watch-player-slot='true']");
    const observer = new ResizeObserver(sync);
    if (slot) observer.observe(slot);
    window.addEventListener("resize", sync);

    let scrollRaf: number | null = null;
    const onScroll = () => {
      if (scrollRaf !== null) return;
      scrollRaf = window.requestAnimationFrame(() => {
        scrollRaf = null;
        sync();
      });
    };
    document.addEventListener("scroll", onScroll, { capture: true, passive: true });

    // Late layout (fonts, images, side panels) can move the slot for a few
    // frames after a route/mode change; each settle tick also writes
    // imperatively through sync().
    let settleCount = 0;
    let settleRaf = window.requestAnimationFrame(function settle() {
      sync();
      if (++settleCount < 8) settleRaf = window.requestAnimationFrame(settle);
    });

    return () => {
      observer.disconnect();
      window.removeEventListener("resize", sync);
      document.removeEventListener("scroll", onScroll, { capture: true });
      if (scrollRaf !== null) window.cancelAnimationFrame(scrollRaf);
      window.cancelAnimationFrame(settleRaf);
    };
  }, [isFloating, isPoppedOut, isVideoFullscreen, currentVideo, location.pathname, tab.active, tab.rootRef]);

  const expandFromFloating = useCallback(() => {
    if (!currentVideo) return;
    if (tab.id) useTabsStore.getState().activateTab(tab.id);
    expandVideoPlayer();
    navigate(`/watch/${currentVideo.id}`);
  }, [currentVideo, expandVideoPlayer, navigate, tab.id]);

  useEffect(() => {
    const expand = (event: Event) => {
      const id = (event as CustomEvent<{ tabId?: string }>).detail?.tabId;
      if (id ? id === tab.id : tab.active) expandFromFloating();
    };
    window.addEventListener("flow-video-expand-request", expand);
    return () => window.removeEventListener("flow-video-expand-request", expand);
  }, [expandFromFloating, tab.id, tab.active]);

  const frameStyle = useMemo(() => {
    if (isVideoFullscreen) {
      return {
        inset: 0,
        width: "100%",
        height: "100%",
      } as const;
    }

    if (isFloating) {
      return {
        top: "auto",
        left: "auto",
        height: "auto",
        bottom: "24px",
        right: "24px",
        width: "min(420px, calc(100vw - 32px))",
        aspectRatio: "16 / 9",
      } as const;
    }

    if (!slotBounds) {
      return {
        opacity: 0,
        pointerEvents: "none" as const,
      };
    }

    return {
      top: `${slotBounds.top}px`,
      left: `${slotBounds.left}px`,
      width: `${slotBounds.width}px`,
      height: `${slotBounds.height}px`,
    };
  }, [isFloating, isVideoFullscreen, slotBounds]);

  const videoIdForPlayer = currentVideo?.id ?? null;
  const playerNode = useMemo(
    () =>
      videoIdForPlayer ? (
        <FlowPlayerCore videoId={videoIdForPlayer} videoDetails={cachedDetails} compact={isFloating} />
      ) : null,
    [videoIdForPlayer, cachedDetails, isFloating],
  );

  if (!currentVideo || isPoppedOut) return null;

  return (
    <>
      <div
        ref={frameRef}
        inert={!visible}
        data-flow-player-tab={tab.id ?? undefined}
        data-floating={isFloating && visible ? "true" : "false"}
        data-player-visible={visible ? "true" : "false"}
        className={
          isVideoFullscreen
            ? "fixed z-[300] overflow-hidden bg-chrome-black"
            : isFloating
            ? "group fixed z-50 overflow-hidden rounded-xl bg-chrome-black shadow-2xl ring-1 ring-chrome-white/10"
            : "fixed z-30 bg-chrome-black"
        }
        style={{ ...frameStyle, ...(!visible ? { visibility: "hidden", pointerEvents: "none" } as const : {}) }}
      >
        {isFloating && (
          <div className="absolute right-2 top-2 z-40 flex items-center gap-1 rounded-full bg-chrome-black/80 p-1 opacity-0 transition-opacity duration-200 ease-out group-hover:opacity-100">
            <button
              type="button"
              aria-label="Expand video"
              onClick={expandFromFloating}
              className="grid h-7 w-7 place-items-center rounded-full text-chrome-white hover:bg-chrome-white/15"
            >
              <Maximize2 size={16} />
            </button>
            <button
              type="button"
              aria-label="Close video"
              onClick={() => {
                if (!tab.id) { dismissVideoPlayer(); return; }
                setIsPlaying(false);
                expandVideoPlayer();
                useTabsStore.getState().dismissPip(tab.id);
              }}
              className="grid h-7 w-7 place-items-center rounded-full text-chrome-white hover:bg-chrome-white/15"
            >
              <X size={16} />
            </button>
          </div>
        )}
        <div className="group h-full w-full">{playerNode}</div>
      </div>

      {isVideoFullscreenTransitioning && (
        <div className="pointer-events-none fixed inset-0 z-[400] bg-chrome-black" />
      )}
    </>
  );
}
