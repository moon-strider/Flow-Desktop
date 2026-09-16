import { logToBackend } from "./diagnostics";
import { useTabContext } from "./tabContext";
import { useTabsStore } from "../store/useTabsStore";
import { useEffect, useRef } from "react";
import { listen } from "@tauri-apps/api/event";
import { useNavigate } from "react-router-dom";

import { usePlayerStoreApi } from "../store/usePlayerStore";
import {
  PIP_EVENTS,
  focusMainWindow,
  type PipHandbackPayload,
  type PipProgressPayload,
  type PipVideoChangedPayload,
} from "./api/pip";
import { dismissPopoutPlayer, ownsPopout } from "./pipHandoff";

export function usePipController() {
  const playerStore = usePlayerStoreApi();
  const tab = useTabContext();
  const navigate = useNavigate();
  // Read through refs: the listeners are registered once, and re-registering
  // them on every route change would drop events mid-swap.
  const navigateRef = useRef(navigate);
  navigateRef.current = navigate;

  useEffect(() => {
    const subscriptions = [
      listen<PipProgressPayload>(PIP_EVENTS.progress, ({ payload }) => {
        const store = playerStore.getState();
        if (store.videoPlayerMode !== "window" || !ownsPopout(playerStore)) return;
        if (store.currentVideo?.id !== payload.videoId) return;
        store.applyPipRemoteState({
          currentTime: payload.positionSeconds,
          duration: payload.durationSeconds,
          isPlaying: payload.playing,
        });
      }),
      listen<PipVideoChangedPayload>(PIP_EVENTS.videoChanged, ({ payload }) => {
        const store = playerStore.getState();
        if (store.videoPlayerMode !== "window" || !ownsPopout(playerStore)) return;
        store.applyPipRemoteState({ video: payload.video });
      }),
      listen<PipHandbackPayload>(PIP_EVENTS.handback, async ({ payload }) => {
        const store = playerStore.getState();
        if (store.videoPlayerMode !== "window" || !ownsPopout(playerStore)) return;
        const video = store.currentVideo;
        if (!video || video.id !== payload.videoId) {
          store.expandVideoPlayer();
          return;
        }

        const playing = payload.playing;
        void logToBackend("info", "pop-out playback returned", { playing, expand: payload.expand });
        store.setPipHandoff(video.id, payload.positionSeconds, playing);
        store.setIsPlaying(playing);
        store.setCurrentTime(payload.positionSeconds);
        if (Number.isFinite(payload.volume)) store.setVolume(payload.volume);
        store.setMuted(payload.muted);
        store.expandVideoPlayer();
        if (payload.expand || playing) {
          if (tab.id) useTabsStore.getState().activateTab(tab.id);
          navigateRef.current(`/watch/${video.id}`);
          if (!payload.expand) await focusMainWindow().catch(() => {});
        }
      }),
    ];

    return () => {
      for (const subscription of subscriptions) {
        void subscription.then((unlisten) => unlisten()).catch(() => {});
      }
    };
  }, [playerStore, tab.id]);

  // Whatever ends the "playing in the pop-out" state here — a new video, the
  // queue being cleared, the watch page taking playback back — must also take
  // the pop-out window down with it.
  useEffect(() => () => { void dismissPopoutPlayer(playerStore); }, [playerStore]);

  useEffect(() => {
    let previousMode = playerStore.getState().videoPlayerMode;
    return playerStore.subscribe((state) => {
      const mode = state.videoPlayerMode;
      if (mode === previousMode) return;
      const left = previousMode === "window";
      previousMode = mode;
      if (left) void dismissPopoutPlayer(playerStore);
    });
  }, [playerStore]);
}
