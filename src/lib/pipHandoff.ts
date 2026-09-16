import { emit, listen } from "@tauri-apps/api/event";

import { useAppSettingsStore } from "../store/useAppSettingsStore";
import { usePlayerStore, type PlayerStoreApi } from "../store/usePlayerStore";
import { logToBackend } from "./diagnostics";
import { SETTINGS } from "./settings/schema";
import { readStreamInfoEntry } from "./streamResolution";
import {
  PIP_EVENTS,
  closePipWindow,
  openPipWindow,
  type PipTakeoverPayload,
} from "./api/pip";

/** Main window → pop-out: hand playback back to the full player. */
export const PIP_RETURN_REQUEST_EVENT = "flow:pip-return-requested";

/**
 * How long this window keeps playing while the pop-out warms up. Long enough to
 * cover a cold webview on a slow machine; past it the swap happens anyway rather
 * than leaving two windows believing they own playback.
 */
const HANDOFF_READY_TIMEOUT_MS = 10_000;
let popoutOwner: PlayerStoreApi | null = null;
let pendingPopout: Promise<boolean> | null = null;
export const ownsPopout = (store: PlayerStoreApi) => popoutOwner === store;

export async function returnOtherPopout(playerStore: PlayerStoreApi) {
  if (!popoutOwner || popoutOwner === playerStore) return;
  const previous = popoutOwner;
  const state = previous.getState();
  popoutOwner = null;
  await closePipWindow().catch(() => {});
  if (state.currentVideo && previous.getState().videoPlayerMode === "window") {
    state.setPipHandoff(state.currentVideo.id, state.currentTime, state.isPlaying);
    state.expandVideoPlayer();
  }
}

type HandoffOutcome = "ready" | "timeout" | "aborted";

/**
 * Waits for the pop-out to report that it is actually decoding. Also watches for
 * a handback, which is how a pop-out closed before it ever started announces
 * itself — that aborts the swap and leaves playback here.
 */
function waitForPopoutReady(): Promise<HandoffOutcome> {
  return new Promise((resolve) => {
    let settled = false;
    const disposers: Array<() => void> = [];

    const finish = (outcome: HandoffOutcome) => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timer);
      for (const dispose of disposers) dispose();
      resolve(outcome);
    };

    const timer = window.setTimeout(() => finish("timeout"), HANDOFF_READY_TIMEOUT_MS);

    const track = (pending: Promise<() => void>) => {
      void pending
        .then((dispose) => {
          if (settled) dispose();
          else disposers.push(dispose);
        })
        .catch(() => finish("timeout"));
    };

    track(listen(PIP_EVENTS.ready, () => finish("ready")));
    track(listen(PIP_EVENTS.handback, () => finish("aborted")));
  });
}

/**
 * Moves playback from the main window into the pop-out player window.
 *
 * Two OS windows are two webviews with separate DOM, so the media element cannot
 * travel between them the way the in-app mini player moves a node — the pop-out
 * has to build its own. What it does not have to do is start from nothing: it
 * inherits this window's resolved stream and settings, warms up silently while
 * this window keeps playing, and only then does the swap, seeking to the exact
 * position playback reached. The audible gap is one in-buffer seek rather than a
 * reload.
 *
 * Returns false when the pop-out could not be opened, so callers can fall back
 * to the in-app mini player rather than dropping playback.
 */
export async function openPopoutPlayer(playerStore: PlayerStoreApi = usePlayerStore): Promise<boolean> {
  const previous = pendingPopout;
  const request = (async () => {
    await previous?.catch(() => false);
    await returnOtherPopout(playerStore);
    return openOwnedPopout(playerStore);
  })();
  pendingPopout = request;
  try {
    return await request;
  } finally {
    if (pendingPopout === request) pendingPopout = null;
  }
}

async function openOwnedPopout(playerStore: PlayerStoreApi): Promise<boolean> {
  const store = playerStore.getState();
  const video = store.currentVideo;
  if (!video) return false;

  const hasQueue = store.queue.length > 0;
  const queue = hasQueue ? store.queue : [video];
  const currentIndex = hasQueue ? Math.max(store.currentIndex, 0) : 0;
  const settingsValues = useAppSettingsStore.getState().values;
  const alwaysOnTop = settingsValues[SETTINGS.PIP_ALWAYS_ON_TOP] !== "false";
  const cachedStream = readStreamInfoEntry(video.id);
  // A paused handoff has nothing to keep playing through, so it swaps at once.
  const silentUntilTakeover = store.isPlaying;

  popoutOwner = playerStore;
  try {
    await openPipWindow({
      queue,
      currentIndex,
      positionSeconds: store.currentTime,
      playing: store.isPlaying,
      alwaysOnTop,
      settings: settingsValues as Record<string, string>,
      stream: cachedStream ? { info: cachedStream.info, resolvedAt: cachedStream.resolvedAt } : null,
      silentUntilTakeover,
    });
  } catch (error) {
    if (popoutOwner === playerStore) popoutOwner = null;
    void logToBackend("warn", "pop-out player window failed to open", {
      videoId: video.id,
      cause: String(error),
    });
    return false;
  }

  if (popoutOwner !== playerStore || playerStore.getState().currentVideo?.id !== video.id) {
    await dismissPopoutPlayer(playerStore);
    return true;
  }

  if (!silentUntilTakeover) {
    playerStore.getState().enterVideoWindowPip();
    return true;
  }

  const outcome = await waitForPopoutReady();
  if (outcome === "aborted") {
    // The pop-out went away before it played anything; playback never left.
    void logToBackend("info", "pop-out handoff aborted before takeover", { videoId: video.id });
    return true;
  }

  const current = playerStore.getState();
  // The video can have changed or stopped while the pop-out was warming up.
  if (popoutOwner !== playerStore || !current.currentVideo || current.currentVideo.id !== video.id) {
    await dismissPopoutPlayer(playerStore);
    return true;
  }

  await emit(PIP_EVENTS.takeover, {
    positionSeconds: current.currentTime,
    volume: current.volume,
    muted: current.muted,
  } satisfies PipTakeoverPayload).catch(() => {});

  playerStore.getState().enterVideoWindowPip();
  if (outcome === "timeout") {
    void logToBackend("warn", "pop-out handoff swapped without a ready signal", {
      videoId: video.id,
    });
  }
  return true;
}

/**
 * Asks the pop-out to return playback to the main window. The pop-out owns the
 * authoritative position, so the request goes to it rather than closing the
 * window from here and guessing where playback got to.
 */
export async function requestPopoutReturn(): Promise<void> {
  await emit(PIP_RETURN_REQUEST_EVENT).catch(() => {});
}

/** Closes the pop-out outright — used when this window takes over playback. */
export async function dismissPopoutPlayer(playerStore: PlayerStoreApi = usePlayerStore): Promise<void> {
  if (popoutOwner !== playerStore) return;
  popoutOwner = null;
  await closePipWindow().catch(() => {});
}
