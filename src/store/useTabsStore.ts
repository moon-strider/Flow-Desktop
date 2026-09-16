import { create } from "zustand";
import { createPath, parsePath, type Location, type To } from "react-router-dom";
import { createPlayerStore, type PlayerStoreApi, type VideoPlayerMode, type VideoPipIntent } from "./usePlayerStore";
import type { VideoSummary } from "../types/video";
import { getString } from "../lib/i18n/index";

export interface TabPlayback {
  videoId: string | null;
  playing: boolean;
  mode: VideoPlayerMode;
  intent: VideoPipIntent | null;
  fullscreen: boolean;
}

export interface AppTab {
  id: string;
  entries: Location[];
  index: number;
  title: string;
  visited: boolean;
  player: PlayerStoreApi;
  playback: TabPlayback;
}

export function routeTitle(location: Pick<Location, "pathname" | "search">) {
  if (location.pathname === "/") return getString("tabs_home");
  if (location.pathname === "/search") return new URLSearchParams(location.search).get("q") || getString("tabs_search");
  if (location.pathname.startsWith("/watch/")) return getString("tabs_video");
  const section = location.pathname.split("/").filter(Boolean)[0] || "Flow";
  return section.charAt(0).toUpperCase() + section.slice(1).split("-").join(" ");
}

function makeLocation(to: To, state: unknown = null): Location {
  const path = typeof to === "string" ? parsePath(to) : to;
  return { pathname: path.pathname || "/", search: path.search || "", hash: path.hash || "", state, key: crypto.randomUUID() };
}

function makeTab(path: string, visited: boolean, video?: VideoSummary): AppTab {
  const location = makeLocation(path);
  const player = createPlayerStore();
  if (video) player.setState({ currentVideo: video, queue: [video], currentIndex: 0, isPlaying: true, duration: video.durationSeconds || 0 });
  return {
    id: crypto.randomUUID(), entries: [location], index: 0,
    title: video?.title || routeTitle(location), visited, player,
    playback: { videoId: null, playing: false, mode: "watch", intent: null, fullscreen: false },
  };
}

interface TabsState {
  tabs: AppTab[];
  activeId: string;
  manualPipId: string | null;
  openTab: (path?: string, options?: { background?: boolean; video?: VideoSummary }) => void;
  activateTab: (id: string) => void;
  closeTab: (id: string) => void;
  navigateTab: (id: string, to: To, state?: unknown, replace?: boolean) => void;
  go: (id: string, delta: number) => void;
  setTitle: (id: string, title: string) => void;
  updatePlayback: (id: string, playback: TabPlayback) => void;
  claimPip: (id: string) => void;
  dismissPip: (id: string) => void;
}

const initialPath = typeof window === "undefined" ? "/" : window.location.pathname + window.location.search + window.location.hash;
const initialTab = makeTab(initialPath, true);

export const useTabsStore = create<TabsState>((set, get) => ({
  tabs: [initialTab], activeId: initialTab.id, manualPipId: null,
  openTab: (path = "/", options = {}) => {
    if (!path.startsWith("/") || path.startsWith("//")) return;
    const tab = makeTab(path, !options.background, options.video);
    set((state) => ({ tabs: [...state.tabs, tab], ...(options.background ? {} : { activeId: tab.id }) }));
  },
  activateTab: (id) => set((state) => {
    const tab = state.tabs.find((item) => item.id === id);
    if (!tab || state.activeId === id) return state;
    return { activeId: id, tabs: state.tabs.map((item) => item.id === id ? { ...item, visited: true } : item) };
  }),
  closeTab: (id) => {
    get().tabs.find((tab) => tab.id === id)?.player.getState().clearQueue();
    set((state) => {
      const index = state.tabs.findIndex((tab) => tab.id === id);
      if (index < 0) return state;
      let tabs = state.tabs.filter((tab) => tab.id !== id);
      if (!tabs.length) tabs = [makeTab("/", true)];
      const next = tabs[Math.min(index, tabs.length - 1)]!;
      const activeId = state.activeId === id ? next.id : state.activeId;
      return { tabs: tabs.map((tab) => tab.id === activeId ? { ...tab, visited: true } : tab), activeId,
        manualPipId: state.manualPipId === id ? null : state.manualPipId };
    });
  },
  navigateTab: (id, to, state, replace = false) => set((current) => ({
    tabs: current.tabs.map((tab) => {
      if (tab.id !== id) return tab;
      const location = makeLocation(to, state);
      const entries = replace ? tab.entries.map((entry, index) => index === tab.index ? location : entry) : [...tab.entries.slice(0, tab.index + 1), location];
      return { ...tab, entries, index: replace ? tab.index : entries.length - 1, title: routeTitle(location) };
    }),
  })),
  go: (id, delta) => set((state) => ({ tabs: state.tabs.map((tab) => {
    if (tab.id !== id) return tab;
    const index = Math.max(0, Math.min(tab.entries.length - 1, tab.index + delta));
    return index === tab.index ? tab : { ...tab, index, title: routeTitle(tab.entries[index]!) };
  }) })),
  setTitle: (id, title) => set((state) => {
    const tab = state.tabs.find((item) => item.id === id);
    if (!title || !tab || tab.title === title) return state;
    return { tabs: state.tabs.map((item) => item.id === id ? { ...item, title } : item) };
  }),
  updatePlayback: (id, playback) => set((state) => {
    const tab = state.tabs.find((item) => item.id === id);
    if (!tab || Object.keys(playback).every((key) => playback[key as keyof TabPlayback] === tab.playback[key as keyof TabPlayback])) return state;
    return { tabs: state.tabs.map((item) => item.id === id ? { ...item, playback } : item),
      ...(state.manualPipId === id && playback.mode === "watch" ? { manualPipId: null } : {}) };
  }),
  claimPip: (id) => {
    set({ manualPipId: id });
    for (const tab of get().tabs) {
      if (tab.id !== id && tab.player.getState().videoPlayerMode === "pip") tab.player.getState().expandVideoPlayer();
    }
  },
  dismissPip: (id) => set((state) => state.manualPipId === id ? { manualPipId: null } : state),
}));

export function tabPath(tab: AppTab) {
  return createPath(tab.entries[tab.index]!);
}
