import { useEffect, useMemo, useRef, type ReactNode } from "react";
import { Router, createPath, type Navigator } from "react-router-dom";
import { Plus, X, Volume2 } from "lucide-react";
import { useTabsStore, tabPath, type AppTab } from "../../store/useTabsStore";
import { PlayerStoreContext } from "../../store/usePlayerStore";
import { TabContext, type TabContextValue } from "../../lib/tabContext";
import { TitleBar } from "./TitleBar";
import { GlobalVideoPlayer } from "../watch/GlobalVideoPlayer";
import { PipWindowController } from "../pip/PipWindowController";
import { getString } from "../../lib/i18n/index";

function TabEnvironment({ tab, context, children }: { tab: AppTab; context: TabContextValue; children: ReactNode }) {
  const navigator = useMemo<Navigator>(() => ({
    createHref: (to) => typeof to === "string" ? to : createPath(to),
    go: (delta) => useTabsStore.getState().go(tab.id, delta),
    push: (to, state) => useTabsStore.getState().navigateTab(tab.id, to, state),
    replace: (to, state) => useTabsStore.getState().navigateTab(tab.id, to, state, true),
  }), [tab.id]);
  return <PlayerStoreContext.Provider value={tab.player}>
    <TabContext.Provider value={context}>
      <Router location={tab.entries[tab.index]!} navigator={navigator}>{children}</Router>
    </TabContext.Provider>
  </PlayerStoreContext.Provider>;
}

function TabSession({ tab, active, pipId, renderRoutes }: {
  tab: AppTab; active: boolean; pipId: string | null; renderRoutes: () => ReactNode;
}) {
  const rootRef = useRef<HTMLDivElement>(null);
  const locationKey = tab.entries[tab.index]!.key;
  useEffect(() => {
    const publish = () => {
      const state = tab.player.getState();
      useTabsStore.getState().updatePlayback(tab.id, { videoId: state.currentVideo?.id ?? null, playing: state.isPlaying,
        mode: state.videoPlayerMode, intent: state.videoPipIntent, fullscreen: state.isVideoFullscreen });
      const current = useTabsStore.getState().tabs.find((item) => item.id === tab.id);
      if (state.currentVideo?.title && current?.entries[current.index]?.pathname === `/watch/${state.currentVideo.id}`) {
        useTabsStore.getState().setTitle(tab.id, state.currentVideo.title);
      }
    };
    publish();
    return tab.player.subscribe(publish);
  }, [tab.id, tab.player, locationKey]);
  const context = useMemo<TabContextValue>(() => ({ id: tab.id, active, rootRef,
    ownsPip: pipId === tab.id }), [tab.id, active, pipId]);
  return <TabEnvironment tab={tab} context={context}>
    <div ref={rootRef} id={`panel-${tab.id}`} role="tabpanel" aria-labelledby={`tab-${tab.id}`} hidden={!active} inert={!active}
      className="h-full min-h-0" data-flow-tab={tab.id}>
      {renderRoutes()}
    </div>
    <GlobalVideoPlayer />
    <PipWindowController />
  </TabEnvironment>;
}

export function TabWorkspace({ ready, renderRoutes, children }: { ready: boolean; renderRoutes: () => ReactNode; children: ReactNode }) {
  const tabs = useTabsStore((state) => state.tabs);
  const activeId = useTabsStore((state) => state.activeId);
  const manualPipId = useTabsStore((state) => state.manualPipId);
  const activate = useTabsStore((state) => state.activateTab);
  const close = useTabsStore((state) => state.closeTab);
  const open = useTabsStore((state) => state.openTab);
  const activeTab = tabs.find((tab) => tab.id === activeId)!;
  const popout = tabs.find((tab) => tab.playback.mode === "window");
  const pipId = popout ? null : manualPipId;
  const stripRef = useRef<HTMLDivElement>(null);
  const selectedPath = tabPath(activeTab);
  const sharedContext = useMemo<TabContextValue>(() => ({ id: activeId, active: true,
    ownsPip: false, rootRef: null }), [activeId]);

  useEffect(() => {
    window.history.replaceState({ flowTab: activeId }, "", selectedPath);
  }, [activeId, selectedPath]);

  useEffect(() => {
    stripRef.current?.querySelector<HTMLElement>(`[id="tab-${activeId}"]`)?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [activeId, tabs.length]);

  useEffect(() => {
    const keydown = (event: KeyboardEvent) => {
      if (!(event.ctrlKey || event.metaKey) || event.altKey) return;
      const state = useTabsStore.getState();
      if (state.tabs.some((tab) => tab.player.getState().isVideoFullscreen)) return;
      if (event.key.toLowerCase() === "t" && !event.shiftKey) {
        event.preventDefault(); state.openTab();
      } else if (event.key.toLowerCase() === "w") {
        event.preventDefault(); state.closeTab(state.activeId);
      } else if (event.key === "Tab") {
        event.preventDefault();
        const index = state.tabs.findIndex((tab) => tab.id === state.activeId);
        state.activateTab(state.tabs[(index + (event.shiftKey ? -1 : 1) + state.tabs.length) % state.tabs.length]!.id);
      }
    };
    window.addEventListener("keydown", keydown);
    return () => window.removeEventListener("keydown", keydown);
  }, []);

  return <>
    <TabEnvironment tab={activeTab} context={sharedContext}><TitleBar /></TabEnvironment>
    <div className="relative z-[60] flex h-10 shrink-0 items-stretch border-b border-outline-variant bg-background">
      <div ref={stripRef} role="tablist" aria-label={getString("tabs_label")} className="flex min-w-0 flex-1 items-stretch overflow-x-auto scrollbar-none">
        {tabs.map((tab) => <div key={tab.id} className={`group flex min-w-32 max-w-60 flex-1 items-center border-r border-outline-variant ${tab.id === activeId ? "bg-surface-container text-on-surface" : "text-on-surface-variant hover:bg-surface-container/60"}`}>
          <button id={`tab-${tab.id}`} role="tab" aria-selected={tab.id === activeId} aria-controls={`panel-${tab.id}`}
            tabIndex={tab.id === activeId ? 0 : -1} title={tab.title} className="flex h-full min-w-0 flex-1 items-center gap-2 px-3 text-left text-xs focus-visible:outline-2 focus-visible:outline-primary"
            onClick={() => activate(tab.id)} onMouseDown={(event) => { if (event.button === 1) event.preventDefault(); }}
            onAuxClick={(event) => { if (event.button === 1) { event.preventDefault(); close(tab.id); } }}
            onKeyDown={(event) => {
              const index = tabs.findIndex((item) => item.id === tab.id);
              const next = event.key === "ArrowRight" ? (index + 1) % tabs.length : event.key === "ArrowLeft" ? (index - 1 + tabs.length) % tabs.length : event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : null;
              if (next !== null) { event.preventDefault(); activate(tabs[next]!.id); document.getElementById(`tab-${tabs[next]!.id}`)?.focus(); }
              if (event.key === "Delete") { event.preventDefault(); close(tab.id); }
            }}>
            {tab.playback.playing && tab.playback.videoId && <Volume2 size={14} className="shrink-0 text-primary" aria-label={getString("tabs_playing")} />}
            <span className="truncate">{tab.title}</span>
          </button>
          <button type="button" aria-label={getString("tabs_close_named", tab.title)} onClick={() => close(tab.id)} className="mr-1 grid h-6 w-6 shrink-0 place-items-center rounded-md hover:bg-on-surface/10"><X size={14} /></button>
        </div>)}
      </div>
      <button type="button" aria-label={getString("tabs_new")} title={getString("tabs_new")} onClick={() => open()} className="grid w-10 shrink-0 place-items-center text-on-surface-variant hover:bg-surface-container"><Plus size={17} /></button>
    </div>
    <div className="relative min-h-0 flex-1">
      {ready ? tabs.filter((tab) => tab.visited).map((tab) => <TabSession key={tab.id} tab={tab} active={tab.id === activeId} pipId={pipId} renderRoutes={renderRoutes} />)
        : <div className="flex h-full items-center justify-center"><div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" /></div>}
    </div>
    <TabEnvironment tab={activeTab} context={sharedContext}>{children}</TabEnvironment>
  </>;
}
