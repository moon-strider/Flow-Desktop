import { useState, useEffect } from "react";
import { useFeedActionsStore } from "./store/useFeedActionsStore";
import { useMusicActionsStore } from "./store/useMusicActionsStore";
import { useAppSettingsStore } from "./store/useAppSettingsStore";
import { useSettingsStore } from "./store/useSettingsStore";
import { useAlbumLibraryStore } from "./store/useAlbumLibraryStore";
import { useLikesStore } from "./store/useLikesStore";
import { useDownloadsLibraryStore } from "./store/useDownloadsLibraryStore";
import { useDownloadCollectionsLibraryStore } from "./store/useDownloadCollectionsLibraryStore";
import { useWatchLaterStore } from "./store/useWatchLaterStore";
import { getOnboardingStatus } from "./lib/api/recommendation";
import { useStartupHealth } from "./lib/useStartupHealth";

import { LayoutGroup } from "framer-motion";
import { ToastHost } from "./components/ui/ToastHost";
import { GlobalMusicAudio } from "./components/music/GlobalMusicAudio";
import { GlobalMusicDock } from "./components/music/GlobalMusicDock";
import { MusicOverlay } from "./components/music/MusicOverlay";
import { AddToAlbumModal } from "./components/music/AddToAlbumModal";
import { AddTracksToAlbumModal } from "./components/music/AddTracksToAlbumModal";
import { AddToPlaylistModal } from "./components/playlist/AddToPlaylistModal";
import { DeepFlowController } from "./components/deep-flow/DeepFlowController";
import { DeepLinkController } from "./components/handoff/DeepLinkController";
import { DownloadDialog } from "./components/downloads/DownloadDialog";
import { DownloadActivity } from "./components/downloads/DownloadActivity";
import { DonationPromptHost } from "./components/donations/DonationPrompt";
import { UpdateManager } from "./components/updater/UpdateManager";
import { ThemeController } from "./lib/useTheme";

import "./App.css";

import { TabWorkspace } from "./components/layout/TabWorkspace";
import { AppRoutes } from "./AppRoutes";
import { useTabsStore } from "./store/useTabsStore";

function App() {
  // Clear the Linux startup-crash sentinel as soon as the app mounts
  useStartupHealth();
  const [loadingOnboarding, setLoadingOnboarding] = useState(true);
  const pathname = useTabsStore((s) => { const tab = s.tabs.find((item) => item.id === s.activeId)!; return tab.entries[tab.index]!.pathname; });
  useEffect(() => {
    void useFeedActionsStore.getState().load();
    void useMusicActionsStore.getState().load();
    void useAppSettingsStore.getState().loadSettings();
    void useSettingsStore.getState().loadSettings();
    void useAlbumLibraryStore.getState().load();
    void useLikesStore.getState().load();
    void useDownloadsLibraryStore.getState().load();
    void useDownloadCollectionsLibraryStore.getState().load();
    void useWatchLaterStore.getState().load();
  }, []);

  useEffect(() => {
    let cancelled = false;
    void getOnboardingStatus().then((completed) => {
      if (!completed && !cancelled) {
        const tabs = useTabsStore.getState();
        tabs.navigateTab(tabs.activeId, "/onboarding", null, true);
      }
    }).catch((error) => console.warn("Failed to read onboarding status", error)).finally(() => { if (!cancelled) setLoadingOnboarding(false); });
    return () => { cancelled = true; };
  }, []);

  return <div className="relative flex h-screen flex-col overflow-hidden bg-background text-chrome-zinc-100 font-sans" style={{ "--flow-sidebar-top": "128px" } as React.CSSProperties}>
    <ThemeController />
    <TabWorkspace ready={!loadingOnboarding} renderRoutes={() => <AppRoutes />}>
      <GlobalMusicAudio />
      <LayoutGroup>
        <GlobalMusicDock />
        <MusicOverlay />
      </LayoutGroup>
      <AddToAlbumModal />
      <AddTracksToAlbumModal />
      <AddToPlaylistModal />
      <DeepFlowController />
      <DeepLinkController />
      <LayoutGroup id="downloads">
        <DownloadDialog />
        <DownloadActivity />
      </LayoutGroup>
      <DonationPromptHost enabled={pathname !== "/onboarding"} />
      <UpdateManager enabled={pathname !== "/onboarding"} />
      <ToastHost />
    </TabWorkspace>
  </div>;
}

export default App;
