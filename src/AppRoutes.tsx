import { useCallback } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
import { usePlayerStore } from "./store/usePlayerStore";
import { PageWrapper } from "./components/layout/PageWrapper";
import { WATCH_LATER_PLAYLIST_ID } from "./lib/playlistLibrary";
import type { VideoSummary } from "./types/video";
import Home from "./pages/Home";
import MusicHome from "./pages/music/MusicHome";
import ArtistPage from "./pages/music/ArtistPage";
import ArtistItemsPage from "./pages/music/ArtistItemsPage";
import MusicCollectionPage from "./pages/music/MusicCollectionPage";
import Search from "./pages/Search";
import ExploreCategories from "./pages/ExploreCategories";
import Subscriptions from "./pages/Subscriptions";
import History from "./pages/History";
import Downloads from "./pages/Downloads";
import Likes from "./pages/Likes";
import LibraryPage from "./pages/LibraryPage";
import AlbumsLibrary from "./pages/AlbumsLibrary";
import SavedShorts from "./pages/SavedShorts";
import Playlists from "./pages/Playlists";
import PlaylistDetailsPage from "./pages/PlaylistDetailsPage";
import Settings from "./pages/Settings";
import Sync from "./pages/Sync";
import Donations from "./pages/Donations";
import ImportData from "./pages/ImportData";
import ExtensionsPage from "./pages/ExtensionsPage";
import Channel from "./pages/Channel";
import Diagnostics from "./pages/Diagnostics";
import { Watch } from "./pages/Watch";
import { ShortsFeed } from "./components/shorts/ShortsFeed";
import Onboarding from "./pages/Onboarding";
import FlowNeuroPersona from "./pages/FlowNeuroPersona";

export function AppRoutes() {
  const navigate = useNavigate();
  /*
    Selected one action at a time, never destructured off a bare `usePlayerStore()`:
    a selectorless subscription re-renders this component — and with it every route,
    feed and card below it — on every `currentTime` write, which the player emits
    about four times a second for the whole of playback.
  */
  const setQueue = usePlayerStore((s) => s.setQueue);
  const addToQueue = usePlayerStore((s) => s.addToQueue);
  /*
    Stable identities matter here beyond the usual hygiene: these are handed down
    to every page and end up as the `onPlay`/`onAddToQueue` prop of every card, so
    a fresh closure per render defeats `React.memo(VideoCard)` for the whole feed.
  */
  const handlePlayVideo = useCallback((video: VideoSummary) => {
    setQueue([video], 0);
    navigate(`/watch/${video.id}`);
  }, [setQueue, navigate]);

  const handleAddToQueue = useCallback((video: VideoSummary) => {
    addToQueue(video);
  }, [addToQueue]);

  return (
      <Routes>
        <Route path="/onboarding" element={<Onboarding />} />

        <Route path="/" element={<PageWrapper />}>
          <Route index element={
            <Home onPlay={handlePlayVideo} onAddToQueue={handleAddToQueue} />
          } />
          <Route path="feed" element={
            <FlowNeuroPersona />
          } />
          <Route path="music" element={
            <MusicHome />
          } />
          <Route path="music/artist/:artistId" element={
            <ArtistPage />
          } />
          <Route path="music/artist/:artistId/items" element={
            <ArtistItemsPage />
          } />
          <Route path="music/album/:id" element={
            <MusicCollectionPage kind="album" />
          } />
          <Route path="music/playlist/:id" element={
            <MusicCollectionPage kind="playlist" />
          } />
          <Route path="search" element={
            <Search onPlay={handlePlayVideo} onAddToQueue={handleAddToQueue} />
          } />
          <Route path="explore" element={
            <ExploreCategories onPlay={handlePlayVideo} onAddToQueue={handleAddToQueue} />
          } />
          <Route path="shorts" element={
            <ShortsFeed />
          } />
          <Route path="shorts/:videoId" element={
            <ShortsFeed />
          } />
          <Route path="subscriptions" element={
            <Subscriptions onPlay={handlePlayVideo} onAddToQueue={handleAddToQueue} />
          } />
          <Route path="channel/:channelId" element={
            <Channel onPlay={handlePlayVideo} onAddToQueue={handleAddToQueue} />
          } />
          <Route path="playlists" element={
            <Playlists onPlay={handlePlayVideo} />
          } />
          <Route path="playlist/:playlistId" element={
            <PlaylistDetailsPage
              onAddToQueue={handleAddToQueue}
            />
          } />
          <Route path="watch-later" element={
            <PlaylistDetailsPage
              playlistIdOverride={WATCH_LATER_PLAYLIST_ID}
              onAddToQueue={handleAddToQueue}
            />
          } />
          <Route path="library" element={
            <LibraryPage onPlay={handlePlayVideo} onAddToQueue={handleAddToQueue} />
          } />
          <Route path="albums" element={
            <AlbumsLibrary />
          } />
          <Route path="saved-shorts" element={
            <SavedShorts />
          } />
          <Route path="history" element={
            <History onPlay={handlePlayVideo} />
          } />
          <Route path="downloads" element={
            <Downloads onPlay={handlePlayVideo} />
          } />
          <Route path="liked" element={
            <Likes onPlay={handlePlayVideo} />
          } />
          <Route path="settings" element={
            <Settings />
          } />
          <Route path="sync" element={
            <Sync />
          } />
          <Route path="support" element={
            <Donations />
          } />
          <Route path="settings/import" element={
            <ImportData />
          } />
          <Route path="settings/diagnostics" element={
            <Diagnostics />
          } />
          <Route path="sponsorblock" element={
            <ExtensionsPage />
          } />
          <Route path="watch/:videoId" element={
            <Watch />
          } />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
  );
}
