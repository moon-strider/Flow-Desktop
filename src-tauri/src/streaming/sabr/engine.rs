//! Async SABR engine: the network loop that POSTs `VideoPlaybackAbrRequest`s,
//! parses the UMP response, accumulates media into a bounded buffer, and serves
//! init/segment bytes to the proxy on demand.
//!
//! The engine is demand-driven: it fetches ahead until a configurable buffer
//! ceiling, then pauses until a consumer (the proxy) asks for more. Consumers
//! call [`SabrEngine::get_init`] / [`SabrEngine::get_segment`], which wait (up to
//! a timeout) for the bytes to arrive while nudging the loop forward.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde::Serialize;
use tokio::sync::{Mutex, Notify, watch};
use tracing::{debug, info, warn};

use super::messages::FormatId;
use super::messages::{
    FormatInitializationMetadata, MediaHeader, NextRequestPolicy, SabrContextSendingPolicy,
    SabrContextUpdate, SabrError as SabrErrorMsg, SabrRedirect, StreamProtectionStatus,
};
use super::selector::{SabrAudioTrack, SelectedFormats, derive_audio_tracks};
use super::session::{RequestMode, SabrState};
use super::ump::{self, UmpParser};
use super::{SabrError, SabrResult, SabrSessionDescriptor, SabrTrack};

const SABR_STREAM_IDLE: Duration = Duration::from_millis(2500);

// How many times we re-mint the PO token and retry when the server escalates to
// ATTESTATION_REQUIRED before giving up on the session.
const MAX_ATTESTATION_REFRESHES: u32 = 3;

#[derive(Debug, Clone)]
pub struct SabrEngineConfig {
    pub mode: RequestMode,
    // Max bytes held per session before the loop pauses for demand.
    pub max_buffer_bytes: usize,
    pub prefetch_segments: i32,
    // How long a consumer waits for a specific init/segment before giving up.
    pub segment_wait: Duration,
    // Max consecutive retryable errors before failing the session.
    pub max_retries: u32,
}

impl Default for SabrEngineConfig {
    fn default() -> Self {
        Self {
            mode: RequestMode::AudioVideo,
            max_buffer_bytes: 256 * 1024 * 1024,
            prefetch_segments: 4,
            segment_wait: Duration::from_secs(10),
            max_retries: 5,
        }
    }
}

// Per-part header info linking MEDIA_HEADER -> MEDIA -> MEDIA_END by header_id.
#[derive(Clone)]
struct PendingHeader {
    is_audio: bool,
    is_init: bool,
    sequence: i32,
    start_ms: i64,
    duration_ms: i64,
}

#[derive(Default)]
struct TrackBuffer {
    init: Option<Vec<u8>>,
    segments: BTreeMap<i32, Vec<u8>>,
    max_seq: i32,
    end_seq: i64,
    total_ms: i64,
    initialized: bool,
    demand_seq: i32,
}

impl TrackBuffer {
    fn new() -> Self {
        Self {
            max_seq: -1,
            demand_seq: 1,
            ..Default::default()
        }
    }
    fn done(&self) -> bool {
        (self.end_seq > 0 && i64::from(self.max_seq) >= self.end_seq)
            || (self.total_ms > 0 && self.init.is_some() && self.segments_cover_total())
    }
    fn segments_cover_total(&self) -> bool {
        // crude: rely on max_seq vs end_seq; total_ms handled by state
        self.end_seq > 0 && i64::from(self.max_seq) >= self.end_seq
    }
}

#[derive(Default)]
struct Store {
    audio: TrackBuffer,
    video: TrackBuffer,
    bytes_used: usize,
    done: bool,
    last_error: Option<SabrError>,
    request_count: u64,
    redirect_count: u32,
    last_protection_status: i32,
}

impl Store {
    fn track(&self, track: SabrTrack) -> &TrackBuffer {
        match track {
            SabrTrack::Audio => &self.audio,
            SabrTrack::Video => &self.video,
        }
    }
    fn track_mut(&mut self, track: SabrTrack) -> &mut TrackBuffer {
        match track {
            SabrTrack::Audio => &mut self.audio,
            SabrTrack::Video => &mut self.video,
        }
    }
}

// Snapshot of engine state for `get_sabr_debug_state`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SabrDebugState {
    pub session_id: String,
    pub video_id: String,
    pub effective_url: String,
    pub request_count: u64,
    pub redirect_count: u32,
    pub bytes_used: usize,
    pub done: bool,
    pub last_error: Option<String>,
    pub audio_initialized: bool,
    pub video_initialized: bool,
    pub audio_segments: usize,
    pub video_segments: usize,
    pub audio_max_seq: i32,
    pub video_max_seq: i32,
    pub last_protection_status: i32,
}

// Timing/codec snapshot used to build a local DASH manifest.
#[derive(Debug, Clone)]
pub struct SabrTiming {
    pub duration_ms: u64,
    pub audio_segment_count: i64,
    pub video_segment_count: i64,
    pub audio_segment_duration_ms: u64,
    pub video_segment_duration_ms: u64,
}

pub struct SabrEngine {
    pub session_id: String,
    pub descriptor: SabrSessionDescriptor,
    selected: SelectedFormats,
    audio_tracks: Vec<SabrAudioTrack>,
    active_audio_key: Mutex<String>,
    active_audio_itag: AtomicI32,
    config: SabrEngineConfig,
    client: reqwest::Client,
    state: Mutex<SabrState>,
    store: Mutex<Store>,
    notify_data: Notify,
    notify_demand: Notify,
    format_ready: Notify,
    cancelled: AtomicBool,
    finished: AtomicBool,
    cancel_signal: watch::Sender<bool>,
    started: AtomicBool,
    request_counter: AtomicU64,
    restart_request: AtomicBool,
    pending_segments: std::sync::Mutex<BTreeMap<(bool, i32), PendingRequest>>,
    demand_counter: AtomicU64,
}

struct PendingRequest {
    count: usize,
    order: u64,
}

struct SegmentDemand<'a> {
    engine: &'a SabrEngine,
    key: (bool, i32),
}

impl Drop for SegmentDemand<'_> {
    fn drop(&mut self) {
        let mut pending = self.engine.pending_segments.lock().unwrap();
        if let Some(request) = pending.get_mut(&self.key) {
            request.count -= 1;
            if request.count == 0 {
                pending.remove(&self.key);
            }
        }
        self.engine.notify_demand.notify_one();
    }
}

impl SabrEngine {
    pub fn new(
        session_id: String,
        descriptor: SabrSessionDescriptor,
        selected: SelectedFormats,
        config: SabrEngineConfig,
    ) -> Self {
        let po_token = descriptor
            .po_token
            .as_ref()
            .and_then(|t| decode_b64_loose(t));
        let mut state = SabrState::new(
            descriptor.server_abr_streaming_url.clone(),
            &selected,
            descriptor.ustreamer_config.clone(),
            po_token,
            descriptor.client_profile.clone(),
            config.mode,
        );

        // Per-language audio tracks; the default (or first) is initially active.
        let audio_tracks = derive_audio_tracks(&descriptor.formats);
        let initial = audio_tracks
            .iter()
            .find(|t| t.is_default)
            .or_else(|| audio_tracks.first());
        let (active_key, active_itag) = match initial {
            Some(t) => {
                state.set_audio_format(
                    FormatId::new(
                        t.format.itag,
                        t.format.last_modified,
                        t.format.xtags.clone(),
                    ),
                    t.format.mime_type.clone(),
                );
                (t.key.clone(), t.format.itag)
            }
            None => (String::new(), selected.audio.itag),
        };

        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_default();

        Self {
            session_id,
            descriptor,
            selected,
            audio_tracks,
            active_audio_key: Mutex::new(active_key),
            active_audio_itag: AtomicI32::new(active_itag),
            config,
            client,
            state: Mutex::new(state),
            store: Mutex::new(Store {
                audio: TrackBuffer::new(),
                video: TrackBuffer::new(),
                ..Default::default()
            }),
            notify_data: Notify::new(),
            notify_demand: Notify::new(),
            format_ready: Notify::new(),
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            cancel_signal: watch::channel(false).0,
            started: AtomicBool::new(false),
            request_counter: AtomicU64::new(0),
            restart_request: AtomicBool::new(false),
            pending_segments: std::sync::Mutex::new(BTreeMap::new()),
            demand_counter: AtomicU64::new(0),
        }
    }

    pub fn selected(&self) -> &SelectedFormats {
        &self.selected
    }

    // Start the background fetch loop (idempotent).
    pub fn spawn(self: Arc<Self>) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        let mut cancelled = self.cancel_signal.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = async { let _ = cancelled.wait_for(|value| *value).await; } => {}
                _ = engine.clone().run() => {}
            }
            if engine.is_cancelled() {
                let mut store = engine.store.lock().await;
                store.audio = TrackBuffer::new();
                store.video = TrackBuffer::new();
                store.bytes_used = 0;
                store.done = true;
                store.last_error = Some(SabrError::Cancelled);
                drop(store);
                engine.notify_data.notify_waiters();
                engine.format_ready.notify_waiters();
            }
            engine.finished.store(true, Ordering::SeqCst);
        });
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.cancel_signal.send_replace(true);
        self.notify_demand.notify_waiters();
        self.notify_data.notify_waiters();
        self.format_ready.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst) || self.is_cancelled()
    }

    async fn needs_data(&self) -> bool {
        let store = self.store.lock().await;
        if self.pending_target(&store).is_some() {
            return true;
        }
        let needs_track = |track: &TrackBuffer| {
            let target = track
                .demand_seq
                .saturating_add(self.config.prefetch_segments);
            let target = if track.end_seq > 0 {
                i64::from(target).min(track.end_seq)
            } else {
                i64::from(target)
            };
            track.init.is_none() || !track.initialized || i64::from(track.max_seq) < target
        };
        needs_track(&store.audio)
            || (self.config.mode == RequestMode::AudioVideo && needs_track(&store.video))
    }

    fn pending_target(&self, store: &Store) -> Option<(SabrTrack, i32)> {
        self.pending_segments
            .lock()
            .unwrap()
            .iter()
            .filter(|((audio, sequence), _)| {
                let track = if *audio { &store.audio } else { &store.video };
                !track.segments.contains_key(sequence)
                    && (track.end_seq == 0 || i64::from(*sequence) <= track.end_seq)
            })
            .min_by_key(|(_, request)| request.order)
            .map(|((audio, sequence), _)| {
                (
                    if *audio {
                        SabrTrack::Audio
                    } else {
                        SabrTrack::Video
                    },
                    *sequence,
                )
            })
    }

    async fn needs_retarget(&self) -> bool {
        let store = self.store.lock().await;
        self.pending_target(&store)
            .is_some_and(|(track, sequence)| {
                let buffer = store.track(track);
                sequence <= buffer.max_seq
                    || sequence
                        > buffer
                            .max_seq
                            .saturating_add(self.config.prefetch_segments.max(1))
            })
    }

    // --- background loop ------------------------------------------------------

    async fn run(self: Arc<Self>) {
        info!(session = %self.session_id, video = %self.descriptor.video_id, "sabr_session_created");
        let mut consecutive_errors: u32 = 0;
        let mut attestation_refreshes: u32 = 0;

        loop {
            if self.is_cancelled() {
                break;
            }

            self.prepare_next_request().await;

            // Backpressure: if we're holding a lot of media, wait for demand.
            if !self.needs_data().await {
                self.notify_demand.notified().await;
                continue;
            }

            match self.request_cycle().await {
                Ok(should_continue) => {
                    consecutive_errors = 0;
                    if !should_continue {
                        self.notify_demand.notified().await;
                        continue;
                    }
                }
                // The server grants a short grace window then demands fresh
                // attestation. Re-mint the PO token and retry rather than killing
                // the session (mirrors the reference client's refresh-on-required).
                Err(SabrError::AttestationRequired)
                    if attestation_refreshes < MAX_ATTESTATION_REFRESHES =>
                {
                    attestation_refreshes += 1;
                    let refreshed = self.refresh_po_token().await;
                    warn!(
                        session = %self.session_id,
                        attempt = attestation_refreshes,
                        refreshed,
                        "sabr_attestation_refresh"
                    );
                    if !refreshed {
                        let mut store = self.store.lock().await;
                        store.last_error = Some(SabrError::AttestationRequired);
                        self.notify_data.notify_waiters();
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
                Err(err) => {
                    warn!(session = %self.session_id, code = err.code(), error = %err, "sabr_error");
                    if !err.is_retryable() || consecutive_errors >= self.config.max_retries {
                        let mut store = self.store.lock().await;
                        store.last_error = Some(err);
                        self.notify_data.notify_waiters();
                        break;
                    }
                    consecutive_errors += 1;
                    let delay = Duration::from_millis(500 * 2u64.pow(consecutive_errors - 1));
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }

            let backoff = { self.state.lock().await.backoff_ms };
            if backoff > 0 {
                debug!(session = %self.session_id, backoff_ms = backoff, "sabr_backoff");
                tokio::time::sleep(Duration::from_millis(backoff as u64)).await;
            }
        }

        // Mark done so waiters stop blocking.
        {
            let mut store = self.store.lock().await;
            store.done = true;
        }
        self.notify_data.notify_waiters();
        info!(session = %self.session_id, "sabr_session_finished");
    }

    async fn request_cycle(&self) -> SabrResult<bool> {
        let rn = self.request_counter.fetch_add(1, Ordering::Relaxed);
        let (url, body, visitor) = {
            let st = self.state.lock().await;
            let sep = if st.effective_url.contains('?') {
                '&'
            } else {
                '?'
            };
            let url = format!("{}{}rn={}", st.effective_url, sep, rn);
            (
                url,
                st.build_request().encode(),
                self.descriptor.visitor_data.clone(),
            )
        };

        debug!(session = %self.session_id, rn, bytes = body.len(), "sabr_request_started");

        let mut req = self
            .client
            .post(&url)
            .header("User-Agent", &self.descriptor.client_profile.user_agent)
            .header("Content-Type", "application/x-protobuf")
            .header("Accept", "*/*")
            .header("Accept-Encoding", "identity")
            .header("Origin", "https://www.youtube.com")
            .header("Referer", "https://www.youtube.com/")
            .body(body);
        if let Some(vd) = visitor.as_deref() {
            if !vd.is_empty() {
                req = req.header("X-Goog-Visitor-Id", vd);
            }
        }

        let resp = tokio::time::timeout(Duration::from_secs(20), req.send())
            .await
            .map_err(|_| SabrError::Network("SABR response headers timed out".to_string()))?
            .map_err(|e| SabrError::Network(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(SabrError::HttpStatus(status.as_u16()));
        }

        {
            let mut store = self.store.lock().await;
            store.request_count += 1;
        }

        let mut parser = UmpParser::new();
        let mut headers: std::collections::HashMap<u32, PendingHeader> =
            std::collections::HashMap::new();
        let mut accums: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        let mut stream = resp.bytes_stream();

        let mut chunk_count = 0u64;
        let mut byte_count = 0u64;
        loop {
            if !self.needs_data().await {
                debug!(session = %self.session_id, rn, byte_count, "sabr_buffer_full");
                break;
            }
            // An audio-language switch ends this cycle so the next one re-requests
            // with the newly-selected track.
            if self.restart_request.swap(false, Ordering::SeqCst) || self.needs_retarget().await {
                debug!(session = %self.session_id, rn, "sabr_restart_for_audio_switch");
                break;
            }
            let next = tokio::select! {
                next = tokio::time::timeout(SABR_STREAM_IDLE, stream.next()) => next,
                _ = self.notify_demand.notified() => {
                    if self.restart_request.swap(false, Ordering::SeqCst) || self.needs_retarget().await {
                        break;
                    }
                    continue;
                }
            };
            let chunk = match next {
                Ok(Some(chunk)) => chunk,
                Ok(None) => {
                    debug!(session = %self.session_id, rn, chunk_count, byte_count, "sabr_stream_eof");
                    break;
                }
                Err(_) => {
                    debug!(session = %self.session_id, rn, chunk_count, byte_count, "sabr_idle_break");
                    break;
                }
            };
            if self.is_cancelled() {
                return Ok(false);
            }
            let chunk = chunk.map_err(|_| SabrError::RemoteReset)?;
            chunk_count += 1;
            byte_count += chunk.len() as u64;
            for payload in chunk.chunks(self.config.max_buffer_bytes.clamp(1, 64 * 1024)) {
                parser.push(payload);
                while let Some(part) = parser.next_part() {
                    if let Some(action) = self
                        .dispatch_part(part.part_type, &part.data, &mut headers, &mut accums)
                        .await?
                    {
                        match action {
                            CycleAction::Reload => return Err(SabrError::ReloadRequired),
                            CycleAction::Stop => return Ok(false),
                        }
                    }
                    if !self.needs_data().await {
                        return Ok(true);
                    }
                    if part.part_type == ump::MEDIA_END && self.needs_retarget().await {
                        return Ok(true);
                    }
                }
                if parser.buffered_len() > self.config.max_buffer_bytes {
                    return Err(SabrError::UmpDecode(
                        "SABR frame exceeds buffer budget".to_string(),
                    ));
                }
            }
        }

        let done = { self.state.lock().await.both_complete() };
        debug!(
            session = %self.session_id,
            rn, byte_count, done,
            "sabr_request_stream_ended"
        );
        if done {
            return Ok(false);
        }
        Ok(true)
    }

    async fn dispatch_part(
        &self,
        part_type: u32,
        data: &[u8],
        headers: &mut std::collections::HashMap<u32, PendingHeader>,
        accums: &mut std::collections::HashMap<u32, Vec<u8>>,
    ) -> SabrResult<Option<CycleAction>> {
        match part_type {
            ump::MEDIA_HEADER => {
                let mh = MediaHeader::decode(data);
                let (audio_itag, video_itag) = self.track_itags();
                let is_audio = mh.itag == audio_itag;
                let is_video = mh.itag == video_itag;
                if is_audio || is_video {
                    headers.insert(
                        mh.header_id,
                        PendingHeader {
                            is_audio,
                            is_init: mh.is_init_seg,
                            sequence: mh.sequence_number,
                            start_ms: mh.effective_start_ms(),
                            duration_ms: mh.effective_duration_ms(),
                        },
                    );
                    accums.entry(mh.header_id).or_default();
                }
            }
            ump::MEDIA => {
                if let Some((header_id, consumed)) = ump::read_varint(data) {
                    let payload = &data[consumed..];
                    if headers.contains_key(&header_id) {
                        let accumulated = accums.entry(header_id).or_default();
                        if accumulated.len().saturating_add(payload.len())
                            > self.config.max_buffer_bytes
                        {
                            return Err(SabrError::UmpDecode(
                                "SABR segment exceeds buffer budget".to_string(),
                            ));
                        }
                        accumulated.extend_from_slice(payload);
                    }
                }
            }
            ump::MEDIA_END => {
                if let Some((header_id, _)) = ump::read_varint(data) {
                    if let (Some(info), Some(bytes)) =
                        (headers.remove(&header_id), accums.remove(&header_id))
                    {
                        self.finalize_segment(info, bytes).await;
                    }
                }
            }
            ump::FORMAT_INITIALIZATION_METADATA => {
                let meta = FormatInitializationMetadata::decode(data);
                self.handle_format_init(&meta).await;
            }
            ump::NEXT_REQUEST_POLICY => {
                let policy = NextRequestPolicy::decode(data);
                let mut st = self.state.lock().await;
                st.apply_next_request_policy(policy.backoff_time_ms, policy.playback_cookie);
            }
            ump::SABR_REDIRECT => {
                let redirect = SabrRedirect::decode(data);
                if let Some(url) = redirect.url {
                    let mut store = self.store.lock().await;
                    store.redirect_count += 1;
                    if store.redirect_count > 8 {
                        return Err(SabrError::RedirectLoop);
                    }
                    drop(store);
                    info!(session = %self.session_id, "sabr_redirect");
                    self.state.lock().await.apply_redirect(url);
                }
            }
            ump::SABR_ERROR => {
                let err = SabrErrorMsg::decode(data);
                return Err(SabrError::ServerError {
                    kind: err.r#type.unwrap_or_else(|| "unknown".into()),
                    code: err.code,
                });
            }
            ump::SABR_CONTEXT_UPDATE => {
                let update = SabrContextUpdate::decode(data);
                let mut st = self.state.lock().await;
                st.apply_context_update(
                    update.r#type,
                    update.value,
                    update.send_by_default,
                    update.write_policy,
                );
            }
            ump::SABR_CONTEXT_SENDING_POLICY => {
                let policy = SabrContextSendingPolicy::decode(data);
                let mut st = self.state.lock().await;
                st.apply_context_sending_policy(
                    &policy.start_policy,
                    &policy.stop_policy,
                    &policy.discard_policy,
                );
            }
            ump::STREAM_PROTECTION_STATUS => {
                let status = StreamProtectionStatus::decode(data);
                {
                    let mut store = self.store.lock().await;
                    store.last_protection_status = status.status;
                }
                if status.status == StreamProtectionStatus::ATTESTATION_REQUIRED {
                    return Err(SabrError::AttestationRequired);
                }
            }
            ump::RELOAD_PLAYER_RESPONSE => {
                return Ok(Some(CycleAction::Reload));
            }
            ump::END_OF_TRACK => {
                debug!(session = %self.session_id, "sabr end_of_track");
            }
            other => {
                debug!(session = %self.session_id, part = ump::part_name(other), "sabr_ump_part_unhandled");
            }
        }
        Ok(None)
    }

    async fn handle_format_init(&self, meta: &FormatInitializationMetadata) {
        let (audio_itag, _video_itag) = self.track_itags();
        let is_audio = meta.itag() == audio_itag;
        {
            let mut st = self.state.lock().await;
            st.apply_format_init(is_audio, meta.end_time_ms, meta.end_segment_number);
        }
        {
            let mut store = self.store.lock().await;
            let track = store.track_mut(if is_audio {
                SabrTrack::Audio
            } else {
                SabrTrack::Video
            });
            track.initialized = true;
            if meta.end_segment_number > 0 {
                track.end_seq = meta.end_segment_number;
            }
            if meta.end_time_ms > 0 {
                track.total_ms = meta.end_time_ms;
            }
        }
        debug!(session = %self.session_id, itag = meta.itag(), is_audio, "sabr_format_init");
        self.format_ready.notify_waiters();
    }

    async fn finalize_segment(&self, info: PendingHeader, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        let track = if info.is_audio {
            SabrTrack::Audio
        } else {
            SabrTrack::Video
        };

        {
            let mut st = self.state.lock().await;
            if !info.is_init {
                st.record_segment(
                    info.is_audio,
                    info.sequence,
                    info.start_ms,
                    info.duration_ms,
                );
            }
        }

        {
            let mut store = self.store.lock().await;
            let added = bytes.len();
            let replaced = {
                let tb = store.track_mut(track);
                if info.is_init {
                    tb.init.replace(bytes)
                } else {
                    if info.sequence > tb.max_seq {
                        tb.max_seq = info.sequence;
                    }
                    tb.segments.insert(info.sequence, bytes)
                }
            };
            store.bytes_used = store
                .bytes_used
                .saturating_sub(replaced.as_ref().map_or(0, Vec::len))
                .saturating_add(added);
            self.evict_if_needed(&mut store);
        }

        debug!(session = %self.session_id, track = ?track, seq = info.sequence, init = info.is_init, "sabr_segment_ready");
        self.notify_data.notify_waiters();
    }

    fn evict_if_needed(&self, store: &mut Store) {
        let pending = self.pending_segments.lock().unwrap();
        for kind in [SabrTrack::Audio, SabrTrack::Video] {
            let buffer = store.track_mut(kind);
            let oldest = buffer.demand_seq.saturating_sub(12);
            let mut freed = 0;
            buffer.segments.retain(|sequence, bytes| {
                if *sequence >= oldest
                    || pending.contains_key(&(kind == SabrTrack::Audio, *sequence))
                {
                    true
                } else {
                    freed += bytes.len();
                    false
                }
            });
            store.bytes_used = store.bytes_used.saturating_sub(freed);
        }
        drop(pending);
        while store.bytes_used > self.config.max_buffer_bytes {
            let pending = self.pending_segments.lock().unwrap();
            let oldest = [SabrTrack::Audio, SabrTrack::Video]
                .into_iter()
                .flat_map(|kind| {
                    store
                        .track(kind)
                        .segments
                        .keys()
                        .map(move |sequence| (kind, *sequence))
                })
                .filter(|(kind, sequence)| {
                    !pending.contains_key(&(*kind == SabrTrack::Audio, *sequence))
                })
                .min_by_key(|(_, sequence)| *sequence);
            let Some((track, sequence)) = oldest else {
                break;
            };
            if let Some(bytes) = store.track_mut(track).segments.remove(&sequence) {
                store.bytes_used = store.bytes_used.saturating_sub(bytes.len());
            }
        }
    }

    fn track_itags(&self) -> (i32, i32) {
        (
            self.active_audio_itag.load(Ordering::Relaxed),
            self.selected.video.itag,
        )
    }

    // The selectable audio languages for this session (for manifest generation).
    pub fn audio_tracks(&self) -> &[SabrAudioTrack] {
        &self.audio_tracks
    }

    // Re-mint the PO token (bound to the session visitor data) and install it for
    // subsequent requests. Returns whether a token was obtained.
    async fn refresh_po_token(&self) -> bool {
        let binding = self
            .descriptor
            .visitor_data
            .clone()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.descriptor.video_id.clone());
        match crate::api::innertube::core::botguard::generate_po_token(&binding).await {
            Some(token) => match decode_b64_loose(&token) {
                Some(bytes) => {
                    self.state.lock().await.po_token = Some(bytes);
                    true
                }
                None => false,
            },
            None => false,
        }
    }

    // Switch the active audio language. Resets audio progress + buffer so the new
    // track re-initializes; video is untouched. No-op if `key` is already active
    // or unknown. Returns whether `key` is a known track.
    pub async fn set_active_audio(&self, key: &str) -> bool {
        let Some(track) = self.audio_tracks.iter().find(|t| t.key == key).cloned() else {
            return false;
        };
        {
            let mut cur = self.active_audio_key.lock().await;
            if *cur == key {
                return true;
            }
            *cur = key.to_string();
        }
        self.active_audio_itag
            .store(track.format.itag, Ordering::Relaxed);
        {
            let mut st = self.state.lock().await;
            st.set_audio_format(
                FormatId::new(
                    track.format.itag,
                    track.format.last_modified,
                    track.format.xtags.clone(),
                ),
                track.format.mime_type.clone(),
            );
        }
        {
            let mut store = self.store.lock().await;
            let freed = store.audio.init.as_ref().map_or(0, Vec::len)
                + store.audio.segments.values().map(Vec::len).sum::<usize>();
            store.audio = TrackBuffer::new();
            store.bytes_used = store.bytes_used.saturating_sub(freed);
        }
        self.restart_request.store(true, Ordering::SeqCst);
        info!(session = %self.session_id, key, itag = track.format.itag, "sabr_audio_switched");
        self.notify_demand.notify_one();
        true
    }

    async fn prepare_next_request(&self) {
        let mut store = self.store.lock().await;
        let Some((track, sequence)) = self.pending_target(&store) else {
            return;
        };
        let duration = |buffer: &TrackBuffer| {
            if buffer.end_seq > 0 && buffer.total_ms > 0 {
                (buffer.total_ms / buffer.end_seq).max(1)
            } else {
                5000
            }
        };
        let buffer = store.track_mut(track);
        buffer.demand_seq = sequence.max(1);
        let needs_seek = sequence <= buffer.max_seq
            || sequence
                > buffer
                    .max_seq
                    .saturating_add(self.config.prefetch_segments.max(1));
        if needs_seek {
            let target = i64::from(sequence.saturating_sub(1)).max(0) * duration(buffer);
            for kind in [SabrTrack::Audio, SabrTrack::Video] {
                let buffer = store.track_mut(kind);
                let next_sequence = (target / duration(buffer))
                    .saturating_add(1)
                    .min(i64::from(i32::MAX)) as i32;
                buffer.demand_seq = next_sequence;
                buffer.max_seq = next_sequence.saturating_sub(1);
            }
            drop(store);
            self.state.lock().await.seek_to(target);
        } else {
            drop(store);
        }
    }

    // --- consumer API ---------------------------------------------------------

    // Wait for a track's init segment.
    pub async fn get_init(&self, track: SabrTrack) -> SabrResult<Vec<u8>> {
        let deadline = Instant::now() + self.config.segment_wait;
        loop {
            let notified = self.notify_data.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return Err(SabrError::Cancelled);
            }
            {
                let store = self.store.lock().await;
                if let Some(bytes) = store.track(track).init.clone() {
                    return Ok(bytes);
                }
                if let Some(err) = &store.last_error {
                    return Err(err.clone());
                }
                if store.done {
                    return Err(SabrError::SegmentTimeout);
                }
            }
            self.notify_demand.notify_one();
            match deadline.checked_duration_since(Instant::now()) {
                None => return Err(SabrError::SegmentTimeout),
                Some(rem) => {
                    let _ = tokio::time::timeout(rem, notified).await;
                }
            }
        }
    }

    // Wait for a specific segment by sequence number.
    pub async fn get_segment(&self, track: SabrTrack, sequence: i32) -> SabrResult<Vec<u8>> {
        if self.is_cancelled() {
            return Err(SabrError::Cancelled);
        }
        if sequence < 1 {
            return Err(SabrError::SegmentTimeout);
        }
        let end = self.store.lock().await.track(track).end_seq;
        if end > 0 && i64::from(sequence) > end {
            return Err(SabrError::SegmentTimeout);
        }
        let key = (track == SabrTrack::Audio, sequence);
        self.pending_segments
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| PendingRequest {
                count: 0,
                order: self.demand_counter.fetch_add(1, Ordering::Relaxed),
            })
            .count += 1;
        let _demand = SegmentDemand { engine: self, key };
        self.notify_demand.notify_one();
        let deadline = Instant::now() + self.config.segment_wait;
        loop {
            let notified = self.notify_data.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return Err(SabrError::Cancelled);
            }
            {
                let mut store = self.store.lock().await;
                if let Some(bytes) = store.track(track).segments.get(&sequence).cloned() {
                    store.track_mut(track).demand_seq = sequence.max(1);
                    return Ok(bytes);
                }
                if let Some(err) = &store.last_error {
                    if !err.is_retryable() {
                        return Err(err.clone());
                    }
                }
                let tb = store.track(track);
                // Past the end of the track: this segment will never arrive.
                if tb.end_seq > 0 && i64::from(sequence) > tb.end_seq {
                    return Err(SabrError::SegmentTimeout);
                }
                if store.done && tb.segments.get(&sequence).is_none() {
                    return Err(SabrError::SegmentTimeout);
                }
            }
            self.notify_demand.notify_one();
            match deadline.checked_duration_since(Instant::now()) {
                None => return Err(SabrError::SegmentTimeout),
                Some(rem) => {
                    let _ = tokio::time::timeout(rem, notified).await;
                }
            }
        }
    }

    pub async fn ensure_audio_segment(&self, sequence: i32) {
        let _ = self.get_segment(SabrTrack::Audio, sequence).await;
    }

    // Wait until both tracks report format-initialization metadata (so a
    // manifest can be built), or time out.
    pub async fn wait_timing(&self, timeout: Duration) -> SabrResult<SabrTiming> {
        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.format_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return Err(SabrError::Cancelled);
            }
            {
                let store = self.store.lock().await;
                if let Some(err) = &store.last_error {
                    return Err(err.clone());
                }
                let a = &store.audio;
                let v = &store.video;
                let want_video = self.config.mode == RequestMode::AudioVideo;
                if a.initialized && (!want_video || v.initialized) {
                    return Ok(self.build_timing(&store));
                }
            }
            match deadline.checked_duration_since(Instant::now()) {
                None => return Err(SabrError::SegmentTimeout),
                Some(rem) => {
                    let _ = tokio::time::timeout(rem, notified).await;
                }
            }
        }
    }

    fn build_timing(&self, store: &Store) -> SabrTiming {
        let duration_ms = if self.descriptor.duration_ms > 0 {
            self.descriptor.duration_ms
        } else {
            store.audio.total_ms.max(store.video.total_ms).max(0) as u64
        };
        let seg_dur = |count: i64| -> u64 {
            if count > 0 && duration_ms > 0 {
                (duration_ms / count as u64).max(1000)
            } else {
                5000
            }
        };
        SabrTiming {
            duration_ms,
            audio_segment_count: store.audio.end_seq,
            video_segment_count: store.video.end_seq,
            audio_segment_duration_ms: seg_dur(store.audio.end_seq),
            video_segment_duration_ms: seg_dur(store.video.end_seq),
        }
    }

    // Request a seek; takes effect at the next request cycle boundary.
    pub async fn seek_to(&self, target_ms: i64) {
        {
            let mut st = self.state.lock().await;
            st.seek_to(target_ms);
        }
        self.notify_demand.notify_one();
    }

    pub async fn debug_state(&self) -> SabrDebugState {
        let store = self.store.lock().await;
        let effective_url = { self.state.lock().await.effective_url.clone() };
        SabrDebugState {
            session_id: self.session_id.clone(),
            video_id: self.descriptor.video_id.clone(),
            effective_url,
            request_count: store.request_count,
            redirect_count: store.redirect_count,
            bytes_used: store.bytes_used,
            done: store.done,
            last_error: store.last_error.as_ref().map(|e| e.code().to_string()),
            audio_initialized: store.audio.initialized,
            video_initialized: store.video.initialized,
            audio_segments: store.audio.segments.len(),
            video_segments: store.video.segments.len(),
            audio_max_seq: store.audio.max_seq,
            video_max_seq: store.video.max_seq,
            last_protection_status: store.last_protection_status,
        }
    }
}

enum CycleAction {
    Reload,
    Stop,
}

// Decode standard or URL-safe base64 (with or without padding).
pub fn decode_b64_loose(input: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    let normalized = input.replace('-', "+").replace('_', "/");
    let trimmed = normalized.trim_end_matches('=');
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(trimmed)
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_loose_handles_urlsafe_and_padding() {
        // "hi" -> standard "aGk=", urlsafe-nopad "aGk"
        assert_eq!(decode_b64_loose("aGk=").unwrap(), b"hi");
        assert_eq!(decode_b64_loose("aGk").unwrap(), b"hi");
    }

    #[test]
    fn track_buffer_done_by_segment_count() {
        let mut tb = TrackBuffer::new();
        tb.end_seq = 3;
        tb.max_seq = 3;
        assert!(tb.done());
    }
}
