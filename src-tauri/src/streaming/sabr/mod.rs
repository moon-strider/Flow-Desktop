//! SABR (Server Adaptive Bit Rate) streaming engine.
//!
//! YouTube is migrating playback away from direct media URLs / DASH / HLS toward
//! SABR, where the client POSTs a protobuf `VideoPlaybackAbrRequest` and the
//! server streams back media wrapped in UMP (Universal Media Protocol) frames.
//! This module implements that protocol in Rust, behind the local media proxy,
//! and exposes plain local HTTP endpoints (a DASH manifest + init/segment URLs)
//! that the existing dash.js-based player can consume without knowing SABR exists.
//!
//! Layering (bottom-up):
//!   - `pb`       — minimal protobuf wire codec (no protoc/prost dependency)
//!   - `ump`      — UMP varint + streaming frame parser
//!   - `messages` — typed SABR request/response messages
//!   - `errors`   — error taxonomy + retry/reload classification
//!   - `selector` — codec/browser-aware format selection
//!   - `session`  — per-request state machine + request builder + part dispatch
//!   - `engine`   — async network loop, bounded segment buffer, lifecycle
//!   - `manifest` — local DASH manifest generation

pub mod errors;
pub mod messages;
pub mod pb;
pub mod selector;
pub mod ump;

pub mod engine;
pub mod manifest;
pub mod session;

pub use errors::{SabrError, SabrResult};
pub use selector::{CodecSupport, SabrFormat, SelectedFormats};

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use crate::api::innertube::core::clients;
use engine::{SabrEngine, SabrEngineConfig};

// Immutable inputs needed to drive a SABR session, assembled by extraction.
#[derive(Debug, Clone)]
pub struct SabrSessionDescriptor {
    pub video_id: String,
    pub server_abr_streaming_url: String,
    pub visitor_data: Option<String>,
    pub po_token: Option<String>,
    // Base64-decoded `videoPlaybackUstreamerConfig`.
    pub ustreamer_config: Vec<u8>,
    pub client_profile: ClientProfile,
    pub duration_ms: u64,
    pub formats: Vec<SabrFormat>,
}

// The InnerTube client identity that produced the player response, echoed back
// in SABR requests so the server keeps serving the same session.
#[derive(Debug, Clone)]
pub struct ClientProfile {
    pub client_name_id: i32,
    pub client_version: String,
    pub user_agent: String,
    pub device_make: String,
    pub device_model: String,
    pub os_name: String,
    pub os_version: String,
}

impl ClientProfile {
    /// Derive the SABR identity from the registry entry of the client whose player
    /// response opened the session, so the two can never disagree.
    #[must_use]
    pub fn from_client(client: &clients::YouTubeClient) -> Self {
        Self {
            client_name_id: client.client_name_id(),
            client_version: client.version.to_string(),
            user_agent: client.user_agent.to_string(),
            device_make: client.device_make.unwrap_or_default().to_string(),
            device_model: client.device_model.unwrap_or_default().to_string(),
            os_name: client.os_name.unwrap_or_default().to_string(),
            os_version: client.os_version.unwrap_or_default().to_string(),
        }
    }

    /// Resolve by InnerTube client name, for callers that only have the name a
    /// media URL carries. Unknown names fall back to WEB, which is what an
    /// unrecognized `c=` parameter most likely is.
    #[must_use]
    pub fn from_client_name(client_name: &str) -> Self {
        Self::from_client(clients::by_name(client_name).unwrap_or(&clients::WEB))
    }

    #[cfg(test)]
    pub fn ios() -> Self {
        Self::from_client(&clients::IOS)
    }
}

// Which track a proxy route refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SabrTrack {
    Audio,
    Video,
}

impl SabrTrack {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "audio" => Some(SabrTrack::Audio),
            "video" => Some(SabrTrack::Video),
            _ => None,
        }
    }
}

pub struct SabrSessionHandle {
    pub session_id: String,
    pub video_id: String,
    pub engine: Arc<SabrEngine>,
    prepared_at: Instant,
}

struct PreparedSession {
    descriptor: SabrSessionDescriptor,
    support: CodecSupport,
    prepared_at: Instant,
}

#[derive(Default)]
struct SessionRegistry {
    sessions: HashMap<String, Arc<SabrSessionHandle>>,
    prepared: HashMap<String, PreparedSession>,
    leases: HashMap<String, HashMap<String, Instant>>,
}

#[derive(Clone)]
pub struct SabrSessionManager {
    registry: Arc<Mutex<SessionRegistry>>,
    counter: Arc<AtomicU64>,
    cleanup_started: Arc<std::sync::atomic::AtomicBool>,
    config: SabrEngineConfig,
}

impl Default for SabrSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SabrSessionManager {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(SessionRegistry::default())),
            counter: Arc::new(AtomicU64::new(1)),
            cleanup_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config: SabrEngineConfig::default(),
        }
    }

    fn prune(registry: &mut SessionRegistry) {
        registry.leases.retain(|_, consumers| {
            consumers.retain(|_, touched| touched.elapsed() < Duration::from_secs(180));
            !consumers.is_empty()
        });
        registry.sessions.retain(|id, handle| {
            if registry.leases.contains_key(id) {
                true
            } else {
                handle.engine.cancel();
                false
            }
        });
        registry.prepared.retain(|id, prepared| {
            registry.leases.contains_key(id)
                || prepared.prepared_at.elapsed() < Duration::from_secs(3600)
        });
    }

    fn start_cleanup(&self) {
        if self.cleanup_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let registry = Arc::downgrade(&self.registry);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(30));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                Self::prune(&mut registry.lock().unwrap());
            }
        });
    }

    pub fn prepare(&self, descriptor: SabrSessionDescriptor, support: CodecSupport) -> String {
        let mut registry = self.registry.lock().unwrap();
        Self::prune(&mut registry);
        let existing = registry
            .prepared
            .iter()
            .find(|(_, prepared)| prepared.descriptor.video_id == descriptor.video_id)
            .map(|(id, _)| id.clone());
        let id = existing
            .unwrap_or_else(|| format!("s{}", self.counter.fetch_add(1, Ordering::Relaxed)));
        if registry.prepared.len() >= 32 {
            let oldest = registry
                .prepared
                .iter()
                .filter(|(candidate, _)| {
                    *candidate != &id && !registry.leases.contains_key(*candidate)
                })
                .min_by_key(|(_, prepared)| prepared.prepared_at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                registry.prepared.remove(&oldest);
            }
        }
        registry.prepared.insert(
            id.clone(),
            PreparedSession {
                descriptor,
                support,
                prepared_at: Instant::now(),
            },
        );
        id
    }

    pub fn acquire(&self, session_id: &str, lease_id: &str) -> SabrResult<()> {
        let mut registry = self.registry.lock().unwrap();
        Self::prune(&mut registry);
        self.activate_locked(&mut registry, session_id)?;
        registry
            .leases
            .entry(session_id.to_string())
            .or_default()
            .insert(lease_id.to_string(), Instant::now());
        drop(registry);
        self.start_cleanup();
        Ok(())
    }

    fn activate_locked(
        &self,
        registry: &mut SessionRegistry,
        session_id: &str,
    ) -> SabrResult<Arc<SabrSessionHandle>> {
        let prepared = registry
            .prepared
            .get(session_id)
            .ok_or(SabrError::Cancelled)?;
        if let Some(handle) = registry.sessions.get(session_id) {
            if !handle.engine.is_finished() || prepared.prepared_at <= handle.prepared_at {
                return Ok(handle.clone());
            }
        }
        let descriptor = prepared.descriptor.clone();
        let selected = selector::select_formats(&descriptor.formats, Some(480), prepared.support)
            .ok_or(SabrError::NoPlayableFormats)?;
        let engine = Arc::new(SabrEngine::new(
            session_id.to_string(),
            descriptor.clone(),
            selected,
            self.config.clone(),
        ));
        let handle = Arc::new(SabrSessionHandle {
            session_id: session_id.to_string(),
            video_id: descriptor.video_id,
            engine: engine.clone(),
            prepared_at: prepared.prepared_at,
        });
        if let Some(previous) = registry
            .sessions
            .insert(session_id.to_string(), handle.clone())
        {
            previous.engine.cancel();
        }
        engine.spawn();
        Ok(handle)
    }

    pub fn touch(&self, session_id: &str, lease_id: &str) -> bool {
        let mut registry = self.registry.lock().unwrap();
        let Some(touched) = registry
            .leases
            .get_mut(session_id)
            .and_then(|leases| leases.get_mut(lease_id))
        else {
            return false;
        };
        *touched = Instant::now();
        true
    }

    pub fn release(&self, session_id: &str, lease_id: &str) {
        let mut registry = self.registry.lock().unwrap();
        if let Some(leases) = registry.leases.get_mut(session_id) {
            leases.remove(lease_id);
            if !leases.is_empty() {
                return;
            }
        }
        registry.leases.remove(session_id);
        if let Some(handle) = registry.sessions.remove(session_id) {
            handle.engine.cancel();
        }
    }

    pub fn activate(&self, session_id: &str) -> SabrResult<Arc<SabrSessionHandle>> {
        let mut registry = self.registry.lock().unwrap();
        if !registry.leases.contains_key(session_id) {
            return Err(SabrError::Cancelled);
        }
        self.activate_locked(&mut registry, session_id)
    }

    pub fn get(&self, session_id: &str) -> Option<Arc<SabrSessionHandle>> {
        self.registry
            .lock()
            .unwrap()
            .sessions
            .get(session_id)
            .cloned()
    }
}
