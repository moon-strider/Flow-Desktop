use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use super::sabr::{SabrError, SabrSessionManager, SabrTrack};
use crate::api::innertube::core::clients;

#[derive(Clone)]
pub enum StreamSessionKind {
    Remote { remote_url: String },
    Inline { body: Vec<u8> },
    Local { path: String },
}

#[derive(Clone)]
pub struct StreamSession {
    pub kind: StreamSessionKind,
    pub content_type: String,
    pub expires_at: u64,
    pub user_agent: String,
}

#[derive(Clone)]
struct CachedResponse {
    status_code: u16,
    reason: String,
    content_type: String,
    content_range: Option<String>,
    accept_ranges: String,
    body: Vec<u8>,
    cached_at: u64,
}

#[derive(Clone)]
pub struct StreamingManager {
    sessions: Arc<Mutex<HashMap<String, StreamSession>>>,
    response_cache: Arc<Mutex<HashMap<String, Arc<CachedResponse>>>>,
    image_fetches: Arc<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>>,
    port: u16,
    sabr: SabrSessionManager,
}

const MAX_CACHED_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const MAX_TOTAL_CACHE_BYTES: usize = 192 * 1024 * 1024;
const CACHE_TTL_SECONDS: u64 = 30 * 60;
pub const REMOTE_SESSION_TTL_SECONDS: u64 = 3600;
const MAX_UPSTREAM_RECOVERIES: u32 = 6;
const MAX_HEADER_BYTES: usize = 32 * 1024;
const LOCAL_FILE_CHUNK_BYTES: usize = 256 * 1024;
const IMAGE_UPSTREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

static IMAGE_FETCH_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(6);

const CORS_HEADERS: &str = "Access-Control-Allow-Origin: *\r\n\
Access-Control-Allow-Methods: GET, HEAD, OPTIONS\r\n\
Access-Control-Allow-Headers: Range, Content-Type, Origin, Accept\r\n\
Access-Control-Expose-Headers: Content-Length, Content-Range, Accept-Ranges\r\n";

// googlevideo validates that the UA fetching a stream matches the client (`c=`)
// that minted the URL; a mismatch (e.g. a web UA on a `c=IOS` URL) is a known
// cause of 403s.
//
// The session UA is the one the winning client actually used, recorded at
// extraction time, so it is authoritative and used whenever it is set. The `c=`
// lookup is the fallback for URLs registered without one (manifest and caption
// proxy routes), and reads the same client registry the request was built from —
// previously these were hand-copied constants that had drifted a version behind.
fn user_agent_for_media_url(url: &str, session_user_agent: &str) -> String {
    if !session_user_agent.is_empty() {
        return session_user_agent.to_string();
    }

    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .query_pairs()
                .find(|(key, _)| key == "c")
                .map(|(_, value)| value.into_owned())
        })
        .and_then(|name| clients::by_name(&name))
        .map_or_else(
            || clients::WEB.user_agent.to_string(),
            |client| client.user_agent.to_string(),
        )
}

impl StreamingManager {
    pub fn new() -> (Self, std::net::TcpListener) {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind streaming proxy");
        let port = listener.local_addr().unwrap().port();

        let manager = Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            response_cache: Arc::new(Mutex::new(HashMap::new())),
            image_fetches: Arc::new(Mutex::new(HashMap::new())),
            port,
            sabr: SabrSessionManager::new(),
        };

        (manager, listener)
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    pub fn sabr(&self) -> &SabrSessionManager {
        &self.sabr
    }

    pub fn register_session(
        &self,
        token: String,
        remote_url: String,
        content_type: String,
        user_agent: String,
    ) {
        self.register_remote_session(token, remote_url, content_type, user_agent);
    }

    pub fn register_remote_session(
        &self,
        token: String,
        remote_url: String,
        content_type: String,
        user_agent: String,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let session = StreamSession {
            kind: StreamSessionKind::Remote { remote_url },
            content_type,
            expires_at: now + REMOTE_SESSION_TTL_SECONDS,
            user_agent,
        };
        let mut lock = self.sessions.lock().unwrap();
        lock.insert(token, session);
        lock.retain(|_, s| s.expires_at > now);
    }

    pub fn register_inline_session(&self, token: String, body: Vec<u8>, content_type: String) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let session = StreamSession {
            kind: StreamSessionKind::Inline { body },
            content_type,
            expires_at: now + 3600,
            user_agent: String::new(),
        };
        let mut lock = self.sessions.lock().unwrap();
        lock.insert(token, session);
        lock.retain(|_, s| s.expires_at > now);
    }

    pub fn register_local_session(&self, token: String, path: String, content_type: String) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let session = StreamSession {
            kind: StreamSessionKind::Local { path },
            content_type,
            expires_at: now + 24 * 3600,
            user_agent: String::new(),
        };
        let mut lock = self.sessions.lock().unwrap();
        lock.insert(token, session);
        lock.retain(|_, s| s.expires_at > now);
    }

    pub fn get_session(&self, token: &str) -> Option<StreamSession> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut lock = self.sessions.lock().unwrap();

        if let Some(session) = lock.get(token) {
            if session.expires_at > now {
                return Some(session.clone());
            }
            lock.remove(token);
        }
        None
    }

    fn image_fetch_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut fetches = self.image_fetches.lock().unwrap();
        fetches.retain(|_, pending| pending.strong_count() > 0);
        if let Some(lock) = fetches.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        fetches.insert(key.to_string(), Arc::downgrade(&lock));
        lock
    }

    fn get_cached_response(&self, key: &str) -> Option<Arc<CachedResponse>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut cache = self.response_cache.lock().unwrap();
        cache.retain(|_, response| now.saturating_sub(response.cached_at) <= CACHE_TTL_SECONDS);
        cache.get(key).map(Arc::clone)
    }

    fn store_cached_response(&self, key: String, response: CachedResponse) {
        if response.body.len() > MAX_CACHED_RESPONSE_BYTES {
            return;
        }

        let mut cache = self.response_cache.lock().unwrap();
        cache.insert(key, Arc::new(response));

        let mut total_bytes: usize = cache.values().map(|cached| cached.body.len()).sum();
        while total_bytes > MAX_TOTAL_CACHE_BYTES {
            let oldest_key = cache
                .iter()
                .min_by_key(|(_, cached)| cached.cached_at)
                .map(|(key, _)| key.clone());

            if let Some(oldest_key) = oldest_key {
                if let Some(removed) = cache.remove(&oldest_key) {
                    total_bytes = total_bytes.saturating_sub(removed.body.len());
                }
            } else {
                break;
            }
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// ---------------------------------------------------------------------------
// HTTP request parsing
// ---------------------------------------------------------------------------

struct RequestHead {
    method: String,
    // Path + query as received.
    target: String,
    range: Option<String>,
    content_length: Option<usize>,
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

// Returns the parsed head plus any body bytes already read past the header
// terminator (so a POST body can be drained by the caller).
async fn read_request_head(
    socket: &mut TcpStream,
) -> std::io::Result<Option<(RequestHead, Vec<u8>)>> {
    let mut buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(end) = find_header_end(&buf) {
            let leftover = buf[end + 4..].to_vec();
            return Ok(parse_head(&buf[..end]).map(|head| (head, leftover)));
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Ok(None);
        }
        let n = socket.read(&mut tmp).await?;
        if n == 0 {
            return Ok(parse_head(&buf).map(|head| (head, Vec::new())));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn parse_head(bytes: &[u8]) -> Option<RequestHead> {
    let text = String::from_utf8_lossy(bytes);
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut range = None;
    let mut content_length = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            if name.eq_ignore_ascii_case("range") {
                range = Some(value.trim().to_string());
            } else if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().ok();
            }
        }
    }
    Some(RequestHead {
        method,
        target,
        range,
        content_length,
    })
}

// ---------------------------------------------------------------------------
// Response writers
// ---------------------------------------------------------------------------

async fn write_status_only(
    socket: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
    head_only: bool,
) -> std::io::Result<()> {
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n{CORS_HEADERS}Connection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    if !head_only {
        socket.write_all(body.as_bytes()).await?;
    }
    Ok(())
}

async fn write_options(socket: &mut TcpStream) -> std::io::Result<()> {
    let headers = format!(
        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n{CORS_HEADERS}Access-Control-Max-Age: 600\r\nConnection: close\r\n\r\n"
    );
    socket.write_all(headers.as_bytes()).await
}

// Write a complete in-memory body (200/inline/SABR segment).
//
// Every response this server writes closes the connection: `handle_connection`
// serves exactly one request per socket. Saying anything else — or saying
// nothing, which HTTP/1.1 reads as keep-alive — leaves the client holding a
// connection that is already gone, and GStreamer's HTTP source (WebKitGTK's
// media stack) reacts to that mid-stream by stalling rather than reconnecting.
async fn write_full_body(
    socket: &mut TcpStream,
    content_type: &str,
    cache_control: &str,
    body: &[u8],
    head_only: bool,
) -> std::io::Result<()> {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{CORS_HEADERS}Cache-Control: {cache_control}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    if !head_only {
        socket.write_all(body).await?;
    }
    Ok(())
}

async fn write_cached_response(
    socket: &mut TcpStream,
    cached: &CachedResponse,
    head_only: bool,
) -> std::io::Result<()> {
    let mut response_headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
        cached.status_code,
        cached.reason,
        cached.content_type,
        cached.body.len()
    );
    if let Some(content_range) = cached.content_range.as_deref() {
        response_headers.push_str(&format!("Content-Range: {content_range}\r\n"));
    }
    response_headers.push_str(&format!("Accept-Ranges: {}\r\n", cached.accept_ranges));
    response_headers.push_str(CORS_HEADERS);
    response_headers.push_str("Cache-Control: private, max-age=1800\r\nConnection: close\r\n\r\n");

    socket.write_all(response_headers.as_bytes()).await?;
    if !head_only {
        socket.write_all(&cached.body).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub async fn start_proxy_server(manager: StreamingManager, std_listener: std::net::TcpListener) {
    std_listener
        .set_nonblocking(true)
        .expect("Failed to set nonblocking");
    let listener = TcpListener::from_std(std_listener).expect("Failed to convert TcpListener");

    info!(
        "Starting local media proxy on 127.0.0.1:{}",
        manager.get_port()
    );
    let client = reqwest::Client::builder()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()
        .unwrap_or_default();

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let mgr = manager.clone();
                let clt = client.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, mgr, clt).await {
                        debug!("Streaming connection closed: {:?}", e);
                    }
                });
            }
            Err(e) => {
                error!("Proxy server accept failed: {:?}", e);
            }
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    manager: StreamingManager,
    client: reqwest::Client,
) -> std::io::Result<()> {
    let (head, body_prefix) = match read_request_head(&mut socket).await? {
        Some(value) => value,
        None => {
            write_status_only(&mut socket, 400, "Bad Request", "Malformed request", false).await?;
            return Ok(());
        }
    };

    let method = head.method.to_ascii_uppercase();
    if method == "OPTIONS" {
        return write_options(&mut socket).await;
    }
    // POST is permitted for the in-page extraction sink (`/ytresult`); media
    // routes ignore the body and behave as GET.
    if method != "GET" && method != "HEAD" && method != "POST" {
        write_status_only(
            &mut socket,
            405,
            "Method Not Allowed",
            "Unsupported method",
            false,
        )
        .await?;
        return Ok(());
    }
    let head_only = method == "HEAD";

    let request_url = match reqwest::Url::parse(&format!("http://localhost{}", head.target)) {
        Ok(url) => url,
        Err(_) => {
            write_status_only(
                &mut socket,
                400,
                "Bad Request",
                "Bad request target",
                head_only,
            )
            .await?;
            return Ok(());
        }
    };
    let path = request_url.path().to_string();

    // WebView poToken minter (see api::innertube::core::webview_pot). `/potmint`
    // serves the in-browser mint page; `/potresult` receives the minted token.
    if path == "/potmint" {
        return write_full_body(
            &mut socket,
            "text/html; charset=utf-8",
            "no-store",
            crate::api::innertube::core::webview_pot::MINT_PAGE_HTML.as_bytes(),
            head_only,
        )
        .await;
    }
    if path == "/potresult" {
        let query: HashMap<String, String> = request_url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        if let Some(id) = query.get("id") {
            crate::api::innertube::core::webview_pot::resolve_from_query(
                id,
                query.get("poToken").map(String::as_str),
                query.get("ttl").and_then(|value| value.parse::<u64>().ok()),
                query.get("error").map(String::as_str),
            );
        }
        return write_full_body(&mut socket, "text/plain", "no-store", b"ok", head_only).await;
    }
    // In-page WebView player-response exfil (see webview_player). The youtube.com
    // page POSTs its ytInitialPlayerResponse here (its CSP has no connect-src, so
    // a cross-origin fetch to the loopback proxy is allowed).
    if path == "/ytresult" {
        let id = request_url
            .query_pairs()
            .find(|(key, _)| key == "id")
            .map(|(_, value)| value.into_owned());
        let want = head.content_length.unwrap_or(0);
        let mut body = body_prefix;
        while body.len() < want {
            let mut tmp = [0u8; 16384];
            let n = socket.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        if let Some(id) = id {
            crate::api::innertube::core::webview_player::resolve(
                &id,
                &String::from_utf8_lossy(&body),
            );
        }
        return write_full_body(&mut socket, "text/plain", "no-store", b"ok", head_only).await;
    }
    if path == "/ytdiag" {
        if let Some((_, msg)) = request_url.query_pairs().find(|(key, _)| key == "msg") {
            info!(msg = %msg, "yt extract diag");
        }
        return write_full_body(&mut socket, "text/plain", "no-store", b"ok", head_only).await;
    }

    if path.starts_with("/sabr/") {
        return handle_sabr_route(&mut socket, &manager, &path, head_only).await;
    }

    // Legacy direct/inline routes ------------------------------------------------
    let (token, target_url_override) = if let Some(token) = path.strip_prefix("/stream/") {
        (token.trim_start_matches('/').to_string(), None)
    } else if let Some(token) = path.strip_prefix("/proxy/") {
        let override_url = request_url
            .query_pairs()
            .find_map(|(key, value)| (key == "url").then(|| value.into_owned()));
        (token.trim_start_matches('/').to_string(), override_url)
    } else {
        write_status_only(
            &mut socket,
            404,
            "Not Found",
            "Unknown proxy route",
            head_only,
        )
        .await?;
        return Ok(());
    };

    let session = match manager.get_session(&token) {
        Some(s) => s,
        None => {
            write_status_only(
                &mut socket,
                404,
                "Not Found",
                "Video stream session not found or expired",
                head_only,
            )
            .await?;
            return Ok(());
        }
    };

    if let StreamSessionKind::Inline { body } = &session.kind {
        return write_full_body(
            &mut socket,
            &session.content_type,
            "no-store",
            body,
            head_only,
        )
        .await;
    }

    if let StreamSessionKind::Local { path } = &session.kind {
        return relay_local_file(
            &mut socket,
            path,
            &session.content_type,
            head.range.as_deref(),
            head_only,
        )
        .await;
    }

    let target_url = target_url_override.unwrap_or_else(|| match &session.kind {
        StreamSessionKind::Remote { remote_url } => remote_url.clone(),
        StreamSessionKind::Inline { .. } | StreamSessionKind::Local { .. } => String::new(),
    });

    relay_remote(
        &mut socket,
        &client,
        &manager,
        &session,
        &target_url,
        head.range.as_deref(),
        &path,
        head_only,
    )
    .await
}

#[derive(Clone, Copy)]
enum ByteRange {
    Closed { start: u64, end: u64 },
    Open { start: u64 },
    Suffix { length: u64 },
}

impl ByteRange {
    fn resolve(self, total: u64) -> Option<(u64, u64)> {
        let last = total.checked_sub(1)?;
        match self {
            Self::Closed { start, end } if start < total => Some((start, end.min(last))),
            Self::Open { start } if start < total => Some((start, last)),
            Self::Suffix { length } if length > 0 => Some((total.saturating_sub(length), last)),
            _ => None,
        }
    }

    fn header_value(self) -> String {
        match self {
            Self::Closed { start, end } => format!("bytes={start}-{end}"),
            Self::Open { start } => format!("bytes={start}-"),
            Self::Suffix { length } => format!("bytes=-{length}"),
        }
    }
}

fn parse_byte_offset(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn parse_range_spec(range: Option<&str>) -> Option<ByteRange> {
    let (unit, spec) = range?.trim().split_once('=')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (start, end) = spec.split_once('-')?;
    match (start.is_empty(), end.is_empty()) {
        (true, false) => Some(ByteRange::Suffix {
            length: parse_byte_offset(end)?,
        }),
        (false, true) => Some(ByteRange::Open {
            start: parse_byte_offset(start)?,
        }),
        (false, false) => {
            let start = parse_byte_offset(start)?;
            let end = parse_byte_offset(end)?;
            (start <= end).then_some(ByteRange::Closed { start, end })
        }
        (true, true) => None,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ResponseRange {
    start: u64,
    end: u64,
    total: u64,
}

impl ResponseRange {
    fn length(self) -> u64 {
        self.end - self.start + 1
    }
}

fn parse_content_range(value: &str) -> Option<ResponseRange> {
    let (unit, spec) = value.split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (bounds, total) = spec.split_once('/')?;
    let (start, end) = bounds.split_once('-')?;
    let start = parse_byte_offset(start)?;
    let end = parse_byte_offset(end)?;
    let total = parse_byte_offset(total)?;
    (start <= end && end < total).then_some(ResponseRange { start, end, total })
}

fn resumable_response_range(response: &reqwest::Response) -> Option<ResponseRange> {
    let headers = response.headers();
    if headers
        .get("Content-Encoding")
        .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return None;
    }
    let length = headers
        .get("Content-Length")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_byte_offset)?;
    match response.status() {
        reqwest::StatusCode::OK => Some(ResponseRange {
            start: 0,
            end: length.checked_sub(1)?,
            total: length,
        }),
        reqwest::StatusCode::PARTIAL_CONTENT => {
            let range = headers
                .get("Content-Range")
                .and_then(|value| value.to_str().ok())
                .and_then(parse_content_range)?;
            (range.length() == length).then_some(range)
        }
        _ => None,
    }
}

async fn relay_local_file(
    socket: &mut TcpStream,
    path: &str,
    content_type: &str,
    client_range: Option<&str>,
    head_only: bool,
) -> std::io::Result<()> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(_) => {
            return write_status_only(socket, 404, "Not Found", "Local file not found", head_only)
                .await;
        }
    };
    let total = match file.metadata().await {
        Ok(meta) => meta.len(),
        Err(_) => {
            return write_status_only(
                socket,
                500,
                "Internal Server Error",
                "Cannot read file",
                head_only,
            )
            .await;
        }
    };

    let range = if head_only {
        None
    } else {
        parse_range_spec(client_range)
    };
    let (start, end_inclusive, length) = if let Some(range) = range {
        let Some((start, end)) = range.resolve(total) else {
            let headers = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\n{CORS_HEADERS}Connection: close\r\n\r\n"
            );
            return socket.write_all(headers.as_bytes()).await;
        };
        (start, end, end - start + 1)
    } else {
        (0, total.saturating_sub(1), total)
    };

    let (status, reason) = if range.is_some() {
        (206, "Partial Content")
    } else {
        (200, "OK")
    };
    let mut headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {length}\r\nAccept-Ranges: bytes\r\n"
    );
    if range.is_some() {
        headers.push_str(&format!(
            "Content-Range: bytes {start}-{end_inclusive}/{total}\r\n"
        ));
    }
    headers.push_str(CORS_HEADERS);
    headers.push_str("Cache-Control: private, max-age=3600\r\nConnection: close\r\n\r\n");
    socket.write_all(headers.as_bytes()).await?;

    if head_only || length == 0 {
        return Ok(());
    }

    if start > 0 && file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return Ok(());
    }

    let mut remaining = length;
    let mut buf = vec![0u8; LOCAL_FILE_CHUNK_BYTES];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let read = match file.read(&mut buf[..want]).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(_) => break,
        };
        if socket.write_all(&buf[..read]).await.is_err() {
            return Ok(());
        }
        remaining -= read as u64;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn relay_remote(
    socket: &mut TcpStream,
    client: &reqwest::Client,
    manager: &StreamingManager,
    session: &StreamSession,
    target_url: &str,
    client_range: Option<&str>,
    _path: &str,
    head_only: bool,
) -> std::io::Result<()> {
    let range = if head_only {
        None
    } else {
        parse_range_spec(client_range)
    };
    // Match the fetch UA to the URL's `c=` client (mobile parity); the target URL
    // is stable across recovery attempts, so resolve it once.
    let upstream_user_agent = user_agent_for_media_url(target_url, &session.user_agent);
    let range_key = range.map_or_else(|| "full".to_string(), ByteRange::header_value);
    let cache_key = format!("{target_url}|{range_key}");

    let is_image = session.content_type.starts_with("image/");
    if is_image && let Some(cached) = manager.get_cached_response(&cache_key) {
        return write_cached_response(socket, &cached, head_only).await;
    }
    let _image_fetch = if is_image && !head_only {
        let lock = manager.image_fetch_lock(&cache_key).lock_owned().await;
        if let Some(cached) = manager.get_cached_response(&cache_key) {
            return write_cached_response(socket, &cached, head_only).await;
        }
        Some(lock)
    } else {
        None
    };

    let mut headers_written = false;
    let mut bytes_relayed: u64 = 0;
    let mut attempt: u32 = 0;
    let mut content_length_value: usize = 0;
    let mut content_type_value = session.content_type.clone();
    let mut content_range_value: Option<String> = None;
    let mut accept_ranges_value = "bytes".to_string();
    let mut status_code_value: u16 = 200;
    let mut reason_value = "OK".to_string();
    let mut cached_body: Option<Vec<u8>> = None;
    let mut recovery_range: Option<ResponseRange> = None;
    let mut recovery_url: Option<reqwest::Url> = None;
    let mut strong_etag: Option<reqwest::header::HeaderValue> = None;

    loop {
        let expected_resume = if headers_written {
            let Some(original) = recovery_range else {
                break;
            };
            let Some(start) = original.start.checked_add(bytes_relayed) else {
                break;
            };
            if start > original.end {
                break;
            }
            Some(ResponseRange { start, ..original })
        } else {
            None
        };
        let range_header = expected_resume.map_or_else(
            || range.map(ByteRange::header_value),
            |resume| Some(format!("bytes={}-{}", resume.start, resume.end)),
        );

        let mut req = if head_only {
            client.head(target_url)
        } else {
            client.get(target_url)
        }
        .header("User-Agent", &upstream_user_agent)
        .header("Accept-Encoding", "identity");
        if is_image {
            req = req
                .header(
                    "Accept",
                    "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                )
                .header("Origin", "https://music.youtube.com")
                .header("Referer", "https://music.youtube.com/")
                .timeout(IMAGE_UPSTREAM_TIMEOUT);
        }
        if let Some(rh) = &range_header {
            req = req.header("Range", rh);
        }
        if expected_resume.is_some()
            && let Some(etag) = &strong_etag
        {
            req = req.header("If-Range", etag);
        }

        // Held for the whole relay so a slot covers the body too, not just the
        // response head. Media is never gated here — only artwork.
        let _image_slot = if is_image {
            IMAGE_FETCH_SLOTS.acquire().await.ok()
        } else {
            None
        };

        let response = match req.send().await {
            Ok(res) => res,
            Err(e) => {
                if headers_written {
                    warn!("Upstream re-request failed after partial relay: {e}");
                    return Ok(());
                }
                error!("Failed to fetch upstream stream: {:?}", e);
                return write_status_only(
                    socket,
                    502,
                    "Bad Gateway",
                    "Failed to proxy media stream",
                    head_only,
                )
                .await;
            }
        };

        if let Some(expected) = expected_resume
            && (response.status() != reqwest::StatusCode::PARTIAL_CONTENT
                || resumable_response_range(&response) != Some(expected)
                || recovery_url.as_ref() != Some(response.url())
                || strong_etag
                    .as_ref()
                    .is_none_or(|etag| response.headers().get("ETag") != Some(etag)))
        {
            warn!("Upstream recovery response did not match the original representation");
            return Ok(());
        }

        if !headers_written {
            recovery_range = resumable_response_range(&response);
            recovery_url = Some(response.url().clone());
            let status = response.status();
            status_code_value = status.as_u16();
            reason_value = status.canonical_reason().unwrap_or("OK").to_string();
            if !status.is_success() && !status.is_redirection() {
                warn!(
                    status = status.as_u16(),
                    range = ?range_header,
                    ua = %upstream_user_agent,
                    url = %target_url,
                    "Upstream rejected stream relay"
                );
            }
            let headers = response.headers();
            strong_etag = headers
                .get("ETag")
                .filter(|value| {
                    let bytes = value.as_bytes();
                    bytes.len() >= 2
                        && bytes.starts_with(b"\"")
                        && bytes.ends_with(b"\"")
                        && bytes[1..bytes.len() - 1]
                            .iter()
                            .all(|&byte| matches!(byte, b'!' | b'#'..=b'~' | 0x80..=0xff))
                })
                .cloned();
            content_type_value = headers
                .get("Content-Type")
                .and_then(|h| h.to_str().ok())
                .unwrap_or(session.content_type.as_str())
                .to_string();
            // Live HLS playlists/segments arrive chunked (no Content-Length); reqwest decodes
            // the framing, so the length is unknown to us here.
            let content_length_header = headers
                .get("Content-Length")
                .and_then(|h| h.to_str().ok())
                .map(ToOwned::to_owned);
            content_length_value = content_length_header
                .as_deref()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            content_range_value = headers
                .get("Content-Range")
                .and_then(|h| h.to_str().ok())
                .map(ToOwned::to_owned);
            accept_ranges_value = headers
                .get("Accept-Ranges")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("bytes")
                .to_string();

            let ct_lower = content_type_value.to_ascii_lowercase();
            let is_manifest = ct_lower.contains("mpegurl")
                || ct_lower.contains("dash+xml")
                || ct_lower.contains("application/vnd.apple")
                || ct_lower.contains("text/vtt");
            if is_manifest || strong_etag.is_none() {
                recovery_range = None;
            }
            let is_cacheable_kind = ct_lower.starts_with("image/");

            let should_cache = is_image
                && status.is_success()
                && !is_manifest
                && is_cacheable_kind
                && content_length_header.is_some()
                && content_length_value > 0
                && content_length_value <= MAX_CACHED_RESPONSE_BYTES;

            let mut response_headers = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: {}\r\n",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                content_type_value,
            );
            if let Some(ref content_length) = content_length_header {
                response_headers.push_str(&format!("Content-Length: {content_length}\r\n"));
            }
            if let Some(range_val) = content_range_value.as_deref() {
                response_headers.push_str(&format!("Content-Range: {range_val}\r\n"));
            }
            response_headers.push_str(&format!("Accept-Ranges: {accept_ranges_value}\r\n"));
            response_headers.push_str(CORS_HEADERS);
            if is_manifest || !status.is_success() {
                response_headers.push_str("Cache-Control: no-cache, no-store, must-revalidate\r\n");
            } else {
                response_headers.push_str("Cache-Control: private, max-age=1800\r\n");
            }
            // Content-Length the body is delimited by that close anyway.
            response_headers.push_str("Connection: close\r\n\r\n");

            socket.write_all(response_headers.as_bytes()).await?;
            headers_written = true;

            if head_only {
                return Ok(());
            }
            if should_cache {
                cached_body = Some(Vec::with_capacity(content_length_value));
            }
        }

        // Stream the body; on a clean finish we break, recover on a reset.
        let mut stream = response.bytes_stream();
        let mut clean_finish = true;
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    if let Some(body) = cached_body.as_mut() {
                        if body.len() + chunk.len() <= MAX_CACHED_RESPONSE_BYTES {
                            body.extend_from_slice(&chunk);
                        } else {
                            cached_body = None;
                        }
                    }
                    if socket.write_all(&chunk).await.is_err() {
                        return Ok(());
                    }
                    bytes_relayed = bytes_relayed.saturating_add(chunk.len() as u64);
                }
                Err(e) => {
                    clean_finish = false;
                    warn!(
                        "Upstream stream chunk error after {bytes_relayed} bytes (attempt {attempt}): {e}"
                    );
                    cached_body = None;
                    break;
                }
            }
        }

        if clean_finish {
            break;
        }
        if recovery_range.is_none_or(|range| bytes_relayed >= range.length()) {
            break;
        }

        attempt += 1;
        if attempt > MAX_UPSTREAM_RECOVERIES {
            warn!("Giving up upstream recovery after {attempt} attempts");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(150 * u64::from(attempt))).await;
    }

    if let Some(body) = cached_body {
        if body.len() == content_length_value && content_length_value > 0 {
            manager.store_cached_response(
                cache_key,
                CachedResponse {
                    status_code: status_code_value,
                    reason: reason_value,
                    content_type: content_type_value,
                    content_range: content_range_value,
                    accept_ranges: accept_ranges_value,
                    body,
                    cached_at: now_secs(),
                },
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SABR routes
// ---------------------------------------------------------------------------

fn sabr_track_content_type(mime: &str, is_audio: bool) -> &'static str {
    let webm = mime.contains("webm");
    match (is_audio, webm) {
        (true, true) => "audio/webm",
        (true, false) => "audio/mp4",
        (false, true) => "video/webm",
        (false, false) => "video/mp4",
    }
}

fn sabr_error_status(err: &SabrError) -> (u16, &'static str) {
    match err {
        SabrError::SegmentTimeout | SabrError::BackoffExceeded => (503, "Service Unavailable"),
        SabrError::AttestationRequired | SabrError::ReloadRequired => (409, "Conflict"),
        SabrError::NoPlayableFormats | SabrError::NoStreamingData => (404, "Not Found"),
        SabrError::HttpStatus(code) => {
            if *code == 403 || *code == 401 {
                (403, "Forbidden")
            } else {
                (502, "Bad Gateway")
            }
        }
        _ => (502, "Bad Gateway"),
    }
}

async fn handle_sabr_route(
    socket: &mut TcpStream,
    manager: &StreamingManager,
    path: &str,
    head_only: bool,
) -> std::io::Result<()> {
    // /sabr/{session}/{...}
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.len() < 3 {
        return write_status_only(socket, 404, "Not Found", "Bad SABR route", head_only).await;
    }
    let session_id = segments[1];

    let handle = match manager.sabr().activate(session_id) {
        Ok(h) => h,
        Err(e) => {
            let (code, reason) = sabr_error_status(&e);
            return write_status_only(socket, code, reason, &format!("SABR: {e}"), head_only).await;
        }
    };
    let engine = handle.engine.clone();

    // Resolve the content-type of an audio track by its manifest key.
    let audio_ct = |key: &str| -> &'static str {
        let mime = engine
            .audio_tracks()
            .iter()
            .find(|t| t.key == key)
            .map(|t| t.format.mime_type.as_str())
            .unwrap_or("audio/mp4");
        sabr_track_content_type(mime, true)
    };
    let video_ct = sabr_track_content_type(&engine.selected().video.mime_type, false);

    match segments[2..] {
        ["manifest.mpd"] => {
            let timing = match engine.wait_timing(std::time::Duration::from_secs(8)).await {
                Ok(t) => t,
                Err(e) => {
                    let (code, reason) = sabr_error_status(&e);
                    warn!(session = %session_id, error = %e, "sabr_manifest_timeout");
                    return write_status_only(
                        socket,
                        code,
                        reason,
                        &format!("SABR manifest: {e}"),
                        head_only,
                    )
                    .await;
                }
            };
            let base = format!(
                "http://127.0.0.1:{}/sabr/{}",
                manager.get_port(),
                session_id
            );
            let xml = super::sabr::manifest::build_dash_manifest(
                &base,
                engine.audio_tracks(),
                &engine.selected().video,
                &timing,
            );
            info!(session = %session_id, tracks = engine.audio_tracks().len(), "sabr_manifest_served");
            write_full_body(
                socket,
                "application/dash+xml",
                "no-store",
                xml.as_bytes(),
                head_only,
            )
            .await
        }
        ["video", "init"] => match engine.get_init(SabrTrack::Video).await {
            Ok(bytes) => {
                write_full_body(socket, video_ct, "private, max-age=3600", &bytes, head_only).await
            }
            Err(e) => {
                let (code, reason) = sabr_error_status(&e);
                write_status_only(socket, code, reason, &format!("SABR init: {e}"), head_only).await
            }
        },
        ["video", "seg", number] => {
            let Ok(sequence) = number.parse::<i32>() else {
                return write_status_only(
                    socket,
                    400,
                    "Bad Request",
                    "Bad segment number",
                    head_only,
                )
                .await;
            };
            match engine.get_segment(SabrTrack::Video, sequence).await {
                Ok(bytes) => write_full_body(socket, video_ct, "no-store", &bytes, head_only).await,
                Err(e) => {
                    let (code, reason) = sabr_error_status(&e);
                    debug!(session = %session_id, seq = sequence, error = %e, "sabr_segment_unavailable");
                    write_status_only(socket, code, reason, &format!("SABR seg: {e}"), head_only)
                        .await
                }
            }
        }
        ["audio", key, "init"] => {
            if !engine.set_active_audio(key).await {
                return write_status_only(
                    socket,
                    404,
                    "Not Found",
                    "Unknown audio track",
                    head_only,
                )
                .await;
            }
            match engine.get_init(SabrTrack::Audio).await {
                Ok(bytes) => {
                    write_full_body(
                        socket,
                        audio_ct(key),
                        "private, max-age=3600",
                        &bytes,
                        head_only,
                    )
                    .await
                }
                Err(e) => {
                    let (code, reason) = sabr_error_status(&e);
                    write_status_only(socket, code, reason, &format!("SABR init: {e}"), head_only)
                        .await
                }
            }
        }
        ["audio", key, "seg", number] => {
            if !engine.set_active_audio(key).await {
                return write_status_only(
                    socket,
                    404,
                    "Not Found",
                    "Unknown audio track",
                    head_only,
                )
                .await;
            }
            let Ok(sequence) = number.parse::<i32>() else {
                return write_status_only(
                    socket,
                    400,
                    "Bad Request",
                    "Bad segment number",
                    head_only,
                )
                .await;
            };
            engine.ensure_audio_segment(sequence).await;
            match engine.get_segment(SabrTrack::Audio, sequence).await {
                Ok(bytes) => {
                    write_full_body(socket, audio_ct(key), "no-store", &bytes, head_only).await
                }
                Err(e) => {
                    let (code, reason) = sabr_error_status(&e);
                    debug!(session = %session_id, key, seq = sequence, error = %e, "sabr_segment_unavailable");
                    write_status_only(socket, code, reason, &format!("SABR seg: {e}"), head_only)
                        .await
                }
            }
        }
        ["health"] => {
            let state = engine.debug_state().await;
            let json = serde_json::to_string(&state).unwrap_or_else(|_| "{}".to_string());
            write_full_body(
                socket,
                "application/json",
                "no-store",
                json.as_bytes(),
                head_only,
            )
            .await
        }
        _ => write_status_only(socket, 404, "Not Found", "Unknown SABR route", head_only).await,
    }
}
