use crate::api::innertube::InnertubeClient;
use crate::api::innertube::core::clients;
use std::time::{Duration, Instant};

impl InnertubeClient {
    pub async fn fetch_visitor_data(&self) -> Option<String> {
        if let Ok(guard) = self.visitor_data.read() {
            if let Some(existing) = guard.as_ref().filter(|value| !value.is_empty()) {
                return Some(existing.clone());
            }
        }

        tokio::time::timeout(Duration::from_secs(3), async {
            let mut last_attempt = self.visitor_bootstrap.lock().await;
            if let Ok(guard) = self.visitor_data.read() {
                if let Some(existing) = guard.as_ref().filter(|value| !value.is_empty()) {
                    return Some(existing.clone());
                }
            }
            if last_attempt.is_some_and(|at| at.elapsed() < Duration::from_secs(30)) {
                return None;
            }
            *last_attempt = Some(Instant::now());
            let mut payload = serde_json::json!({});
            if let Ok(res) = self
                .post_innertube("visitor_id", &clients::WEB, &mut payload)
                .await
            {
                if let Some(vd) = res["responseContext"]["visitorData"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                {
                    if let Ok(mut guard) = self.visitor_data.write() {
                        *guard = Some(vd.to_string());
                    }
                    return Some(vd.to_string());
                }
            }
            None
        })
        .await
        .unwrap_or(None)
    }
}
