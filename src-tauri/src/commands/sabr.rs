use tauri::State;

use crate::streaming::proxy::StreamingManager;

fn validate_lease(session_id: &str, lease_id: &str) -> Result<(), String> {
    if !session_id.starts_with('s')
        || session_id.len() > 24
        || session_id.len() < 2
        || !session_id[1..].bytes().all(|byte| byte.is_ascii_digit())
        || uuid::Uuid::parse_str(lease_id).is_err()
    {
        return Err("Invalid SABR session lease".to_string());
    }
    Ok(())
}

#[tauri::command]
pub async fn acquire_sabr_session(
    session_id: String,
    lease_id: String,
    streaming_manager: State<'_, StreamingManager>,
) -> Result<(), String> {
    validate_lease(&session_id, &lease_id)?;
    streaming_manager
        .sabr()
        .acquire(&session_id, &lease_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn touch_sabr_session(
    session_id: String,
    lease_id: String,
    streaming_manager: State<'_, StreamingManager>,
) -> Result<bool, String> {
    validate_lease(&session_id, &lease_id)?;
    Ok(streaming_manager.sabr().touch(&session_id, &lease_id))
}

#[tauri::command]
pub async fn release_sabr_session(
    session_id: String,
    lease_id: String,
    streaming_manager: State<'_, StreamingManager>,
) -> Result<(), String> {
    validate_lease(&session_id, &lease_id)?;
    streaming_manager.sabr().release(&session_id, &lease_id);
    Ok(())
}
