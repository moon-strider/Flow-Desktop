import { invokeBackend } from "./errors";

export function acquireSabrSession(sessionId: string, leaseId: string) {
  return invokeBackend<void>("acquire_sabr_session", { sessionId, leaseId });
}

export function touchSabrSession(sessionId: string, leaseId: string) {
  return invokeBackend<boolean>("touch_sabr_session", { sessionId, leaseId });
}

export function releaseSabrSession(sessionId: string, leaseId: string) {
  return invokeBackend<void>("release_sabr_session", { sessionId, leaseId });
}
