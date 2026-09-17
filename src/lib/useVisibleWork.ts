import { useEffect, useState, useSyncExternalStore, type RefObject } from 'react';
import { useTabContext } from './tabContext';

const visibilityListeners = new Set<() => void>();

function notifyVisibility() {
  for (const listener of visibilityListeners) listener();
}

function subscribeVisibility(listener: () => void) {
  if (visibilityListeners.size === 0) document.addEventListener('visibilitychange', notifyVisibility);
  visibilityListeners.add(listener);
  return () => {
    visibilityListeners.delete(listener);
    if (visibilityListeners.size === 0) document.removeEventListener('visibilitychange', notifyVisibility);
  };
}

function isDocumentVisible() {
  return document.visibilityState !== 'hidden';
}

export function useVisibleWork() {
  const { active } = useTabContext();
  const visible = useSyncExternalStore(subscribeVisibility, isDocumentVisible);
  return active && visible;
}

const targets = new Map<Element, (visible: boolean) => void>();
let observer: IntersectionObserver | null = null;

export function useNearViewport(ref: RefObject<HTMLElement | null>, enabled = true) {
  const pageVisible = useVisibleWork();
  const active = enabled && pageVisible;
  const [near, setNear] = useState(false);

  useEffect(() => {
    const element = ref.current;
    if (!active || !element) {
      setNear(false);
      return;
    }
    if (typeof IntersectionObserver === 'undefined') {
      setNear(true);
      return;
    }
    if (!observer) {
      observer = new IntersectionObserver((entries) => {
        for (const entry of entries) targets.get(entry.target)?.(entry.isIntersecting);
      }, { rootMargin: '400px 0px' });
    }
    targets.set(element, setNear);
    observer.observe(element);
    return () => {
      targets.delete(element);
      observer?.unobserve(element);
      if (targets.size === 0) {
        observer?.disconnect();
        observer = null;
      }
    };
  }, [active, ref]);

  return active && near;
}
