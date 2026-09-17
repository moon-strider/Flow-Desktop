import { useEffect, useState } from 'react';
import { useSubscriptionStore } from '../store/useSubscriptionStore';
import { getChannelDetails } from './api/youtube';
import { getCachedChannelDetails } from './channelDetailsCache';
import { useVisibleWork } from './useVisibleWork';

function isValidAvatarUrl(url?: string | null): url is string {
  return !!url?.startsWith('http') && !/ytimg\.com\/vi\//i.test(url);
}

/**
 * Resolves a channel avatar URL by:
 * 1. Checking the subscription store (instant, free)
 * 2. Checking the in-memory cache
 * 3. Queueing a batched `getChannelDetails()` fetch (max 4 concurrent)
 *
 * Automatically re-renders when the avatar resolves.
 */
export function useChannelAvatar(
  channelId: string | null | undefined,
  enabled = true,
): string | null {
  const visible = useVisibleWork();
  const subAvatar = useSubscriptionStore((state) => {
    if (!channelId) return null;
    const sub = state.subscriptions.find((channel) => channel.id === channelId);
    return sub && isValidAvatarUrl(sub.avatarUrl) ? sub.avatarUrl : null;
  });
  const [resolved, setResolved] = useState<{ id: string; url: string | null } | null>(null);
  const cached = channelId ? getCachedChannelDetails(channelId)?.avatarUrl : null;
  const cachedAvatar = isValidAvatarUrl(cached) ? cached : null;
  const retainedAvatar = resolved && resolved.id === channelId ? resolved.url : null;

  useEffect(() => {
    if (!channelId || !enabled || !visible || subAvatar || retainedAvatar) return;
    const controller = new AbortController();
    void getChannelDetails(channelId, { signal: controller.signal, background: true }).then(
      (details) => {
        if (!controller.signal.aborted) {
          setResolved({ id: channelId, url: isValidAvatarUrl(details.avatarUrl) ? details.avatarUrl : null });
        }
      },
      () => {},
    );
    return () => controller.abort();
  }, [channelId, enabled, visible, subAvatar, retainedAvatar]);

  return subAvatar || retainedAvatar || cachedAvatar;
}
