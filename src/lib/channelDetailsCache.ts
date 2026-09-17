import type { ChannelDetails } from '../types/video';

const MAX_ENTRIES = 256;
const MAX_CONCURRENT = 4;
const MAX_QUEUED = 128;
const CACHE_TTL_MS = 15 * 60 * 1000;
const FAILURE_TTL_MS = 30 * 1000;

export interface ChannelDetailsOptions {
  signal?: AbortSignal;
  background?: boolean;
}

interface CachedDetails {
  details?: ChannelDetails;
  error?: unknown;
  expiresAt: number;
}

interface Consumer {
  resolve: (details: ChannelDetails) => void;
  reject: (error: unknown) => void;
  detach: () => void;
}

interface Request {
  channelId: string;
  load: () => Promise<ChannelDetails>;
  background: boolean;
  started: boolean;
  consumers: Set<Consumer>;
}

const cache = new Map<string, CachedDetails>();
const requests = new Map<string, Request>();
const queue: Request[] = [];
let active = 0;

function readCache(channelId: string): CachedDetails | undefined {
  const entry = cache.get(channelId);
  if (!entry) return undefined;
  cache.delete(channelId);
  if (entry.expiresAt <= Date.now()) return undefined;
  cache.set(channelId, entry);
  return entry;
}

export function getCachedChannelDetails(channelId: string): ChannelDetails | undefined {
  return readCache(channelId)?.details;
}

function storeCache(channelId: string, entry: CachedDetails) {
  cache.delete(channelId);
  cache.set(channelId, entry);
  while (cache.size > MAX_ENTRIES) {
    cache.delete(cache.keys().next().value!);
  }
}

function abortError(): DOMException {
  return new DOMException('Channel metadata request cancelled', 'AbortError');
}

function removeQueued(request: Request, error: unknown) {
  const index = queue.indexOf(request);
  if (index >= 0) queue.splice(index, 1);
  requests.delete(request.channelId);
  for (const consumer of request.consumers) {
    consumer.detach();
    consumer.reject(error);
  }
  request.consumers.clear();
}

function drainQueue() {
  while (active < MAX_CONCURRENT && queue.length > 0) {
    const foregroundIndex = queue.findIndex((request) => !request.background);
    const request = queue.splice(foregroundIndex < 0 ? 0 : foregroundIndex, 1)[0]!;
    request.started = true;
    active += 1;
    const finish = (entry: CachedDetails) => {
      storeCache(request.channelId, entry);
      requests.delete(request.channelId);
      active -= 1;
      for (const consumer of request.consumers) {
        consumer.detach();
        if (entry.details) consumer.resolve(entry.details);
        else consumer.reject(entry.error);
      }
      request.consumers.clear();
      drainQueue();
    };
    void Promise.resolve().then(request.load).then(
      (details) => finish({ details, expiresAt: Date.now() + CACHE_TTL_MS }),
      (error: unknown) => finish({ error, expiresAt: Date.now() + FAILURE_TTL_MS }),
    );
  }
}

export function requestChannelDetails(
  channelId: string,
  load: () => Promise<ChannelDetails>,
  { signal, background = false }: ChannelDetailsOptions = {},
): Promise<ChannelDetails> {
  if (signal?.aborted) return Promise.reject(abortError());
  const cached = readCache(channelId);
  if (cached && (cached.details || background)) {
    return cached.details ? Promise.resolve(cached.details) : Promise.reject(cached.error);
  }

  let request = requests.get(channelId);
  if (!request) {
    if (queue.length >= MAX_QUEUED) {
      let optionalIndex = -1;
      if (!background) {
        for (let index = queue.length - 1; index >= 0; index -= 1) {
          if (queue[index]!.background) {
            optionalIndex = index;
            break;
          }
        }
      }
      if (optionalIndex < 0) return Promise.reject(new Error('Channel metadata queue is full'));
      removeQueued(queue[optionalIndex]!, abortError());
    }
    request = { channelId, load, background, started: false, consumers: new Set() };
    requests.set(channelId, request);
    queue.push(request);
  } else if (!background) {
    request.background = false;
  }

  const shared = request;
  const result = new Promise<ChannelDetails>((resolve, reject) => {
    const onAbort = () => {
      shared.consumers.delete(consumer);
      consumer.detach();
      reject(abortError());
      if (!shared.started && shared.consumers.size === 0) removeQueued(shared, abortError());
    };
    const consumer: Consumer = {
      resolve,
      reject,
      detach: () => signal?.removeEventListener('abort', onAbort),
    };
    shared.consumers.add(consumer);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
  drainQueue();
  return result;
}
