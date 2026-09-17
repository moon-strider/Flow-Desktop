import { invokeBackend } from "./errors";

export interface ImageProxyUrl {
  url: string;
  expiresAt: number;
}

export function proxyImageUrl(url: string): Promise<ImageProxyUrl> {
  return invokeBackend<ImageProxyUrl>("proxy_image_url", { url });
}
