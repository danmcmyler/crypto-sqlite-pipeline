import { Logger } from "./logger";
import { sleep, clamp } from "./utils";

export interface Kline {
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  closeTime: number;
  quoteAssetVolume: number;
  numberOfTrades: number;
  takerBuyBaseVolume: number;
  takerBuyQuoteVolume: number;
}

export interface RateLimitConfig {
  requestsPerMinute: number;
  maxConcurrent: number;
  retry: { baseMs: number; maxMs: number; maxRetries: number; };
}

class TokenBucket {
  private capacity: number;
  private tokens: number;
  private refillPerMs: number;
  private lastRefill: number;

  constructor(tokensPerMinute: number) {
    this.capacity = tokensPerMinute;
    this.tokens = tokensPerMinute;
    this.refillPerMs = tokensPerMinute / 60000;
    this.lastRefill = Date.now();
  }

  async take(): Promise<void> {
    while (true) {
      this.refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      await sleep(100);
    }
  }

  private refill(): void {
    const now = Date.now();
    const delta = now - this.lastRefill;
    if (delta <= 0) { return; }
    this.tokens = Math.min(this.capacity, this.tokens + delta * this.refillPerMs);
    this.lastRefill = now;
  }
}

export class RequestScheduler {
  private bucket: TokenBucket;
  private maxConcurrent: number;
  private active: number;

  constructor(cfg: RateLimitConfig) {
    this.bucket = new TokenBucket(cfg.requestsPerMinute);
    this.maxConcurrent = cfg.maxConcurrent;
    this.active = 0;
  }

  async schedule<T>(fn: () => Promise<T>): Promise<T> {
    while (this.active >= this.maxConcurrent) {
      await sleep(10);
    }
    await this.bucket.take();
    this.active += 1;
    try {
      const res = await fn();
      return res;
    } finally {
      this.active -= 1;
    }
  }
}

export class BinanceClient {
  private scheduler: RequestScheduler;
  private retry: { baseMs: number; maxMs: number; maxRetries: number; };
  private timeoutMs: number;

  constructor(rateLimit: RateLimitConfig, timeoutMs: number) {
    this.scheduler = new RequestScheduler(rateLimit);
    this.retry = rateLimit.retry;
    this.timeoutMs = timeoutMs;
  }

  async getKlines(symbol: string, interval: string, startTime?: number, endTime?: number, limit=1000): Promise<Kline[]> {
    const url = new URL("https://api.binance.com/api/v3/klines");
    url.searchParams.set("symbol", symbol);
    url.searchParams.set("interval", interval);
    if (startTime !== undefined) { url.searchParams.set("startTime", String(startTime)); }
    if (endTime !== undefined) { url.searchParams.set("endTime", String(endTime)); }
    if (limit !== undefined) { url.searchParams.set("limit", String(limit)); }

    const doReq = async (): Promise<Kline[]> => {
      const controller = new AbortController();
      const t = setTimeout(() => { controller.abort(); }, this.timeoutMs);
      try {
        const resp = await fetch(url.toString(), { signal: controller.signal, headers: { "Accept": "application/json" } });
        const retryAfter = resp.headers.get("Retry-After");
        if (resp.status === 418) {
          const wait = this.backoffWaitMs(0, retryAfter);
          Logger.warn("HTTP 418 received. IP banned temporarily. Backing off.", { wait });
          await sleep(wait);
          throw new Error("418");
        }
        if (resp.status === 429) {
          const wait = this.backoffWaitMs(0, retryAfter);
          Logger.warn("HTTP 429 received. Backing off.", { wait });
          await sleep(wait);
          throw new Error("429");
        }
        if (!resp.ok) {
          const body = await resp.text();
          throw new Error(`HTTP ${resp.status}: ${body}`);
        }
        const json = await resp.json();
        const out: Kline[] = (json as any[]).map(a => ({
          openTime: a[0],
          open: parseFloat(a[1]),
          high: parseFloat(a[2]),
          low: parseFloat(a[3]),
          close: parseFloat(a[4]),
          volume: parseFloat(a[5]),
          closeTime: a[6],
          quoteAssetVolume: parseFloat(a[7]),
          numberOfTrades: a[8],
          takerBuyBaseVolume: parseFloat(a[9]),
          takerBuyQuoteVolume: parseFloat(a[10])
        }));
        return out;
      } finally {
        clearTimeout(t);
      }
    };

    return await this.scheduler.schedule(async () => {
      let attempt = 0;
      while (true) {
        try {
          const res = await doReq();
          return res;
        } catch (err: any) {
          attempt++;
          if (attempt > this.retry.maxRetries) {
            throw err;
          }
          const wait = this.backoffWaitMs(attempt, null);
          Logger.warn("Request failed, retrying with backoff", { attempt, wait, message: String(err) });
          await sleep(wait);
        }
      }
    });
  }

  private backoffWaitMs(attempt: number, retryAfterHeader: string | null): number {
    if (retryAfterHeader) {
      const ra = Number(retryAfterHeader);
      if (!Number.isNaN(ra)) {
        return clamp(ra * 1000, this.retry.baseMs, this.retry.maxMs);
      }
    }
    const exp = Math.min(this.retry.maxMs, this.retry.baseMs * Math.pow(2, attempt));
    const jitter = Math.random() * exp * 0.5;
    return clamp(exp * 0.75 + jitter, this.retry.baseMs, this.retry.maxMs);
  }
}
