import fs from "fs";
import path from "path";

export interface RetryCfg {
  baseMs: number;
  maxMs: number;
  maxRetries: number;
}

export interface RateLimitCfg {
  requestsPerMinute: number;
  maxConcurrent: number;
  retry: RetryCfg;
}

export interface HttpCfg {
  timeoutMs: number;
}

export interface AppConfig {
  dbPath: string;
  symbols: string[];
  intervals: string[];
  bootstrap: { startDate: string };
  rateLimit: RateLimitCfg;
  http: HttpCfg;
  logLevel: "debug" | "info" | "warn" | "error";
}

export function loadConfig(configPath?: string): AppConfig {
  const file = configPath || path.resolve(process.cwd(), "config", "default.json");
  const raw = fs.readFileSync(file, "utf8");
  const cfg = JSON.parse(raw) as AppConfig;
  return cfg;
}
