export const INTERVAL_MS: Record<string, number> = {
  "1m": 60_000,
  "3m": 180_000,
  "5m": 300_000,
  "15m": 900_000,
  "30m": 1_800_000,
  "1h": 3_600_000,
  "2h": 7_200_000,
  "4h": 14_400_000,
  "6h": 21_600_000,
  "8h": 28_800_000,
  "12h": 43_200_000,
  "1d": 86_400_000,
  "3d": 259_200_000,
  "1w": 604_800_000
};

export function floorToInterval(ms: number, intervalMs: number): number {
  if (intervalMs <= 0) { throw new Error("intervalMs must be positive"); }
  return Math.floor(ms / intervalMs) * intervalMs;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => { setTimeout(resolve, ms); });
}

export function clamp(n: number, min: number, max: number): number {
  if (n < min) { return min; }
  if (n > max) { return max; }
  return n;
}
