import { AppConfig } from "./config";
import { openDatabase } from "./db";
import { Logger } from "./logger";

type SeriesMeta = { id: number; symbol: string; interval: string; intervalMs: number };

const IGNORE_NULL_WARMUP_BARS = 200;

export async function runVerify(cfg: AppConfig): Promise<void> {
  const db = openDatabase(cfg.dbPath);
  try {
    const pragma = db.conn.prepare("PRAGMA integrity_check;").get() as any;
    Logger.info("integrity_check", { result: pragma.integrity_check });

    const metas = db.conn.prepare(`
      SELECT s.id AS id, sy.symbol AS symbol, i.code AS interval, i.ms AS ms
      FROM series s
      JOIN symbols sy ON sy.id = s.symbol_id
      JOIN intervals i ON i.id = s.interval_id
      ORDER BY sy.symbol, i.ms
    `).all() as Array<{ id: number; symbol: string; interval: string; ms: number }>;

    for (const m of metas) {
      const meta: SeriesMeta = { id: m.id, symbol: m.symbol, interval: m.interval, intervalMs: m.ms };
      const label = `${meta.symbol}-${meta.interval}`;

      const times = db.conn.prepare(`
        SELECT open_time FROM candles WHERE series_id = ? ORDER BY open_time ASC
      `).all(meta.id) as Array<{ open_time: number }>;
      if (times.length === 0) {
        Logger.warn("series_empty", { series: label });
        continue;
      }
      const first = times[0].open_time;
      const last = times[times.length - 1].open_time;

      const gaps = findGaps(times.map(t => t.open_time), meta.intervalMs);
      const warmupCut = first + IGNORE_NULL_WARMUP_BARS * meta.intervalMs;
      const nullRows = db.conn.prepare(`
        SELECT open_time FROM indicators
        WHERE series_id = ? AND open_time > ? AND
              ema50 IS NULL AND ema200 IS NULL AND rsi14 IS NULL AND atr14 IS NULL AND adx14 IS NULL AND
              vol_ma20 IS NULL AND macd IS NULL AND macd_signal IS NULL AND macd_hist IS NULL AND
              bb_sma20 IS NULL AND bb_upper IS NULL AND bb_lower IS NULL AND pct_return_1 IS NULL AND log_return_1 IS NULL
      `).all(meta.id, warmupCut) as Array<{ open_time: number }>;
      const nullSpans = mergeContiguous(nullRows.map(r => r.open_time), meta.intervalMs);

      Logger.info("series_summary", {
        series: label,
        window: { from: new Date(first).toISOString(), to: new Date(last).toISOString() },
        candles: { count: times.length },
        gaps: { count: gaps.length, sample: sampleGaps(gaps, meta.intervalMs) },
        null_indicator_spans: { count: nullSpans.length, sample: sampleSpans(nullSpans, meta.intervalMs) }
      });
    }
  } finally {
    db.close();
  }
}

function findGaps(sortedTimes: number[], step: number): Array<{ startMissing: number; endMissing: number; missingBars: number }> {
  const out: Array<{ startMissing: number; endMissing: number; missingBars: number }> = [];
  for (let i = 1; i < sortedTimes.length; i++) {
    const prev = sortedTimes[i - 1];
    const next = sortedTimes[i];
    const diff = next - prev;
    if (diff > step) {
      const missingBars = Math.floor(diff / step) - 1;
      const startMissing = prev + step;
      const endMissing = next - step;
      out.push({ startMissing, endMissing, missingBars });
    }
  }
  return out;
}

function mergeContiguous(times: number[], step: number): Array<{ start: number; end: number }> {
  if (times.length === 0) { return []; }
  const out: Array<{ start: number; end: number }> = [];
  let s = times[0];
  let p = times[0];
  for (let i = 1; i < times.length; i++) {
    const t = times[i];
    if (t === p + step) { p = t; } else { out.push({ start: s, end: p }); s = t; p = t; }
  }
  out.push({ start: s, end: p });
  return out;
}

function sampleGaps(
  gaps: Array<{ startMissing: number; endMissing: number; missingBars: number }>,
  step: number
) {
  return gaps.slice(0, 5).map(g => ({
    from: new Date(g.startMissing).toISOString(),
    to: new Date(g.endMissing).toISOString(),
    missing_bars: g.missingBars,
    approx_duration: msToHuman((g.endMissing - g.startMissing) + step)
  }));
}

function sampleSpans(spans: Array<{ start: number; end: number }>, step: number) {
  return spans.slice(0, 5).map(s => ({
    from: new Date(s.start).toISOString(),
    to: new Date(s.end).toISOString(),
    bars: ((s.end - s.start) / step) + 1,
    approx_duration: msToHuman((s.end - s.start) + step)
  }));
}

function msToHuman(ms: number): string {
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  return `${d}d ${h}h ${m}m`;
}
