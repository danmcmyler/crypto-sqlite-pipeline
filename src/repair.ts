import { AppConfig } from "./config";
import { openDatabase, upsertCandles, upsertIndicators } from "./db";
import { Logger } from "./logger";
import { BinanceClient } from "./binance";
import { computeIndicators } from "./indicators";

type SeriesMeta = { id: number; symbol: string; interval: string; intervalMs: number };

const MAX_API_LIMIT = 1000;
const OVERLAP_BARS = 600;
const IGNORE_NULL_WARMUP_BARS = 200;
const MIN_GAP_BARS_TO_REPAIR = 1;

export async function runRepair(cfg: AppConfig): Promise<void> {
  const db = openDatabase(cfg.dbPath);
  const client = new BinanceClient(cfg.rateLimit as any, cfg.http.timeoutMs);
  try {
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

      // Continuity
      const times = db.conn.prepare(`SELECT open_time FROM candles WHERE series_id = ? ORDER BY open_time ASC`).all(meta.id) as Array<{ open_time: number }>;
      if (times.length === 0) {
        Logger.info("repair_skip_series_empty", { series: label });
        continue;
      }
      const gaps = findGaps(times.map(t => t.open_time), meta.intervalMs);
      if (gaps.length > 0) {
        Logger.info("repair_gaps_begin", { series: label, count: gaps.length });
        await repairGaps(db, client, meta, gaps);
      }

      // Null-indicator spans (ignore first warm-up)
      const first = times[0].open_time;
      const warmupCut = first + IGNORE_NULL_WARMUP_BARS * meta.intervalMs;
      const nullRows = db.conn.prepare(`
        SELECT open_time FROM indicators
        WHERE series_id = ? AND open_time > ? AND
              ema50 IS NULL AND ema200 IS NULL AND rsi14 IS NULL AND atr14 IS NULL AND adx14 IS NULL AND
              vol_ma20 IS NULL AND macd IS NULL AND macd_signal IS NULL AND macd_hist IS NULL AND
              bb_sma20 IS NULL AND bb_upper IS NULL AND bb_lower IS NULL AND pct_return_1 IS NULL AND log_return_1 IS NULL
        ORDER BY open_time ASC
      `).all(meta.id, warmupCut) as Array<{ open_time: number }>;
      const nullSpans = mergeContiguous(nullRows.map(r => r.open_time), meta.intervalMs);
      if (nullSpans.length > 0) {
        Logger.info("repair_nulls_begin", { series: label, spans: nullSpans.length });
        await repairNullSpans(db, client, meta, nullSpans);
      }

      // Post summary
      const times2 = db.conn.prepare(`SELECT open_time FROM candles WHERE series_id = ? ORDER BY open_time ASC`).all(meta.id) as Array<{ open_time: number }>;
      const gaps2 = findGaps(times2.map(t => t.open_time), meta.intervalMs);
      const nullRows2 = db.conn.prepare(`
        SELECT COUNT(*) AS c FROM indicators
        WHERE series_id = ? AND open_time > ? AND
              ema50 IS NULL AND ema200 IS NULL AND rsi14 IS NULL AND atr14 IS NULL AND adx14 IS NULL AND
              vol_ma20 IS NULL AND macd IS NULL AND macd_signal IS NULL AND macd_hist IS NULL AND
              bb_sma20 IS NULL AND bb_upper IS NULL AND bb_lower IS NULL AND pct_return_1 IS NULL AND log_return_1 IS NULL
      `).get(meta.id, warmupCut) as { c: number };

      Logger.info("repair_summary", { series: label, gaps_remaining: gaps2.length, null_indicator_rows_remaining: nullRows2.c });
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
      if (missingBars >= MIN_GAP_BARS_TO_REPAIR) {
        const startMissing = prev + step;
        const endMissing = next - step;
        out.push({ startMissing, endMissing, missingBars });
      }
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

async function repairGaps(
  db: ReturnType<typeof openDatabase>,
  client: BinanceClient,
  meta: SeriesMeta,
  gaps: Array<{ startMissing: number; endMissing: number; missingBars: number }>
): Promise<void> {
  for (const g of gaps) {
    const from = g.startMissing - OVERLAP_BARS * meta.intervalMs;
    const to = g.endMissing + OVERLAP_BARS * meta.intervalMs;
    Logger.info("repair_gap_window", {
      series: `${meta.symbol}-${meta.interval}`,
      from: new Date(from).toISOString(),
      to: new Date(to).toISOString(),
      missing_bars: g.missingBars
    });

    let cursor = from;
    while (cursor <= to) {
      const fetchEnd = Math.min(to, cursor + meta.intervalMs * (MAX_API_LIMIT - 1));
      const klines = await client.getKlines(meta.symbol, meta.interval, cursor, fetchEnd, MAX_API_LIMIT);
      if (klines.length === 0) { cursor += meta.intervalMs; continue; }

      const ohlcv = klines.map(k => ({ time: k.openTime, open: k.open, high: k.high, low: k.low, close: k.close, volume: k.volume }));
      const ind = computeIndicators(ohlcv);

      const candleRows = klines.map(k => ({
        series_id: meta.id,
        open_time: k.openTime,
        open: k.open,
        high: k.high,
        low: k.low,
        close: k.close,
        volume: k.volume,
        quote_asset_volume: k.quoteAssetVolume,
        trades: k.numberOfTrades,
        taker_buy_base_volume: k.takerBuyBaseVolume,
        taker_buy_quote_volume: k.takerBuyQuoteVolume
      }));

      const candleWrite = candleRows.filter(r => r.open_time >= g.startMissing && r.open_time <= g.endMissing);
      const indicWrite = ohlcv.map((row, i) => ({
        series_id: meta.id,
        open_time: row.time,
        ema50: ind.ema50[i],
        ema200: ind.ema200[i],
        rsi14: ind.rsi14[i],
        atr14: ind.atr14[i],
        adx14: ind.adx14[i],
        vol_ma20: ind.vol_ma20[i],
        macd: ind.macd[i],
        macd_signal: ind.macd_signal[i],
        macd_hist: ind.macd_hist[i],
        bb_sma20: ind.bb_sma20[i],
        bb_upper: ind.bb_upper[i],
        bb_lower: ind.bb_lower[i],
        pct_return_1: ind.pct_return_1[i],
        log_return_1: ind.log_return_1[i]
      }));

      if (candleWrite.length > 0 || indicWrite.length > 0) {
        db.tx(() => {
          if (candleWrite.length > 0) { upsertCandles(db, candleWrite); }
          if (indicWrite.length > 0) { upsertIndicators(db, indicWrite); }
        });
      }

      cursor += Math.max(1, klines.length) * meta.intervalMs;
    }
  }
}

async function repairNullSpans(
  db: ReturnType<typeof openDatabase>,
  client: BinanceClient,
  meta: SeriesMeta,
  spans: Array<{ start: number; end: number }>
): Promise<void> {
  for (const s of spans) {
    const from = s.start - OVERLAP_BARS * meta.intervalMs;
    const to = s.end + OVERLAP_BARS * meta.intervalMs;
    Logger.info("repair_null_window", {
      series: `${meta.symbol}-${meta.interval}`,
      from: new Date(from).toISOString(),
      to: new Date(to).toISOString()
    });

    let cursor = from;
    while (cursor <= to) {
      const fetchEnd = Math.min(to, cursor + meta.intervalMs * (MAX_API_LIMIT - 1));
      const klines = await client.getKlines(meta.symbol, meta.interval, cursor, fetchEnd, MAX_API_LIMIT);
      if (klines.length === 0) { cursor += meta.intervalMs; continue; }

      const ohlcv = klines.map(k => ({ time: k.openTime, open: k.open, high: k.high, low: k.low, close: k.close, volume: k.volume }));
      const ind = computeIndicators(ohlcv);

      const indicWrite = ohlcv.map((row, i) => ({
        series_id: meta.id,
        open_time: row.time,
        ema50: ind.ema50[i],
        ema200: ind.ema200[i],
        rsi14: ind.rsi14[i],
        atr14: ind.atr14[i],
        adx14: ind.adx14[i],
        vol_ma20: ind.vol_ma20[i],
        macd: ind.macd[i],
        macd_signal: ind.macd_signal[i],
        macd_hist: ind.macd_hist[i],
        bb_sma20: ind.bb_sma20[i],
        bb_upper: ind.bb_upper[i],
        bb_lower: ind.bb_lower[i],
        pct_return_1: ind.pct_return_1[i],
        log_return_1: ind.log_return_1[i]
      }));

      if (indicWrite.length > 0) {
        db.tx(() => { upsertIndicators(db, indicWrite); });
      }

      cursor += Math.max(1, klines.length) * meta.intervalMs;
    }
  }
}
