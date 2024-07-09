import { openDatabase, ensureSymbol, ensureInterval, ensureSeries, upsertCandles, upsertIndicators, getMaxOpenTime } from "./db";
import { Logger } from "./logger";
import { INTERVAL_MS, floorToInterval } from "./utils";
import { BinanceClient } from "./binance";
import { computeIndicators, OHLCV } from "./indicators";
import { AppConfig } from "./config";

const MAX_API_LIMIT = 1000;
const OVERLAP_BARS = 600; // robust warm-up for long EMAs/ADX

function mapToRows(seriesId: number, kl: { openTime: number; open: number; high: number; low: number; close: number; volume: number; quoteAssetVolume: number; numberOfTrades: number; takerBuyBaseVolume: number; takerBuyQuoteVolume: number; }[]) {
  return kl.map(k => ({
    series_id: seriesId,
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
}

function mapToOHLCV(kl: { openTime: number; open: number; high: number; low: number; close: number; volume: number; }[]): OHLCV[] {
  return kl.map(k => ({ time: k.openTime, open: k.open, high: k.high, low: k.low, close: k.close, volume: k.volume }));
}

export async function bootstrapSeries(cfg: AppConfig, symbol: string, interval: string, dryRun: boolean): Promise<void> {
  const db = openDatabase(cfg.dbPath);
  try {
    const client = new BinanceClient(cfg.rateLimit as any, cfg.http.timeoutMs);
    const symbolId = ensureSymbol(db, symbol, symbol.replace(/USDT$/, ""), "USDT");
    const intervalId = ensureInterval(db, interval, INTERVAL_MS[interval]);
    const seriesId = ensureSeries(db, symbolId, intervalId);
    const intervalMs = INTERVAL_MS[interval];
    const start = Date.parse(cfg.bootstrap.startDate);
    const endClosed = floorToInterval(Date.now(), intervalMs) - 1;

    let cursor = start;

    while (cursor <= endClosed) {
      const fetchEnd = Math.min(endClosed, cursor + intervalMs * (MAX_API_LIMIT - 1));
      const overlapStart = Math.max(start, cursor - OVERLAP_BARS * intervalMs);

      Logger.info("Bootstrap fetch", { symbol, interval, overlapStart, fetchEnd });
      const klines = await client.getKlines(symbol, interval, overlapStart, fetchEnd, MAX_API_LIMIT);

      if (klines.length === 0) {
        // advance minimally to avoid a stall
        cursor = cursor + intervalMs;
        continue;
      }

      // Compute indicators over overlap+chunk, write only >= cursor
      const ohlcvAll = mapToOHLCV(klines);
      const indAll = computeIndicators(ohlcvAll);

      const rowsAll = mapToRows(seriesId, klines);
      const writeIndices: number[] = [];
      for (let i = 0; i < rowsAll.length; i++) {
        if (rowsAll[i].open_time >= cursor) {
          writeIndices.push(i);
        }
      }

      const candleWrite = writeIndices.map(i => rowsAll[i]);
      const indicWrite = writeIndices.map(i => ({
        series_id: seriesId,
        open_time: ohlcvAll[i].time,
        ema50: indAll.ema50[i],
        ema200: indAll.ema200[i],
        rsi14: indAll.rsi14[i],
        atr14: indAll.atr14[i],
        adx14: indAll.adx14[i],
        vol_ma20: indAll.vol_ma20[i],
        macd: indAll.macd[i],
        macd_signal: indAll.macd_signal[i],
        macd_hist: indAll.macd_hist[i],
        bb_sma20: indAll.bb_sma20[i],
        bb_upper: indAll.bb_upper[i],
        bb_lower: indAll.bb_lower[i],
        pct_return_1: indAll.pct_return_1[i],
        log_return_1: indAll.log_return_1[i]
      }));

      // Transactional write
      db.tx(() => {
        if (candleWrite.length > 0) {
          upsertCandles(db, candleWrite);
          upsertIndicators(db, indicWrite);
        }
      }, dryRun);

      // Dynamic advance by actual closed bars written
      const advancedBars = candleWrite.length;
      if (advancedBars > 0) {
        cursor = cursor + advancedBars * intervalMs;
      } else {
        cursor = cursor + intervalMs;
      }
    }
  } finally {
    db.close();
  }
}

export async function updateSeries(cfg: AppConfig, symbol: string, interval: string, dryRun: boolean): Promise<void> {
  const db = openDatabase(cfg.dbPath);
  try {
    const client = new BinanceClient(cfg.rateLimit as any, cfg.http.timeoutMs);
    const symbolId = ensureSymbol(db, symbol, symbol.replace(/USDT$/, ""), "USDT");
    const intervalId = ensureInterval(db, interval, INTERVAL_MS[interval]);
    const seriesId = ensureSeries(db, symbolId, intervalId);
    const intervalMs = INTERVAL_MS[interval];

    const maxOpen = getMaxOpenTime(db, seriesId);
    const nowClosed = floorToInterval(Date.now(), intervalMs) - 1;

    const start = maxOpen === null
      ? Date.parse(cfg.bootstrap.startDate)
      : Math.max(Date.parse(cfg.bootstrap.startDate), maxOpen - OVERLAP_BARS * intervalMs);

    let cursor = start;

    while (cursor <= nowClosed) {
      const fetchEnd = Math.min(nowClosed, cursor + intervalMs * (MAX_API_LIMIT - 1));
      const overlapStart = Math.max(start, cursor - OVERLAP_BARS * intervalMs);

      Logger.info("Update fetch", { symbol, interval, overlapStart, fetchEnd });
      const klines = await client.getKlines(symbol, interval, overlapStart, fetchEnd, MAX_API_LIMIT);

      if (klines.length === 0) {
        cursor = cursor + intervalMs;
        continue;
      }

      const ohlcvAll = mapToOHLCV(klines);
      const indAll = computeIndicators(ohlcvAll);
      const rowsAll = mapToRows(seriesId, klines);

      const writeIndices: number[] = [];
      for (let i = 0; i < rowsAll.length; i++) {
        if (rowsAll[i].open_time >= cursor) {
          writeIndices.push(i);
        }
      }

      const candleWrite = writeIndices.map(i => rowsAll[i]);
      const indicWrite = writeIndices.map(i => ({
        series_id: seriesId,
        open_time: ohlcvAll[i].time,
        ema50: indAll.ema50[i],
        ema200: indAll.ema200[i],
        rsi14: indAll.rsi14[i],
        atr14: indAll.atr14[i],
        adx14: indAll.adx14[i],
        vol_ma20: indAll.vol_ma20[i],
        macd: indAll.macd[i],
        macd_signal: indAll.macd_signal[i],
        macd_hist: indAll.macd_hist[i],
        bb_sma20: indAll.bb_sma20[i],
        bb_upper: indAll.bb_upper[i],
        bb_lower: indAll.bb_lower[i],
        pct_return_1: indAll.pct_return_1[i],
        log_return_1: indAll.log_return_1[i]
      }));

      db.tx(() => {
        if (candleWrite.length > 0) {
          upsertCandles(db, candleWrite);
          upsertIndicators(db, indicWrite);
        }
      }, dryRun);

      const advancedBars = candleWrite.length;
      if (advancedBars > 0) {
        cursor = cursor + advancedBars * intervalMs;
      } else {
        cursor = cursor + intervalMs;
      }
    }
  } finally {
    // Close DB
    db.close();
  }
}
