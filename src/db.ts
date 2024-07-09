import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import { Logger } from "./logger";

export interface DB {
  conn: Database.Database;
  close(): void;
  tx<T>(fn: () => T, dryRun?: boolean): T;
}

export function openDatabase(dbPath: string): DB {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const conn = new Database(dbPath);
  conn.pragma("journal_mode = WAL");
  conn.pragma("synchronous = NORMAL");
  conn.pragma("foreign_keys = ON");
  conn.pragma("busy_timeout = 5000");

  // Schema
  conn.exec(`
  CREATE TABLE IF NOT EXISTS symbols(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS intervals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    ms INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS series(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    interval_id INTEGER NOT NULL,
    UNIQUE(symbol_id, interval_id),
    FOREIGN KEY(symbol_id) REFERENCES symbols(id),
    FOREIGN KEY(interval_id) REFERENCES intervals(id)
  );
  CREATE TABLE IF NOT EXISTS candles(
    series_id INTEGER NOT NULL,
    open_time INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    quote_asset_volume REAL NOT NULL,
    trades INTEGER NOT NULL,
    taker_buy_base_volume REAL NOT NULL,
    taker_buy_quote_volume REAL NOT NULL,
    PRIMARY KEY(series_id, open_time),
    FOREIGN KEY(series_id) REFERENCES series(id)
  );
  CREATE TABLE IF NOT EXISTS indicators(
    series_id INTEGER NOT NULL,
    open_time INTEGER NOT NULL,
    ema50 REAL,
    ema200 REAL,
    rsi14 REAL,
    atr14 REAL,
    adx14 REAL,
    vol_ma20 REAL,
    macd REAL,
    macd_signal REAL,
    macd_hist REAL,
    bb_sma20 REAL,
    bb_upper REAL,
    bb_lower REAL,
    pct_return_1 REAL,
    log_return_1 REAL,
    PRIMARY KEY(series_id, open_time),
    FOREIGN KEY(series_id) REFERENCES series(id)
  );
  CREATE TABLE IF NOT EXISTS series_state(
    series_id INTEGER PRIMARY KEY,
    last_open_time INTEGER,
    last_updated_at INTEGER NOT NULL,
    FOREIGN KEY(series_id) REFERENCES series(id)
  );
  -- Optional registry for truly missing-market windows (pre-listing, exchange outages)
  CREATE TABLE IF NOT EXISTS known_gaps(
    series_id INTEGER NOT NULL,
    start_open_time INTEGER NOT NULL,
    end_open_time INTEGER NOT NULL,
    PRIMARY KEY(series_id, start_open_time, end_open_time),
    FOREIGN KEY(series_id) REFERENCES series(id)
  );

  CREATE INDEX IF NOT EXISTS idx_candles_series_time ON candles(series_id, open_time);
  CREATE INDEX IF NOT EXISTS idx_indicators_series_time ON indicators(series_id, open_time);
  CREATE INDEX IF NOT EXISTS idx_known_gaps_series ON known_gaps(series_id, start_open_time, end_open_time);
  `);

  const db: DB = {
    conn,
    close(): void { conn.close(); },
    tx<T>(fn: () => T, dryRun?: boolean): T {
      conn.exec("BEGIN IMMEDIATE");
      try {
        const result = fn();
        if (dryRun === true) {
          conn.exec("ROLLBACK");
          Logger.info("Transaction rolled back (--dry-run=true)");
        } else {
          conn.exec("COMMIT");
        }
        return result;
      } catch (err) {
        conn.exec("ROLLBACK");
        throw err;
      }
    }
  };
  return db;
}

// Metadata upserts
export function ensureSymbol(db: DB, symbol: string, base: string, quote: string): number {
  const up = db.conn.prepare(`
    INSERT INTO symbols(symbol, base_asset, quote_asset)
    VALUES(@symbol,@base,@quote)
    ON CONFLICT(symbol) DO UPDATE SET base_asset=excluded.base_asset, quote_asset=excluded.quote_asset
  `);
  up.run({ symbol, base, quote });
  const row = db.conn.prepare("SELECT id FROM symbols WHERE symbol = ?").get(symbol) as { id: number };
  return row.id;
}

export function ensureInterval(db: DB, code: string, ms: number): number {
  const up = db.conn.prepare(`
    INSERT INTO intervals(code, ms)
    VALUES(@code,@ms)
    ON CONFLICT(code) DO UPDATE SET ms=excluded.ms
  `);
  up.run({ code, ms });
  const row = db.conn.prepare("SELECT id FROM intervals WHERE code = ?").get(code) as { id: number };
  return row.id;
}

export function ensureSeries(db: DB, symbolId: number, intervalId: number): number {
  const up = db.conn.prepare(`
    INSERT INTO series(symbol_id, interval_id)
    VALUES(@symbolId,@intervalId)
    ON CONFLICT(symbol_id, interval_id) DO NOTHING
  `);
  up.run({ symbolId, intervalId });
  const row = db.conn.prepare("SELECT id FROM series WHERE symbol_id = ? AND interval_id = ?").get(symbolId, intervalId) as { id: number };
  return row.id;
}

// I/O helpers
export interface CandleRow {
  series_id: number;
  open_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  quote_asset_volume: number;
  trades: number;
  taker_buy_base_volume: number;
  taker_buy_quote_volume: number;
}

export function upsertCandles(db: DB, rows: CandleRow[]): void {
  const stmt = db.conn.prepare(`
    INSERT INTO candles(series_id, open_time, open, high, low, close, volume, quote_asset_volume, trades, taker_buy_base_volume, taker_buy_quote_volume)
    VALUES(@series_id,@open_time,@open,@high,@low,@close,@volume,@quote_asset_volume,@trades,@taker_buy_base_volume,@taker_buy_quote_volume)
    ON CONFLICT(series_id, open_time) DO UPDATE SET
      open=excluded.open,
      high=excluded.high,
      low=excluded.low,
      close=excluded.close,
      volume=excluded.volume,
      quote_asset_volume=excluded.quote_asset_volume,
      trades=excluded.trades,
      taker_buy_base_volume=excluded.taker_buy_base_volume,
      taker_buy_quote_volume=excluded.taker_buy_quote_volume
  `);
  const insertMany = db.conn.transaction((batch: CandleRow[]) => {
    for (const r of batch) { stmt.run(r); }
  });
  insertMany(rows);
}

export interface IndicatorRow {
  series_id: number;
  open_time: number;
  ema50?: number | null;
  ema200?: number | null;
  rsi14?: number | null;
  atr14?: number | null;
  adx14?: number | null;
  vol_ma20?: number | null;
  macd?: number | null;
  macd_signal?: number | null;
  macd_hist?: number | null;
  bb_sma20?: number | null;
  bb_upper?: number | null;
  bb_lower?: number | null;
  pct_return_1?: number | null;
  log_return_1?: number | null;
}

export function upsertIndicators(db: DB, rows: IndicatorRow[]): void {
  const stmt = db.conn.prepare(`
    INSERT INTO indicators(series_id, open_time, ema50, ema200, rsi14, atr14, adx14, vol_ma20, macd, macd_signal, macd_hist, bb_sma20, bb_upper, bb_lower, pct_return_1, log_return_1)
    VALUES(@series_id,@open_time,@ema50,@ema200,@rsi14,@atr14,@adx14,@vol_ma20,@macd,@macd_signal,@macd_hist,@bb_sma20,@bb_upper,@bb_lower,@pct_return_1,@log_return_1)
    ON CONFLICT(series_id, open_time) DO UPDATE SET
      ema50=excluded.ema50,
      ema200=excluded.ema200,
      rsi14=excluded.rsi14,
      atr14=excluded.atr14,
      adx14=excluded.adx14,
      vol_ma20=excluded.vol_ma20,
      macd=excluded.macd,
      macd_signal=excluded.macd_signal,
      macd_hist=excluded.macd_hist,
      bb_sma20=excluded.bb_sma20,
      bb_upper=excluded.bb_upper,
      bb_lower=excluded.bb_lower,
      pct_return_1=excluded.pct_return_1,
      log_return_1=excluded.log_return_1
  `);
  const insertMany = db.conn.transaction((batch: IndicatorRow[]) => {
    for (const r of batch) { stmt.run(r); }
  });
  insertMany(rows);
}

export function getSeriesId(db: DB, symbol: string, interval: string): number | null {
  const row = db.conn.prepare(`
    SELECT s.id AS series_id FROM series s
    JOIN symbols sy ON sy.id = s.symbol_id
    JOIN intervals i ON i.id = s.interval_id
    WHERE sy.symbol = ? AND i.code = ?
  `).get(symbol, interval) as { series_id: number } | undefined;
  if (!row) { return null; }
  return row.series_id;
}

export function getMaxOpenTime(db: DB, seriesId: number): number | null {
  const row = db.conn.prepare("SELECT MAX(open_time) AS max_ot FROM candles WHERE series_id = ?").get(seriesId) as { max_ot: number | null };
  return row.max_ot;
}

export function deleteRange(db: DB, seriesId: number, fromInclusive: number, toInclusive: number): void {
  const delInd = db.conn.prepare("DELETE FROM indicators WHERE series_id = ? AND open_time BETWEEN ? AND ?");
  const delCan = db.conn.prepare("DELETE FROM candles WHERE series_id = ? AND open_time BETWEEN ? AND ?");
  db.conn.transaction(() => {
    delInd.run(seriesId, fromInclusive, toInclusive);
    delCan.run(seriesId, fromInclusive, toInclusive);
  })();
}
