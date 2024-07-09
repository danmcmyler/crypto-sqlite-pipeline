import { openDatabase } from "./db";
import { Logger } from "./logger";

export function runQuery(dbPath: string, symbol: string, interval: string, limit: number): void {
  const db = openDatabase(dbPath);
  try {
    const rows = db.conn.prepare(`
      SELECT sy.symbol, i.code AS interval, c.open_time, c.open, c.high, c.low, c.close, c.volume,
             ind.ema50, ind.ema200, ind.rsi14, ind.atr14, ind.adx14, ind.macd, ind.macd_signal, ind.macd_hist
      FROM candles c
      JOIN series s ON s.id = c.series_id
      JOIN symbols sy ON sy.id = s.symbol_id
      JOIN intervals i ON i.id = s.interval_id
      LEFT JOIN indicators ind ON ind.series_id = c.series_id AND ind.open_time = c.open_time
      WHERE sy.symbol = ? AND i.code = ?
      ORDER BY c.open_time DESC
      LIMIT ?
    `).all(symbol, interval, limit);
    Logger.info("Query result", { count: (rows as any[]).length });
    for (const r of rows as any[]) {
      process.stdout.write(JSON.stringify(r) + "\n");
    }
  } finally {
    db.close();
  }
}
