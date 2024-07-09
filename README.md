# Crypto SQLite Pipeline

A command-line tool written in TypeScript/Node.js for collecting and storing cryptocurrency OHLCV data in SQLite.  
It retrieves closed candles from the Binance REST API, stores them transactionally, and computes a set of common technical indicators for analysis and trading.


The pipeline is designed to serve as the **single source of truth** for both backtesting and live trading bots, prioritising **data integrity and atomicity**.

---

## Features

- Deterministic historical backfill and incremental updates
- ACID-compliant writes with SQLite transactions (`--dry-run` supported)
- Rate-limit aware Binance client with exponential backoff and jitter
- Configurable symbols, intervals, and rate limits
- Computation of technical indicators:
  - EMA(50), EMA(200)
  - Wilder RSI(14), ATR(14), ADX(14)
  - MACD(12,26,9)
  - Bollinger Bands (SMA20, 2σ)
  - Volume SMA20
  - Percentage/log returns
- Verification utilities:
  - SQLite `PRAGMA integrity_check`
  - Gap detection in series continuity
  - NaN/null checks
  - Cross-validation by recomputing indicators over random slices
- CLI commands for `bootstrap`, `update`, `verify`, `query`
- JSON structured logging

---

## Installation

### Prerequisites

- **Ubuntu (or Linux equivalent)**
- **Node.js v18.17.0+**
- **npm** (comes with Node.js)
- **SQLite3 CLI tools** (optional for debugging)

```bash
sudo apt-get update
sudo apt-get install -y build-essential sqlite3
```

### Setup

```bash
# Unpack the project
unzip crypto-sqlite-pipeline.zip
cd crypto-sqlite-pipeline

# Install dependencies (locked to package-lock.json)
npm ci

# Build the TypeScript sources
npm run build
```

---

## Configuration

Configuration is stored in `config/default.json`:

```json
{
  "dbPath": "./market.sqlite",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "intervals": ["1m", "15m", "1h"],
  "bootstrap": { "startDate": "2018-01-01T00:00:00Z" },
  "rateLimit": {
    "requestsPerMinute": 600,
    "maxConcurrent": 4,
    "retry": {
      "baseMs": 500,
      "maxMs": 60000,
      "maxRetries": 8
    }
  },
  "http": { "timeoutMs": 15000 },
  "logLevel": "info"
}
```

- `dbPath` – SQLite database file path
- `symbols` – Binance symbols to track
- `intervals` – Kline intervals (must exist in `src/utils.ts` map)
- `bootstrap.startDate` – ISO UTC start date for historical backfill
- `rateLimit` – Scheduler and retry controls
- `http.timeoutMs` – Request timeout in ms
- `logLevel` – `debug`, `info`, `warn`, or `error`

---

## CLI Usage

### Bootstrap (historical backfill)

```bash
node dist/index.js bootstrap --dry-run=false
```

Downloads all historical closed candles from Binance, computes indicators, and writes them transactionally.

- Use `--dry-run` to validate logic without committing.

### Update (incremental)

```bash
node dist/index.js update
```

Fetches new closed candles, handles gaps, and recomputes indicators on an overlap window.

### Verify (integrity checks)

```bash
node dist/index.js verify
```

- Runs `PRAGMA integrity_check`
- Detects missing timestamps/gaps
- Reports null/NaN values
- Cross-validates indicator computations

### Query (preview utility)

```bash
node dist/index.js query --symbol BTCUSDT --interval 1h --limit 20
```

Prints the most recent candles and indicators as JSON.

---

## Testing

Unit tests are included (Jest). Example:

```bash
npm test
```

Tests cover indicator calculations and basic pipeline behaviour.

---

## Scheduling Updates

### Cron (simple)

Run incremental updates every 5 minutes:

```bash
*/5 * * * * /usr/bin/node /path/to/crypto-sqlite-pipeline/dist/index.js update >> /var/log/cryptopipe.log 2>&1
```

### Systemd Timer (recommended)

Create `/etc/systemd/system/cryptopipe.service`:

```ini
[Unit]
Description=Crypto SQLite Pipeline Update
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/opt/crypto-sqlite-pipeline
ExecStart=/usr/bin/node /opt/crypto-sqlite-pipeline/dist/index.js update
User=crypto
Group=crypto
Nice=10
```

Then `/etc/systemd/system/cryptopipe.timer`:

```ini
[Unit]
Description=Run cryptopipe update every 5 minutes

[Timer]
OnCalendar=*:0/5
Persistent=true
Unit=cryptopipe.service

[Install]
WantedBy=timers.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cryptopipe.timer
```

---

## Safe Database Reads

Consumers (e.g., bots) should:

- Only read **closed** candles: `nowClosed = floor(now/interval)-1`
- Always query `candles` joined with `indicators` on `(series_id, open_time)`
- Use `ORDER BY open_time ASC` for deterministic time series

Example SQL:

```sql
SELECT c.*, ind.*
FROM candles c
JOIN series s ON s.id = c.series_id
JOIN symbols sy ON sy.id = s.symbol_id
JOIN intervals it ON it.id = s.interval_id
LEFT JOIN indicators ind ON ind.series_id = c.series_id AND ind.open_time = c.open_time
WHERE sy.symbol = 'BTCUSDT' AND it.code = '1h' AND c.open_time <= :nowClosed
ORDER BY c.open_time ASC;
```

---

## Schema Overview

- **symbols** – list of assets (`BTCUSDT`, …)
- **intervals** – list of intervals (`1m`, `15m`, …)
- **series** – unique `(symbol, interval)` pairs
- **candles** – OHLCV data keyed by `(series_id, open_time)`
- **indicators** – technical indicators keyed by `(series_id, open_time)`
- **series_state** – last updated timestamp for each series

Indexes:

- `candles(series_id, open_time)`
- `indicators(series_id, open_time)`

---

## Logging

Logs are single-line JSON for easy parsing:

```json
{"ts":"2025-09-15T12:00:00.000Z","level":"info","msg":"Update complete","symbol":"BTCUSDT","interval":"1h"}
```

---

## Notes

- Only closed candles are processed to avoid lookahead bias.
- Writes are idempotent (UPSERT on `(series_id, open_time)`).
- Indicators are recomputed with overlap to ensure continuity.
- Entire system is configuration-driven; no schema changes required when adding assets or intervals.

---

## License

Internal use only. No external distribution.
