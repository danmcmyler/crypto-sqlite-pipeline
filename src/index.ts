#!/usr/bin/env node
import { Command } from "commander";
import { loadConfig } from "./config";
import { setLogLevel, Logger } from "./logger";
import { INTERVAL_MS } from "./utils";
import { bootstrapSeries, updateSeries } from "./pipeline";
import { runVerify } from "./verify";
import { runRepair } from "./repair";

const program = new Command();

program
  .name("cryptopipe")
  .description("Crypto SQLite market data pipeline")
  .version("1.0.4");

program.command("bootstrap")
  .description("Full historical backfill, transactional")
  .option("--config <path>", "Config file path")
  .option("--dry-run", "Run logic but rollback final transaction", false)
  .action(async (opts) => {
    const cfg = loadConfig(opts.config);
    setLogLevel(cfg.logLevel);
    for (const symbol of cfg.symbols) {
      for (const interval of cfg.intervals) {
        if (!INTERVAL_MS[interval]) { throw new Error(`Unsupported interval: ${interval}`); }
        await bootstrapSeries(cfg, symbol, interval, opts.dry_run === true || opts.dryRun === true);
      }
    }
    Logger.info("Bootstrap complete");
  });

program.command("update")
  .description("Incremental update with overlap recompute, transactional")
  .option("--config <path>", "Config file path")
  .option("--dry-run", "Run logic but rollback final transaction", false)
  .action(async (opts) => {
    const cfg = loadConfig(opts.config);
    setLogLevel(cfg.logLevel);
    for (const symbol of cfg.symbols) {
      for (const interval of cfg.intervals) {
        if (!INTERVAL_MS[interval]) { throw new Error(`Unsupported interval: ${interval}`); }
        await updateSeries(cfg, symbol, interval, opts.dry_run === true || opts.dryRun === true);
      }
    }
    Logger.info("Update complete");
  });

program.command("verify")
  .description("Read-only integrity report (human readable)")
  .option("--config <path>", "Config file path")
  .action(async (opts) => {
    const cfg = loadConfig(opts.config);
    setLogLevel(cfg.logLevel);
    await runVerify(cfg); // no writes
  });

program.command("repair")
  .description("Surgical auto-repair of gaps and null-indicator spans (no flags needed)")
  .option("--config <path>", "Config file path")
  .action(async (opts) => {
    const cfg = loadConfig(opts.config);
    setLogLevel(cfg.logLevel);
    await runRepair(cfg); // hands-off, writes transactionally
  });

program.command("query")
  .description("Preview data for a symbol/interval")
  .requiredOption("--symbol <symbol>", "Symbol e.g. BTCUSDT")
  .requiredOption("--interval <interval>", "Interval e.g. 1h")
  .option("--limit <n>", "Row limit", "50")
  .option("--config <path>", "Config file path")
  .action(async (opts) => {
    const { runQuery } = require("./query");
    const cfg = loadConfig(opts.config);
    setLogLevel(cfg.logLevel);
    runQuery(cfg.dbPath, opts.symbol, opts.interval, parseInt(opts.limit, 10));
  });

program.parseAsync(process.argv);
