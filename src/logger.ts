/* Simple JSON logger */
export type LogLevel = "debug" | "info" | "warn" | "error";

const levelOrder: Record<LogLevel, number> = {
  debug: 10, info: 20, warn: 30, error: 40
};

let current: LogLevel = (process.env.LOG_LEVEL as LogLevel) || "info";

export function setLogLevel(level: LogLevel): void {
  current = level;
}

function log(level: LogLevel, msg: string, data?: Record<string, unknown>): void {
  if (levelOrder[level] < levelOrder[current]) { return; }
  const entry: Record<string, unknown> = {
    ts: new Date().toISOString(),
    level,
    msg,
    ...data
  };
  // Single-line JSON
  process.stdout.write(JSON.stringify(entry) + "\n");
}

export const Logger = {
  debug: (msg: string, data?: Record<string, unknown>) => { log("debug", msg, data); },
  info: (msg: string, data?: Record<string, unknown>) => { log("info", msg, data); },
  warn: (msg: string, data?: Record<string, unknown>) => { log("warn", msg, data); },
  error: (msg: string, data?: Record<string, unknown>) => { log("error", msg, data); }
};
