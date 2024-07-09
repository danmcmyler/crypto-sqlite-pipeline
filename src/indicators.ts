/* Deterministic indicator calculations */
export interface OHLCV {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

function ema(values: number[], period: number, alphaOverride?: number): (number | null)[] {
  if (period <= 0) { throw new Error("period must be > 0"); }
  const out: (number | null)[] = new Array(values.length).fill(null);
  const k = alphaOverride !== undefined ? alphaOverride : (2 / (period + 1));
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (i < period) {
      sum += v;
      if (i === period - 1) { out[i] = sum / period; }
      continue;
    }
    const prev = out[i - 1] as number;
    const next = v * k + prev * (1 - k);
    out[i] = next;
  }
  return out;
}

function sma(values: number[], period: number): (number | null)[] {
  const out: (number | null)[] = new Array(values.length).fill(null);
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= period) { sum -= values[i - period]; }
    if (i >= period - 1) { out[i] = sum / period; }
  }
  return out;
}

function stddev(values: number[], period: number, ma: (number | null)[]): (number | null)[] {
  const out: (number | null)[] = new Array(values.length).fill(null);
  for (let i = period - 1; i < values.length; i++) {
    const m = ma[i];
    if (m === null) { continue; }
    let s = 0;
    for (let j = i - period + 1; j <= i; j++) {
      const d = values[j] - (m as number);
      s += d * d;
    }
    out[i] = Math.sqrt(s / period);
  }
  return out;
}

export function rsiWilder(close: number[], period = 14): (number | null)[] {
  const out: (number | null)[] = new Array(close.length).fill(null);
  let prevClose: number | null = null;
  let gainSum = 0;
  let lossSum = 0;
  for (let i = 0; i < close.length; i++) {
    const c = close[i];
    if (prevClose === null) { prevClose = c; continue; }
    const change = c - prevClose;
    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? -change : 0;
    if (i <= period) {
      gainSum += gain;
      lossSum += loss;
      if (i === period) {
        let avgGain = gainSum / period;
        let avgLoss = lossSum / period;
        const rs = avgLoss === 0 ? Infinity : avgGain / avgLoss;
        out[i] = 100 - (100 / (1 + rs));
        // Continue smoothing
        prevClose = c;
        for (let k = i + 1; k < close.length; k++) {
          const ch = close[k] - close[k - 1];
          const g = ch > 0 ? ch : 0;
          const l = ch < 0 ? -ch : 0;
          avgGain = ((avgGain * (period - 1)) + g) / period;
          avgLoss = ((avgLoss * (period - 1)) + l) / period;
          const rs2 = avgLoss === 0 ? Infinity : avgGain / avgLoss;
          out[k] = 100 - (100 / (1 + rs2));
        }
        return out;
      }
    }
    prevClose = c;
  }
  return out;
}

export function atrWilder(high: number[], low: number[], close: number[], period = 14): (number | null)[] {
  const out: (number | null)[] = new Array(close.length).fill(null);
  const tr: number[] = new Array(close.length).fill(0);
  for (let i = 0; i < close.length; i++) {
    if (i === 0) {
      tr[i] = high[i] - low[i];
    } else {
      const m1 = high[i] - low[i];
      const m2 = Math.abs(high[i] - close[i - 1]);
      const m3 = Math.abs(low[i] - close[i - 1]);
      tr[i] = Math.max(m1, m2, m3);
    }
  }
  let sum = 0;
  for (let i = 0; i < tr.length; i++) {
    if (i < period) {
      sum += tr[i];
      if (i === period - 1) { out[i] = sum / period; }
    } else {
      const prev = out[i - 1] as number;
      out[i] = ((prev * (period - 1)) + tr[i]) / period;
    }
  }
  return out;
}

export function adxWilder(high: number[], low: number[], close: number[], period = 14): (number | null)[] {
  const len = close.length;
  const out: (number | null)[] = new Array(len).fill(null);
  const tr: number[] = new Array(len).fill(0);
  const plusDM: number[] = new Array(len).fill(0);
  const minusDM: number[] = new Array(len).fill(0);
  for (let i = 1; i < len; i++) {
    const upMove = high[i] - high[i - 1];
    const downMove = low[i - 1] - low[i];
    plusDM[i] = (upMove > downMove && upMove > 0) ? upMove : 0;
    minusDM[i] = (downMove > upMove && downMove > 0) ? downMove : 0;
    const m1 = high[i] - low[i];
    const m2 = Math.abs(high[i] - close[i - 1]);
    const m3 = Math.abs(low[i] - close[i - 1]);
    tr[i] = Math.max(m1, m2, m3);
  }
  // Wilder smoothing
  let tr14 = 0, plusDM14 = 0, minusDM14 = 0;
  for (let i = 1; i <= period; i++) {
    tr14 += tr[i];
    plusDM14 += plusDM[i];
    minusDM14 += minusDM[i];
  }
  let dxArr: (number | null)[] = new Array(len).fill(null);
  let plusDI = (plusDM14 / tr14) * 100;
  let minusDI = (minusDM14 / tr14) * 100;
  let dx = (Math.abs(plusDI - minusDI) / (plusDI + minusDI)) * 100;
  dxArr[period] = dx;
  for (let i = period + 1; i < len; i++) {
    tr14 = tr14 - (tr14 / period) + tr[i];
    plusDM14 = plusDM14 - (plusDM14 / period) + plusDM[i];
    minusDM14 = minusDM14 - (minusDM14 / period) + minusDM[i];
    plusDI = (plusDM14 / tr14) * 100;
    minusDI = (minusDM14 / tr14) * 100;
    dx = (Math.abs(plusDI - minusDI) / (plusDI + minusDI)) * 100;
    dxArr[i] = dx;
  }
  // Smooth DX into ADX
  let adx: (number | null)[] = new Array(len).fill(null);
  let sum = 0;
  let count = 0;
  for (let i = 0; i < len; i++) {
    if (dxArr[i] !== null) {
      sum += dxArr[i] as number;
      count++;
      if (count === period) {
        adx[i] = sum / period;
        break;
      }
    }
  }
  for (let i = adx.findIndex(v => v !== null) + 1; i < len; i++) {
    const prev = adx[i - 1] as number;
    const dxv = dxArr[i];
    if (dxv === null) { continue; }
    adx[i] = ((prev * (period - 1)) + dxv) / period;
  }
  return adx;
}

export function macd(close: number[], fast=12, slow=26, signal=9): { macd: (number | null)[], signal: (number | null)[], hist: (number | null)[] } {
  const emaFast = ema(close, fast);
  const emaSlow = ema(close, slow);
  const macdLine: (number | null)[] = new Array(close.length).fill(null);
  for (let i = 0; i < close.length; i++) {
    const a = emaFast[i];
    const b = emaSlow[i];
    if (a === null || b === null) { continue; }
    macdLine[i] = (a as number) - (b as number);
  }
  const signalLine = ema(macdLine.map(v => v === null ? 0 : (v as number)), signal);
  const hist: (number | null)[] = new Array(close.length).fill(null);
  for (let i = 0; i < close.length; i++) {
    if (macdLine[i] === null || signalLine[i] === null) { continue; }
    hist[i] = (macdLine[i] as number) - (signalLine[i] as number);
  }
  return { macd: macdLine, signal: signalLine, hist };
}

export interface IndicatorsOut {
  ema50: (number | null)[];
  ema200: (number | null)[];
  rsi14: (number | null)[];
  atr14: (number | null)[];
  adx14: (number | null)[];
  vol_ma20: (number | null)[];
  macd: (number | null)[];
  macd_signal: (number | null)[];
  macd_hist: (number | null)[];
  bb_sma20: (number | null)[];
  bb_upper: (number | null)[];
  bb_lower: (number | null)[];
  pct_return_1: (number | null)[];
  log_return_1: (number | null)[];
}

export function computeIndicators(series: OHLCV[]): IndicatorsOut {
  const close = series.map(r => r.close);
  const high = series.map(r => r.high);
  const low = series.map(r => r.low);
  const volume = series.map(r => r.volume);

  const ema50 = ema(close, 50);
  const ema200 = ema(close, 200);
  const rsi14 = rsiWilder(close, 14);
  const atr14 = atrWilder(high, low, close, 14);
  const adx14 = adxWilder(high, low, close, 14);
  const vol_ma20 = sma(volume, 20);

  const macdAll = macd(close, 12, 26, 9);

  const bb_mid = sma(close, 20);
  const bb_std = stddev(close, 20, bb_mid);
  const bb_upper: (number | null)[] = new Array(close.length).fill(null);
  const bb_lower: (number | null)[] = new Array(close.length).fill(null);
  for (let i = 0; i < close.length; i++) {
    if (bb_mid[i] === null || bb_std[i] === null) { continue; }
    bb_upper[i] = (bb_mid[i] as number) + 2 * (bb_std[i] as number);
    bb_lower[i] = (bb_mid[i] as number) - 2 * (bb_std[i] as number);
  }

  const pct_return_1: (number | null)[] = new Array(close.length).fill(null);
  const log_return_1: (number | null)[] = new Array(close.length).fill(null);
  for (let i = 1; i < close.length; i++) {
    const r = close[i - 1] === 0 ? null : (close[i] / close[i - 1] - 1);
    pct_return_1[i] = r;
    log_return_1[i] = close[i - 1] === 0 ? null : Math.log(close[i] / close[i - 1]);
  }

  return {
    ema50,
    ema200,
    rsi14,
    atr14,
    adx14,
    vol_ma20,
    macd: macdAll.macd,
    macd_signal: macdAll.signal,
    macd_hist: macdAll.hist,
    bb_sma20: bb_mid,
    bb_upper,
    bb_lower,
    pct_return_1,
    log_return_1
  };
}
