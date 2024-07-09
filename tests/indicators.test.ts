import { computeIndicators } from "../src/indicators";

test("indicators compute without throwing and produce expected lengths", () => {
  const series = [];
  for (let i = 0; i < 300; i++) {
    const base = 100 + i * 0.1;
    series.push({ time: i, open: base, high: base + 1, low: base - 1, close: base + 0.5, volume: 1000 + i });
  }
  const ind = computeIndicators(series);
  expect(ind.ema50.length).toBe(series.length);
  expect(ind.ema200.length).toBe(series.length);
  expect(ind.rsi14.length).toBe(series.length);
  expect(ind.atr14.length).toBe(series.length);
  expect(ind.adx14.length).toBe(series.length);
  expect(ind.vol_ma20.length).toBe(series.length);
  expect(ind.macd.length).toBe(series.length);
  expect(ind.bb_sma20.length).toBe(series.length);
  expect(ind.bb_upper.length).toBe(series.length);
  expect(ind.bb_lower.length).toBe(series.length);
});
