/* Codexin Order Flow — browser-safe USD-M Futures terminal.
 * One venue, one market, one instrument. No Spot fallback and no execution.
 * Missing, stale or invalid evidence keeps the decision gate locked.
 */
const CONFIG = {
  symbol: "BTCUSDT",
  lower: "btcusdt",
  venue: "BINANCE",
  market: "USD-M FUTURES",
  rest: "https://fapi.binance.com/fapi/v1",
  ws: "wss://fstream.binance.com/stream?streams=",
  backend: (new URLSearchParams(window.location.search).get("api") || window.CODEXIN_API_BASE || "https://nce-api.78.46.134.148.sslip.io/api/v2").replace(/\/$/, ""),
  maxTrades: 600,
  maxTape: 32,
  bookLevels: 18,
  freshness: { trade: 3000, book: 1500, kline: 5000, oi: 60000, funding: 7200000, ws: 5000 }
};

const state = {
  interval: "1m", price: null, mark: null, index: null, prevClose: null,
  oi: null, funding: null, ratios: null,
  lastTradeAt: 0, lastBookAt: 0, lastTickerAt: 0, lastKlineAt: 0,
  lastOiAt: 0, lastFundingAt: 0, lastRatioAt: 0, lastMarkAt: 0, lastLiquidationAt: 0,
  lastWsAt: 0, liquidationConnectedAt: 0, tradeCount: 0, errors: 0,
  cvd: 0, vwapPV: 0, vwapVolume: 0, events: [], trades: [],
  liquidations: [], buckets: new Map(), profile: new Map(), klines: [],
  lifecycle: new Map(), lastBookSyncAt: 0, lastDepthEventAt: 0,
  lastBookEventTime: 0, lastTickerEventTime: 0,
  aggression: { buy: 0, sell: 0, count: 0, volume: 0 },
  book: { bids: new Map(), asks: new Map(), lastUpdateId: null, lastEventId: null,
    valid: false, resyncs: 0, queue: [], syncing: false, renderQueued: false },
  ws: null, wsConnected: false, reconnects: 0, uiRenderScheduled: false, backendHealth: null, directFallbackStarted: false, chart: null, candleSeries: null,
  volumeSeries: null
};

const $ = id => document.getElementById(id);
const fmt = (n, d = 2) => Number.isFinite(Number(n)) ? Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }) : "—";
const usd = n => Number.isFinite(Number(n)) ? `$${fmt(n, 0)}` : "—";
const ago = t => t ? ((Date.now() - t) / 1000 < 10 ? `${Math.max(0, (Date.now() - t) / 1000).toFixed(1)}s` : `${Math.round((Date.now() - t) / 1000)}s`) : "—";
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[c]));

function log(message, kind = "info") {
  state.events.unshift({ time: new Date(), message, kind });
  state.events = state.events.slice(0, 80);
  renderHealthLog();
}

function setText(id, value) { const el = $(id); if (el) el.textContent = value; }
function setClass(id, cls) { const el = $(id); if (el) el.className = cls; }
function scheduleUiRender() {
  if (state.uiRenderScheduled) return;
  state.uiRenderScheduled = true;
  setTimeout(() => { state.uiRenderScheduled = false; renderTape(); renderFlow(); renderHeader(); renderDecision(); renderContext(); }, 100);
}

async function json(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeout || 8000);
  try {
    const response = await fetch(url, { signal: controller.signal, cache: "no-store" });
    if (!response.ok) throw Error(`HTTP ${response.status}`);
    return await response.json();
  } finally { clearTimeout(timer); }
}

async function pollBackendSnapshot() {
  if (!CONFIG.backend || state.directFallbackStarted) return;
  try {
    const snapshot = await json(`${CONFIG.backend}/live/${CONFIG.symbol}/futures/snapshot`, { timeout: 5000 });
    state.backendHealth = snapshot.health || await json(`${CONFIG.backend}/health`, { timeout: 5000 });
    const freshness = snapshot.freshness || {}, price = snapshot.price || {}, flow = snapshot.flow || {}, derivatives = snapshot.derivatives || {}, orderbook = snapshot.orderbook || {};
    state.price = price.last ?? state.price; state.mark = price.mark ?? state.mark; state.index = price.index ?? state.index; state.cvd = Number(flow.cvd || 0); state.vwapPV = Number(flow.vwap || 0); state.vwapVolume = flow.vwap ? 1 : 0; state.tradeCount = Number(flow.trade_count || 0);
    state.oi = derivatives.open_interest ?? state.oi; state.funding = derivatives.funding_rate ?? state.funding; state.ratios = derivatives.positioning ?? state.ratios;
    state.buckets = new Map((flow.buckets || []).map(bucket => [Number(bucket.time), bucket]));
    state.klines = (snapshot.candles?.rows || []).map(row => ({ time: Number(row.time), open: Number(row.open), high: Number(row.high), low: Number(row.low), close: Number(row.close), volume: Number(row.volume), closed: Boolean(row.closed) }));
    state.liquidations = snapshot.liquidations?.recent || [];
    if (orderbook.bids && orderbook.asks) {
      const nextBids = orderbook.bids.map(row => [String(row.price), Number(row.quantity)]), nextAsks = orderbook.asks.map(row => [String(row.price), Number(row.quantity)]);
      for (const price of state.book.bids.keys()) if (!nextBids.some(row => row[0] === price)) trackDepthSide("bid", [[price, 0]]);
      for (const price of state.book.asks.keys()) if (!nextAsks.some(row => row[0] === price)) trackDepthSide("ask", [[price, 0]]);
      trackDepthSide("bid", nextBids); trackDepthSide("ask", nextAsks);
      state.book.bids = new Map(nextBids); state.book.asks = new Map(nextAsks); state.book.lastUpdateId = orderbook.sequence; state.book.valid = Boolean(orderbook.valid); state.lastBookAt = Date.now();
    }
    const age = value => value == null ? 0 : Math.max(1, Date.now() - Number(value)); state.lastTradeAt = freshness.trade_age_ms == null ? state.lastTradeAt : Date.now() - Number(freshness.trade_age_ms); state.lastBookAt = freshness.book_age_ms == null ? state.lastBookAt : Date.now() - Number(freshness.book_age_ms); state.lastKlineAt = snapshot.candles?.age_ms == null ? state.lastKlineAt : Date.now() - Number(snapshot.candles.age_ms); state.lastOiAt = freshness.oi_age_ms == null ? state.lastOiAt : Date.now() - Number(freshness.oi_age_ms); state.lastFundingAt = freshness.funding_age_ms == null ? state.lastFundingAt : Date.now() - Number(freshness.funding_age_ms);
    setFeed("trades", state.backendHealth.feeds?.trades?.status || "WAITING"); setFeed("orderbook", state.backendHealth.feeds?.orderbook?.status || "WAITING");
    renderAll();
  } catch (error) {
    state.backendHealth = null; state.errors += 1; log(`Backend snapshot unavailable: ${error.message}`, "warn");
    if (!state.directFallbackStarted) { state.directFallbackStarted = true; log("Falling back to direct Binance Futures browser feed", "warn"); Promise.all([loadHistory(), loadDerivatives()]).finally(() => { openStream(); setInterval(loadDerivatives, 30000); setInterval(pollKline, 5000); pollKline(); }); }
  }
}

function streamUrl() {
  return CONFIG.ws + [
    `${CONFIG.lower}@trade`,
    `${CONFIG.lower}@depth@100ms`,
    `${CONFIG.lower}@bookTicker`,
    `${CONFIG.lower}@forceOrder`,
    `${CONFIG.lower}@markPrice@1s`
  ].join("/");
}

function streamData(message) { return message.data || message; }

function openStream() {
  if (state.ws) try { state.ws.close(); } catch { /* noop */ }
  state.wsConnected = false;
  setFeed("trades", "CONNECTING");
  setFeed("orderbook", "CONNECTING");
  state.ws = new WebSocket(streamUrl());
  state.ws.onopen = () => {
    state.wsConnected = true;
    state.reconnects += 1;
    state.liquidationConnectedAt = Date.now();
    log("USD-M Futures WebSocket connected: trades · L2 · forceOrder · mark price", "good");
    setFeed("trades", "LIVE");
    setFeed("orderbook", state.book.valid ? "LIVE" : "SYNCING");
    syncBook();
  };
  state.ws.onmessage = event => {
    try {
      const message = JSON.parse(event.data);
      const data = streamData(message);
      const stream = message.stream || "";
      state.lastWsAt = Date.now();
      if (stream.includes("@trade") || data.e === "trade" || data.e === "aggTrade") onTrade(data);
      else if (stream.includes("depth") || data.e === "depthUpdate") onDepth(data);
      else if (stream.includes("kline") || data.e === "kline") onKline(data);
      else if (stream.includes("bookTicker") || data.e === "bookTicker") onBookTicker(data);
      else if (stream.includes("forceOrder") || data.e === "forceOrder") onLiquidation(data);
      else if (stream.includes("markPrice") || data.e === "markPriceUpdate") onMarkPrice(data);
    } catch (error) {
      state.errors += 1;
      log(`Stream parse error: ${error.message}`, "warn");
    }
  };
  state.ws.onerror = () => {
    state.errors += 1;
    state.wsConnected = false;
    log("WebSocket error; validated feeds are degraded", "warn");
    setFeed("trades", "ERROR"); setFeed("orderbook", "ERROR");
  };
  state.ws.onclose = () => {
    state.wsConnected = false;
    setFeed("trades", "RECONNECT"); setFeed("orderbook", "RECONNECT");
    setTimeout(openStream, 1500);
  };
}

function invalidateBook(reason, event) {
  const b = state.book;
  b.valid = false;
  b.resyncs += 1;
  if (event) b.queue.push(event);
  if (b.queue.length > 1500) b.queue.splice(0, b.queue.length - 1500);
  setFeed("orderbook", "RESYNC");
  log(`Order book invalidated: ${reason}`, "warn");
  if (!b.syncing) setTimeout(syncBook, 100);
}

function applyDepth(event) {
  const b = state.book;
  const finalId = Number(event.u);
  if (!Number.isFinite(finalId)) return false;
  if ((b.lastEventId !== null && finalId <= b.lastEventId) || (b.lastEventId === null && b.lastUpdateId !== null && finalId <= b.lastUpdateId)) return true;
  if (b.lastEventId !== null) {
    if (event.pu !== undefined && Number(event.pu) !== b.lastEventId) return false;
    if (event.pu === undefined && Number(event.U) > b.lastEventId + 1) return false;
  } else if (b.lastUpdateId !== null && Number(event.U) > b.lastUpdateId + 1) {
    return false;
  }
  trackDepthSide("bid", event.b || []); trackDepthSide("ask", event.a || []);
  b.lastUpdateId = finalId; b.lastEventId = finalId; b.valid = true;
  state.lastBookAt = Date.now(); state.lastDepthEventAt = state.lastBookAt; state.lastBookEventTime = Number(event.E || event.T || state.lastBookAt);
  return true;
}

function lifecycleKey(side, price) { return `${side}:${price}`; }
function trackDepthSide(side, updates, snapshot = false) {
  const map = side === "bid" ? state.book.bids : state.book.asks;
  const now = Date.now();
  for (const [price, rawQuantity] of updates) {
    const key = String(price), quantity = Number(rawQuantity), old = Number(map.get(key) || 0);
    const id = lifecycleKey(side, key);
    let level = state.lifecycle.get(id);
    if (!level) level = { side, price: Number(price), firstSeen: now, lastSeen: now, lastAddedAt: now, addCount: 0, removeCount: 0, refillCount: 0, visibleQuantity: 0, peakQuantity: 0, addedQuantity: 0, removedQuantity: 0, replenishedQuantity: 0, consumedQuantity: 0, pulledQuantity: 0, visible: false };
    level.lastSeen = now;
    if (quantity > 0) {
      if (!old) { level.firstSeen = level.firstSeen || now; level.addCount += 1; level.lastAddedAt = now; level.addedQuantity += quantity; }
      else if (quantity > old) { const add = quantity - old; level.addCount += 1; level.lastAddedAt = now; level.addedQuantity += add; level.refillCount += 1; level.replenishedQuantity += add; }
      else if (quantity < old) { const removed = old - quantity; level.removeCount += 1; level.removedQuantity += removed; level.consumedQuantity += removed; }
      level.visibleQuantity = quantity; level.peakQuantity = Math.max(level.peakQuantity, quantity); level.visible = true; map.set(key, quantity);
    } else if (old > 0) {
      level.removeCount += 1; level.removedQuantity += old; level.pulledQuantity += old; level.visibleQuantity = 0; level.visible = false; map.delete(key);
    }
    state.lifecycle.set(id, level);
  }
}

async function syncBook() {
  const b = state.book;
  if (b.syncing) return;
  b.syncing = true; b.valid = false; setFeed("orderbook", "SYNCING");
  try {
    const snapshot = await json(`${CONFIG.rest}/depth?symbol=${CONFIG.symbol}&limit=1000`, { timeout: 8000 });
    b.bids = new Map(snapshot.bids.map(row => [String(row[0]), Number(row[1])]).filter(row => row[1] > 0));
    b.asks = new Map(snapshot.asks.map(row => [String(row[0]), Number(row[1])]).filter(row => row[1] > 0));
    const syncedAt = Date.now(); state.lastBookSyncAt = syncedAt;
    for (const [price, quantity] of b.bids) trackDepthSide("bid", [[price, quantity]], true);
    for (const [price, quantity] of b.asks) trackDepthSide("ask", [[price, quantity]], true);
    b.lastUpdateId = Number(snapshot.lastUpdateId); b.lastEventId = null; b.valid = false;
    const buffered = b.queue.splice(0);
    const first = buffered.findIndex(event => Number(event.U) <= b.lastUpdateId + 1 && Number(event.u) >= b.lastUpdateId);
    if (first < 0) {
      b.syncing = false; setFeed("orderbook", "RESYNC");
      setTimeout(syncBook, 350); return;
    }
    for (const event of buffered.slice(first)) {
      if (Number(event.u) <= b.lastUpdateId) continue;
      if (!applyDepth(event)) {
        b.syncing = false; invalidateBook("buffer sequence gap", event); return;
      }
    }
    b.syncing = false; b.valid = true; setFeed("orderbook", "LIVE");
    log(`Order book synced at sequence ${b.lastUpdateId}`, "good");
    renderAll();
  } catch (error) {
    b.syncing = false; b.valid = false;
    state.errors += 1; setFeed("orderbook", "ERROR");
    log(`Order book sync failed: ${error.message}`, "warn");
    setTimeout(syncBook, 2500);
  }
}

function scheduleBookRender() {
  if (state.book.renderQueued) return;
  state.book.renderQueued = true;
  const render = () => { state.book.renderQueued = false; renderBook(); renderHeader(); };
  if (window.requestAnimationFrame) requestAnimationFrame(render); else setTimeout(render, 100);
}

function onDepth(event) {
  const b = state.book;
  if (!b.valid) {
    b.queue.push(event);
    if (b.queue.length > 1500) b.queue.shift();
    return;
  }
  if (!applyDepth(event)) { invalidateBook(`live sequence gap at ${event.u}`, event); return; }
  scheduleBookRender();
}

function onTrade(event) {
  const price = Number(event.p), quantity = Number(event.q), notional = price * quantity;
  if (!Number.isFinite(price) || !Number.isFinite(quantity)) return;
  const buy = !event.m, timestamp = Number(event.T) || Date.now();
  state.price = price; state.lastTradeAt = Date.now(); state.tradeCount += 1;
  state.cvd += buy ? notional : -notional;
  state.aggression[buy ? "buy" : "sell"] += notional; state.aggression.count += 1; state.aggression.volume += quantity;
  state.vwapPV += price * notional; state.vwapVolume += notional;
  state.trades.unshift({ price, quantity, notional, buy, timestamp });
  state.trades = state.trades.slice(0, CONFIG.maxTrades);
  const bucketTime = Math.floor(timestamp / 60000) * 60000;
  let bucket = state.buckets.get(bucketTime);
  if (!bucket) bucket = { time: bucketTime, buy: 0, sell: 0, price, high: price, low: price, count: 0 };
  bucket.price = price; bucket.high = Math.max(bucket.high, price); bucket.low = Math.min(bucket.low, price);
  bucket.count += 1; buy ? bucket.buy += notional : bucket.sell += notional; state.buckets.set(bucketTime, bucket);
  const profileKey = (Math.round(price / 50) * 50).toFixed(0);
  state.profile.set(profileKey, (state.profile.get(profileKey) || 0) + notional);
  if (state.buckets.size > 90) state.buckets.delete([...state.buckets.keys()].sort((a, b) => a - b)[0]);
  scheduleUiRender();
}

function onLiquidation(event) {
  const order = event.o || event;
  const price = Number(order.ap || order.p), quantity = Number(order.z || order.q), notional = price * quantity;
  if (!Number.isFinite(price) || !Number.isFinite(quantity)) return;
  state.lastLiquidationAt = Date.now();
  state.liquidations.unshift({ time: Number(order.T || Date.now()), side: order.S || "—", price, quantity, notional, position: order.ps || "—", status: order.X || "FILLED" });
  state.liquidations = state.liquidations.slice(0, 80);
  renderLiquidations(); renderHealth(); renderDecision();
  log(`Observed forceOrder: ${order.S || "?"} ${usd(notional)}`, "warn");
}

function onMarkPrice(event) {
  state.mark = Number(event.p) || state.mark; state.index = Number(event.i) || state.index;
  if (Number.isFinite(Number(event.r))) state.funding = Number(event.r);
  state.lastMarkAt = Date.now(); renderDerivatives(); renderHeader();
}

function onKline(event) {
  const kline = event.k; if (!kline) return;
  const row = { time: Number(kline.t) / 1000, open: Number(kline.o), high: Number(kline.h), low: Number(kline.l), close: Number(kline.c), volume: Number(kline.q), closed: !!kline.x };
  state.lastKlineAt = Date.now();
  const index = state.klines.findIndex(rowItem => rowItem.time === row.time);
  if (index >= 0) state.klines[index] = row; else state.klines.push(row);
  state.klines = state.klines.slice(-500); renderChart(); renderHeader();
}

function onBookTicker(event) {
  const bid = Number(event.b), ask = Number(event.a);
  if (!bid || !ask) return;
  state.price = (bid + ask) / 2; state.lastTickerAt = Date.now(); state.lastTickerEventTime = Number(event.E || event.T || state.lastTickerAt);
  setText("best-bid", fmt(bid)); setText("best-ask", fmt(ask)); setText("spread-value", `${((ask - bid) / state.price * 10000).toFixed(2)} bp`); scheduleUiRender();
}

async function loadHistory() {
  try {
    const rows = await json(`${CONFIG.rest}/klines?symbol=${CONFIG.symbol}&interval=1m&limit=500`);
    state.klines = rows.filter(row => Number(row[6]) < Date.now()).map(row => ({ time: Number(row[0]) / 1000, open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), volume: Number(row[7]), closed: true }));
    state.prevClose = state.klines.at(-2)?.close || null; renderChart(); log(`Loaded ${state.klines.length} closed Futures candles`, "good");
  } catch (error) { state.errors += 1; log(`Kline history unavailable: ${error.message}`, "warn"); }
}

async function pollKline() {
  try {
    const rows = await json(`${CONFIG.rest}/klines?symbol=${CONFIG.symbol}&interval=1m&limit=2`, { timeout: 5000 });
    for (const row of rows) {
      const item = { time: Number(row[0]) / 1000, open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), volume: Number(row[7]), closed: Number(row[6]) < Date.now() };
      const index = state.klines.findIndex(existing => existing.time === item.time);
      if (index >= 0) state.klines[index] = item; else state.klines.push(item);
    }
    state.klines = state.klines.slice(-500); state.lastKlineAt = Date.now(); renderChart(); renderHeader();
  } catch (error) { log(`Live kline poll unavailable: ${error.message}`, "warn"); }
}

async function loadDerivatives() {
  try {
    const [oi, funding, premium, ratio] = await Promise.all([
      json(`${CONFIG.rest}/openInterest?symbol=${CONFIG.symbol}`),
      json(`${CONFIG.rest}/fundingRate?symbol=${CONFIG.symbol}&limit=1`),
      json(`${CONFIG.rest}/premiumIndex?symbol=${CONFIG.symbol}`),
      json(`https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=${CONFIG.symbol}&period=5m&limit=1`)
    ]);
    state.oi = Number(oi.openInterest); state.funding = Number(funding?.[0]?.fundingRate ?? premium.lastFundingRate);
    state.mark = Number(premium.markPrice); state.index = Number(premium.indexPrice); state.ratios = ratio?.[0] || null;
    state.lastOiAt = state.lastFundingAt = state.lastRatioAt = Date.now(); renderDerivatives(); renderHeader();
    log("OI, funding, mark price and positioning feeds refreshed", "good");
  } catch (error) { state.errors += 1; log(`Derivative feed unavailable: ${error.message}`, "warn"); }
}

function setFeed(feed, status) {
  const id = feed === "trades" ? "tape-status" : "book-status";
  setText(id, status); if (feed === "orderbook") setText("large-order-status", status);
}

function bookMetrics() {
  const b = state.book; if (!b.valid) return { valid: false };
  const bids = [...b.bids.entries()].map(([price, quantity]) => ({ p: Number(price), q: quantity })).sort((a, c) => c.p - a.p);
  const asks = [...b.asks.entries()].map(([price, quantity]) => ({ p: Number(price), q: quantity })).sort((a, c) => a.p - c.p);
  if (!bids.length || !asks.length || bids[0].p >= asks[0].p) return { valid: false, reason: "best bid must be below best ask" };
  const mid = (bids[0]?.p + asks[0]?.p) / 2 || state.price, range = mid * 0.001;
  const bid10 = bids.filter(row => row.p >= mid - range).reduce((sum, row) => sum + row.p * row.q, 0);
  const ask10 = asks.filter(row => row.p <= mid + range).reduce((sum, row) => sum + row.p * row.q, 0);
  const bidBtc = bids.reduce((sum, row) => sum + row.q, 0), askBtc = asks.reduce((sum, row) => sum + row.q, 0);
  const bidNotional = bids.reduce((sum, row) => sum + row.p * row.q, 0), askNotional = asks.reduce((sum, row) => sum + row.p * row.q, 0), total = bidNotional + askNotional;
  const weighted = rows => rows.reduce((sum, row) => sum + row.q * Math.exp(-Math.abs(row.p - mid) / (mid * 0.002)), 0);
  const weightedBid = weighted(bids), weightedAsk = weighted(asks), weightedTotal = weightedBid + weightedAsk;
  const microprice = (asks[0].q * bids[0].p + bids[0].q * asks[0].p) / (bids[0].q + asks[0].q);
  return { valid: true, bids, asks, bid10, ask10, bidBtc, askBtc, bidNotional, askNotional, bidLevels: bids.length, askLevels: asks.length, bidShare: total ? bidNotional / total * 100 : 50, askShare: total ? askNotional / total * 100 : 50, imbalance: total ? (bidNotional - askNotional) / total * 100 : 0, weightedImbalance: weightedTotal ? (weightedBid - weightedAsk) / weightedTotal * 100 : 0, microprice, mid, spread: asks[0]?.p - bids[0]?.p, spreadBps: (asks[0]?.p - bids[0]?.p) / mid * 10000 };
}

function renderHeader() {
  setText("last-price", usd(state.price));
  const change = state.prevClose && state.price ? (state.price - state.prevClose) / state.prevClose * 100 : null;
  setText("price-change", change == null ? "—" : `${change >= 0 ? "+" : ""}${change.toFixed(2)}%`); setClass("price-change", change == null ? "neutral" : change >= 0 ? "up-text" : "down-text");
  const bucket = [...state.buckets.values()].at(-1), delta = bucket ? bucket.buy - bucket.sell : null, metrics = bookMetrics();
  setText("delta-value", delta == null ? "—" : usd(delta)); setClass("delta-value", delta == null ? "neutral" : delta >= 0 ? "up-text" : "down-text");
  setText("delta-detail", bucket ? `${bucket.count.toLocaleString()} executions · ${new Date(bucket.time).toISOString().slice(11, 16)} UTC` : "No closed bucket yet");
  setText("book-imbalance", metrics.valid ? `${metrics.imbalance >= 0 ? "+" : ""}${metrics.imbalance.toFixed(1)}%` : "—"); setClass("book-imbalance", metrics.imbalance > 0 ? "up-text" : metrics.imbalance < 0 ? "down-text" : "neutral");
  setText("book-detail", metrics.valid ? `${metrics.bidShare.toFixed(1)}% bid / ${metrics.askShare.toFixed(1)}% ask` : "Order book syncing");
  setText("data-age", ago(Math.max(state.lastTradeAt, state.lastBookAt))); setText("data-age-detail", state.lastTradeAt ? `Last trade ${ago(state.lastTradeAt)} ago` : "No tick received");
  const fresh = state.lastTradeAt && Date.now() - state.lastTradeAt < CONFIG.freshness.trade && metrics.valid;
  setText("market-state", fresh ? (delta > 0 ? "BUY PRESSURE" : delta < 0 ? "SELL PRESSURE" : "BALANCED") : "WAITING");
  setClass("market-state", fresh ? (delta > 0 ? "up-text" : delta < 0 ? "down-text" : "neutral") : "neutral"); setText("market-state-detail", fresh ? "Measurement only · no authorization" : "Awaiting validated feeds");
  setText("footer-event-age", ago(Math.max(state.lastTradeAt, state.lastBookAt))); setText("footer-status", fresh ? "Validated Futures feeds active · execution disabled" : "Initializing validated USD-M Futures feeds…");
  const vwap = state.vwapVolume ? state.vwapPV / state.vwapVolume : null; setText("flow-vwap", vwap ? fmt(vwap) : "—"); setText("flow-cvd", usd(state.cvd)); setText("flow-trades", state.tradeCount.toLocaleString());
}

function renderBook() {
  const metrics = bookMetrics(); if (!metrics.valid) { setText("book-age", "Age —"); return; }
  const rows = [...metrics.asks.slice(0, CONFIG.bookLevels).reverse().map(row => ({ ...row, side: "ask" })), { p: metrics.mid, q: null, side: "mid" }, ...metrics.bids.slice(0, CONFIG.bookLevels).map(row => ({ ...row, side: "bid" }))];
  const max = Math.max(...rows.filter(row => row.q).map(row => row.q), 1);
  $("orderbook").innerHTML = rows.map(row => row.side === "mid" ? `<div class="book-row mid-row"><span></span><span class="price">${fmt(row.p)}</span><span></span></div>` : `<div class="book-row ${row.side}"><span class="price">${fmt(row.p)}</span><span></span><span class="qty">${fmt(row.q, 3)}</span><i class="depth-fill" style="width:${Math.min(100, row.q / max * 100)}%"></i></div>`).join("");
  setText("bid-total", usd(metrics.bid10)); setText("ask-total", usd(metrics.ask10)); setText("book-sequence", `Sequence ${state.book.lastUpdateId || "—"}`); setText("book-age", `Age ${ago(state.lastBookAt)}`); renderLiquidity(metrics);
}

function estimatedZones() {
  const price = state.mark || state.price, oi = state.oi; if (!price || !oi) return [];
  return [-.025, -.018, -.012, -.008, .008, .012, .018, .025].map((distance, index) => ({ price: price * (1 + distance), size: Math.max(25, oi * .00035 * (1 - Math.abs(distance) * 8) * (index % 3 === 0 ? 1.8 : 1)) })).sort((a, b) => Math.abs(a.price - price) - Math.abs(b.price - price)).slice(0, 6);
}

function renderLiquidity(metrics = bookMetrics()) {
  if (!metrics.valid) return;
  setText("liq-bid-notional", usd(metrics.bid10)); setText("liq-ask-notional", usd(metrics.ask10)); setText("liq-bid-share", `${metrics.bidShare.toFixed(1)}% of ±10bps displayed book`); setText("liq-ask-share", `${metrics.askShare.toFixed(1)}% of ±10bps displayed book`);
  const zones = estimatedZones(); setText("liq-nearest", zones[0] ? usd(zones[0].price) : "—");
  $("liquidity-bars").innerHTML = zones.length ? zones.map(zone => `<div class="liq-zone"><span class="zone-price">${usd(zone.price)}</span><span class="zone-bar" style="width:${Math.max(8, zone.size / zones[0].size * 100)}%"></span><span class="zone-size">${fmt(zone.size, 0)} BTC</span></div>`).join("") : `<div class="empty-state">Waiting for OI and mark price</div>`;
  const large = [...metrics.asks.slice(0, 20), ...metrics.bids.slice(0, 20)].filter(row => row.p * row.q > 250000).sort((a, b) => b.p * b.q - a.p * a.q).slice(0, 12);
  $("large-orders-table").innerHTML = large.length ? large.map(row => { const life = lifecycleFor(row, row.p > metrics.mid ? "ask" : "bid"), lifetime = (Date.now() - Number(life.firstSeen || Date.now())) / 1000, pulledRatio = Number(life.pulledQuantity || 0) / Math.max(Number(life.removedQuantity || 0), 1), refillRatio = Number(life.replenishedQuantity || 0) / Math.max(Number(life.addedQuantity || 0), 1), status = pulledRatio > .7 && lifetime < 10 ? "SPOOF WATCH" : refillRatio > .25 ? "REPLENISHING WALL" : lifetime > 8 ? "PERSISTENT WALL" : "UNCONFIRMED"; return `<tr><td class="${row.p > metrics.mid ? "sell-text" : "buy-text"}">${row.p > metrics.mid ? "ASK" : "BID"}</td><td>${usd(row.p)}</td><td>${usd(row.p * row.q)}</td><td>${((row.p - metrics.mid) / metrics.mid * 10000).toFixed(1)} bp</td><td>${lifetime.toFixed(1)}s</td><td class="warning-text">${status}<br><span class="muted-cell">lifetime ${lifetime.toFixed(1)}s · pulled ${(pulledRatio * 100).toFixed(0)}% · refill ${(refillRatio * 100).toFixed(0)}% · price reaction UNAVAILABLE</span></td></tr>`; }).join("") : `<tr><td colspan="6" class="empty-state">No large validated resting orders in current range</td></tr>`;
}

function renderLiquidations() {
  const rows = state.liquidations.slice(0, 20), total = state.liquidations.reduce((sum, row) => sum + row.notional, 0), longLiq = state.liquidations.filter(row => row.side === "SELL").reduce((sum, row) => sum + row.notional, 0), shortLiq = state.liquidations.filter(row => row.side === "BUY").reduce((sum, row) => sum + row.notional, 0);
  const backendLiq = state.backendHealth?.feeds?.liquidations?.status; const live = backendLiq ? backendLiq.startsWith("LIVE") : state.wsConnected && Date.now() - state.lastWsAt < CONFIG.freshness.ws;
  setText("liquidation-feed-status", live ? "LIVE / QUIET" : "UNAVAILABLE"); setText("liq-observed-total", usd(total)); setText("liq-long-total", usd(longLiq)); setText("liq-short-total", usd(shortLiq)); setText("liq-observed-age", state.lastLiquidationAt ? `Last event ${ago(state.lastLiquidationAt)} ago` : live ? "Connected · no events in session" : "Feed unavailable");
  $("liquidation-table").innerHTML = rows.length ? rows.map(row => `<tr><td class="${row.side === "SELL" ? "sell-text" : "buy-text"}">${row.side === "SELL" ? "LONG LIQ" : "SHORT LIQ"}</td><td>${new Date(row.time).toISOString().slice(11, 19)}</td><td>${usd(row.price)}</td><td>${usd(row.notional)}</td><td>${fmt(row.quantity, 3)}</td><td>${esc(row.position)}</td></tr>`).join("") : `<tr><td colspan="6" class="empty-state">No observed forceOrder events received in this session</td></tr>`;
}

function renderTape() {
  $("tape").innerHTML = state.trades.slice(0, CONFIG.maxTape).map(trade => `<div class="tape-row ${trade.buy ? "buy" : "sell"}"><span>${new Date(trade.timestamp).toISOString().slice(11, 19)}</span><span>${fmt(trade.price)}</span><span class="size">${usd(trade.notional)}</span><span class="side">${trade.buy ? "BUY" : "SELL"}</span></div>`).join("") || `<div class="empty-state">Waiting for Futures aggTrade</div>`;
}

function renderFlow() {
  const rows = [...state.buckets.values()].sort((a, b) => a.time - b.time).slice(-12), max = Math.max(...rows.map(row => Math.max(row.buy, row.sell)), 1);
  $("footprint").innerHTML = rows.map(row => { const delta = row.buy - row.sell; return `<div class="flow-row"><span class="time">${new Date(row.time).toISOString().slice(11, 16)}</span><span><i class="bar buy-bar" style="width:${Math.max(3, row.buy / max * 100)}%"></i></span><span><i class="bar sell-bar" style="width:${Math.max(3, row.sell / max * 100)}%"></i></span><span class="flow-num ${delta >= 0 ? "buy-text" : "sell-text"}">${delta >= 0 ? "+" : ""}${usd(delta)}</span></div>`; }).join("") || `<div class="empty-state">Waiting for closed 1m buckets</div>`;
}

function renderChart() {
  if (!state.chart || !state.candleSeries) return;
  state.candleSeries.setData(state.klines.map(row => ({ time: row.time, open: row.open, high: row.high, low: row.low, close: row.close })));
  state.volumeSeries?.setData(state.klines.map(row => ({ time: row.time, value: row.volume, color: row.close >= row.open ? "#4fe39b55" : "#ff6f8555" })));
  setText("chart-last-update", `Last update ${ago(state.lastKlineAt)}`);
}

function initChart() {
  if (!window.LightweightCharts) { setText("chart-last-update", "Chart library unavailable"); return; }
  const element = $("price-chart");
  state.chart = LightweightCharts.createChart(element, { layout: { background: { color: "transparent" }, textColor: "#7d98ad" }, grid: { vertLines: { color: "#142a40" }, horzLines: { color: "#142a40" } }, rightPriceScale: { borderColor: "#1d3953" }, timeScale: { borderColor: "#1d3953", timeVisible: true, secondsVisible: false }, crosshair: { mode: 1 }, width: element.clientWidth, height: 405 });
  state.candleSeries = state.chart.addCandlestickSeries({ upColor: "#4fe39b", downColor: "#ff6f85", borderVisible: false, wickUpColor: "#4fe39b", wickDownColor: "#ff6f85" });
  state.volumeSeries = state.chart.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "volume", scaleMargins: { top: .82, bottom: 0 }, color: "#4c8" });
  new ResizeObserver(() => state.chart?.applyOptions({ width: element.clientWidth })).observe(element); renderChart();
}

function renderDerivatives() {
  setText("oi-value", state.oi ? fmt(state.oi, 3) : "—"); setText("oi-age", state.lastOiAt ? `Updated ${ago(state.lastOiAt)} ago` : "Unavailable");
  setText("funding-value", state.funding == null ? "—" : `${(state.funding * 100).toFixed(4)}%`); setText("funding-state", state.funding == null ? "Unavailable" : state.funding > 0 ? "Positive · long pays short" : "Negative · short pays long"); setText("mark-value", usd(state.mark));
  const ratio = state.ratios, long = Number(ratio?.longAccount) * 100, short = Number(ratio?.shortAccount) * 100;
  setText("positioning-value", Number.isFinite(long) ? `${long.toFixed(1)}% LONG` : "—"); setText("positioning-detail", Number.isFinite(long) ? `Ratio ${Number(ratio.longShortRatio).toFixed(2)}` : "Public ratio unavailable"); setText("long-ratio", Number.isFinite(long) ? `Long ${long.toFixed(1)}%` : "Long —"); setText("short-ratio", Number.isFinite(short) ? `Short ${short.toFixed(1)}%` : "Short —");
  $("long-bar").style.width = `${Number.isFinite(long) ? long : 50}%`; $("short-bar").style.width = `${Number.isFinite(short) ? short : 50}%`; setText("ratio-status", ratio ? "LIVE" : "UNAVAILABLE"); setText("basis-value", state.mark && state.index ? usd(state.mark - state.index) : "—"); setText("funding-regime", state.funding == null ? "—" : Math.abs(state.funding) > .0005 ? "EXTREME" : "NORMAL");
}

function renderContext() {
  const bucket = [...state.buckets.values()].at(-1), metrics = bookMetrics();
  setText("context-flow", state.lastTradeAt ? (bucket && bucket.buy - bucket.sell >= 0 ? "BUY PRESSURE" : "SELL PRESSURE") : "WAITING"); setText("context-book", metrics.valid ? `${metrics.imbalance >= 0 ? "BID" : "ASK"} IMBALANCE` : "WAITING"); setText("context-structure", state.klines.length ? "MEASURED" : "WAITING");
}

function feedRows() {
  const now = Date.now(), wsLive = state.wsConnected && now - state.lastWsAt < CONFIG.freshness.ws;
  return [
    { label: "Futures trade tape", status: state.lastTradeAt && now - state.lastTradeAt < CONFIG.freshness.trade ? "LIVE" : "STALE", age: state.lastTradeAt ? now - state.lastTradeAt : null, detail: "wss://fstream.binance.com · @trade" },
    { label: "Local L2 order book", status: state.book.valid && now - state.lastBookAt < CONFIG.freshness.book ? "LIVE" : state.book.valid ? "STALE" : "INVALID", age: state.lastBookAt ? now - state.lastBookAt : null, detail: `sequence ${state.book.lastUpdateId || "—"} · resync ${state.book.resyncs}` },
    { label: "Closed Futures candles", status: state.lastKlineAt && now - state.lastKlineAt < CONFIG.freshness.kline ? "LIVE" : "STALE", age: state.lastKlineAt ? now - state.lastKlineAt : null, detail: `${state.klines.length} rows · ${state.interval}` },
    { label: "Open interest", status: state.lastOiAt && now - state.lastOiAt < CONFIG.freshness.oi ? "LIVE" : state.oi !== null ? "STALE" : "UNAVAILABLE", age: state.lastOiAt ? now - state.lastOiAt : null, detail: "Binance Futures REST · SLA <60s" },
    { label: "Funding rate", status: state.lastFundingAt && now - state.lastFundingAt < CONFIG.freshness.funding ? "LIVE" : state.funding !== null ? "STALE" : "UNAVAILABLE", age: state.lastFundingAt ? now - state.lastFundingAt : null, detail: "Binance Futures REST · SLA <2h" },
    { label: "Observed forceOrder", status: wsLive ? "LIVE · QUIET" : "UNAVAILABLE", age: state.lastLiquidationAt ? now - state.lastLiquidationAt : null, detail: wsLive ? "Binance Futures liquidation stream" : "No verified liquidation stream connection" },
    { label: "Macro / news", status: "UNAVAILABLE", age: null, detail: "Not connected; never used for live decisions" }
  ];
}

function renderHealth() {
  const rows = state.backendHealth ? Object.entries(state.backendHealth.feeds || {}).map(([key, feed]) => ({ label: key.replaceAll("_", " ").toUpperCase(), status: feed.status === "LIVE_QUIET" ? "LIVE / QUIET" : feed.status, age: feed.age_ms, detail: `${feed.source} · ${feed.methodology || "OBSERVED"} · ${feed.confidence || "REAL"} · ${feed.detail}${feed.timestamp ? ` · event ${feed.timestamp}` : ""}` })) : feedRows();
  const critical = rows.slice(0, 6), degraded = critical.some(row => !row.status.startsWith("LIVE"));
  setText("health-pill", degraded ? "DEGRADED" : "LIVE"); setClass("health-pill", `pill ${degraded ? "warning" : ""}`);
  $("health-grid").innerHTML = rows.map(row => `<div class="health-card ${row.status.startsWith("LIVE") ? "good" : "warn"}"><div class="health-title"><h3>${row.label}</h3><span class="health-state ${row.status.startsWith("LIVE") ? "good-text" : "warning-text"}">${row.status}</span></div><div class="health-meta"><span>${esc(row.detail)}</span><span class="health-age">${row.age == null ? "age —" : `age ${(row.age / 1000).toFixed(1)}s`}</span></div></div>`).join("");
  setClass("global-live-dot", `live-dot ${degraded ? "warning" : ""}`); setText("sidebar-health", degraded ? "DEGRADED" : "LIVE"); setClass("sidebar-health-ring", `status-ring ${degraded ? "warning" : ""}`); setText("sidebar-health-detail", degraded ? "Decision gate locked" : "Validated feeds active");
}

function renderHealthLog() { if ($("health-log")) $("health-log").innerHTML = state.events.map(event => `<div class="log-line"><b>${event.time.toISOString().slice(11, 19)} UTC</b><span class="${event.kind === "good" ? "log-good" : ""}">${esc(event.message)}</span></div>`).join("") || `<div class="empty-state">No system events</div>`; }

function renderDecision() {
  const metrics = bookMetrics(), now = Date.now(), backendLiq = state.backendHealth?.feeds?.liquidations?.status, wsLive = backendLiq ? backendLiq.startsWith("LIVE") : state.wsConnected && now - state.lastWsAt < CONFIG.freshness.ws;
  const checks = [
    { label: "Futures trade stream", ok: state.lastTradeAt && now - state.lastTradeAt < CONFIG.freshness.trade, value: state.lastTradeAt ? "LIVE" : "WAITING" },
    { label: "L2 sequence validity", ok: metrics.valid && now - state.lastBookAt < CONFIG.freshness.book, value: metrics.valid ? "VALID" : "INVALID" },
    { label: "Observed liquidation feed", ok: wsLive, value: wsLive ? "LIVE / QUIET" : "UNAVAILABLE" },
    { label: "Historical calibration", ok: false, value: "UNAVAILABLE" },
    { label: "Execution authorization", ok: false, value: "LOCKED" }
  ];
  $("decision-checks").innerHTML = checks.map(check => `<div class="check-row"><span>${check.label}</span><b class="${check.ok ? "ok" : "bad"}">${check.value}</b></div>`).join(""); setText("decision-reason", "Observed flow is displayed. Historical calibration is unavailable; execution remains locked.");
}

const SIZE_BUCKETS = [[0, 10, "0–10 BTC"], [10, 20, "10–20 BTC"], [20, 50, "20–50 BTC"], [50, 100, "50–100 BTC"], [100, 150, "100–150 BTC"], [150, 300, "150–300 BTC"], [300, Infinity, "300+ BTC"]];
function intelligenceFresh(metrics) {
  const now = Date.now(), backendFeeds = state.backendHealth?.feeds;
  const bookLive = backendFeeds ? String(backendFeeds.orderbook?.status || "").startsWith("LIVE") && state.lastBookAt && now - state.lastBookAt < CONFIG.freshness.book : state.book.valid && now - state.lastBookAt < CONFIG.freshness.book;
  const tradeLive = backendFeeds ? String(backendFeeds.trades?.status || "").startsWith("LIVE") && state.lastTradeAt && now - state.lastTradeAt < CONFIG.freshness.trade : state.lastTradeAt && now - state.lastTradeAt < CONFIG.freshness.trade;
  const wsLive = backendFeeds ? bookLive && tradeLive : state.wsConnected && now - state.lastWsAt < CONFIG.freshness.ws;
  return Boolean(metrics.valid && bookLive && tradeLive && wsLive);
}

function lifecycleFor(row, side) {
  return state.lifecycle.get(lifecycleKey(side, String(row.p))) || { firstSeen: state.lastBookSyncAt || Date.now(), lastAddedAt: state.lastBookSyncAt || Date.now(), addCount: 0, removeCount: 0, refillCount: 0, addedQuantity: 0, removedQuantity: 0, replenishedQuantity: 0, consumedQuantity: 0, pulledQuantity: 0, visibleQuantity: row.q, peakQuantity: row.q, lastSeen: Date.now() };
}

function summarizeLevels(rows, side, now = Date.now()) {
  const levels = rows.map(row => ({ ...row, life: lifecycleFor(row, side) }));
  const btc = levels.reduce((sum, row) => sum + row.q, 0), notional = levels.reduce((sum, row) => sum + row.p * row.q, 0);
  const lifetimes = levels.map(row => Math.max(0, now - Number(row.life.firstSeen || now)));
  const base = state.mark || state.price || levels[0]?.p || 1, distances = levels.map(row => Math.abs(row.p - base) / base * 10000);
  return { levels, count: levels.length, btc, notional, added: levels.reduce((s, r) => s + Number(r.life.addedQuantity || 0), 0), removed: levels.reduce((s, r) => s + Number(r.life.removedQuantity || 0), 0), replenished: levels.reduce((s, r) => s + Number(r.life.replenishedQuantity || 0), 0), pulled: levels.reduce((s, r) => s + Number(r.life.pulledQuantity || 0), 0), oldest: lifetimes.length ? Math.max(...lifetimes) : 0, newest: levels.length ? Math.min(...levels.map(row => now - Number(row.life.lastAddedAt || now))) : 0, averageLifetime: lifetimes.length ? lifetimes.reduce((s, v) => s + v, 0) / lifetimes.length : 0, nearestBps: distances.length ? Math.min(...distances) : null, averageDistanceBps: distances.length ? distances.reduce((s, v) => s + v, 0) / distances.length : null, largest: levels.sort((a, b) => b.q - a.q)[0] || null };
}

function bucketSummaries(metrics) {
  return SIZE_BUCKETS.map(([min, max, label]) => {
    const bid = summarizeLevels(metrics.bids.filter(row => row.q >= min && row.q < max), "bid"), ask = summarizeLevels(metrics.asks.filter(row => row.q >= min && row.q < max), "ask");
    const total = bid.btc + ask.btc;
    return { min, max, label, bid, ask, dominance: total ? (bid.btc - ask.btc) / total * 100 : 0 };
  });
}

function clusterSummaries(metrics) {
  const current = state.mark || state.price || metrics.mid, step = current * 0.0005, structure = marketStructure(), clusters = [];
  for (const [side, rows] of [["BID", metrics.bids.filter(row => row.p < current)], ["ASK", metrics.asks.filter(row => row.p > current)]]) {
    const groups = new Map();
    rows.slice(0, 180).forEach(row => { const key = Math.round((row.p - current) / step); const group = groups.get(key) || []; group.push(row); groups.set(key, group); });
    for (const [key, group] of groups) {
      const summary = summarizeLevels(group, side === "BID" ? "bid" : "ask"), low = Math.min(...group.map(row => row.p)), high = Math.max(...group.map(row => row.p)), notional = summary.notional;
      if (!notional) continue;
      const life = summary.averageLifetime / 1000, removedRatio = summary.removed / Math.max(summary.added, summary.added + summary.removed, 1), replenishment = summary.replenished / Math.max(summary.added, 1);
      let status = "UNCONFIRMED";
      if (removedRatio > .7 && life < 10) status = "SPOOF_WATCH";
      else if (replenishment > .25) status = "REPLENISHING_WALL";
      else if (life > 8 && summary.largest?.q > Math.max(10, current * 0.00001)) status = "PERSISTENT_WALL";
      clusters.push({ side, low, high, price: side === "ASK" ? low : high, distancePct: Math.abs((side === "ASK" ? low : high) - current) / current * 100, distanceBps: Math.abs((side === "ASK" ? low : high) - current) / current * 10000, distanceAtr: structure.atr ? Math.abs((side === "ASK" ? low : high) - current) / structure.atr : null, btc: summary.btc, notional, density: notional / Math.max(high - low, current * .0001), persistence: life, replenishment, depletion: summary.removed, status, summary });
    }
  }
  return clusters.sort((a, b) => a.distanceBps - b.distanceBps);
}

function recentFlow() {
  const cutoff = Date.now() - 60000, trades = state.trades.filter(row => row.timestamp >= cutoff);
  if (trades.length) return { buy: trades.filter(row => row.buy).reduce((s, r) => s + r.notional, 0), sell: trades.filter(row => !row.buy).reduce((s, r) => s + r.notional, 0), count: trades.length, volume: trades.reduce((s, r) => s + r.quantity, 0) };
  const rows = [...state.buckets.values()].filter(row => row.time >= cutoff);
  return { buy: rows.reduce((s, r) => s + Number(r.buy || 0), 0), sell: rows.reduce((s, r) => s + Number(r.sell || 0), 0), count: rows.reduce((s, r) => s + Number(r.count || 0), 0), volume: 0 };
}

function marketStructure() {
  const rows = state.klines.filter(row => row.closed).slice(-40), recent = rows.slice(-20), previous = rows.slice(-40, -20), current = state.price || state.mark || rows.at(-1)?.close;
  if (!recent.length || !current) return { atr: null, swingHigh: null, swingLow: null, equalHigh: null, equalLow: null, label: "UNAVAILABLE" };
  const tr = recent.map((row, i) => { const prior = recent[i - 1]?.close || row.open; return Math.max(row.high - row.low, Math.abs(row.high - prior), Math.abs(row.low - prior)); }), atr = tr.reduce((s, value) => s + value, 0) / tr.length, swingHigh = Math.max(...recent.map(row => row.high)), swingLow = Math.min(...recent.map(row => row.low)), highs = [...recent].sort((a, b) => b.high - a.high), lows = [...recent].sort((a, b) => a.low - b.low), equalHigh = highs[1] && Math.abs(highs[0].high - highs[1].high) / highs[0].high < .001 ? (highs[0].high + highs[1].high) / 2 : null, equalLow = lows[1] && Math.abs(lows[0].low - lows[1].low) / lows[0].low < .001 ? (lows[0].low + lows[1].low) / 2 : null, previousHigh = previous.length ? Math.max(...previous.map(row => row.high)) : null, previousLow = previous.length ? Math.min(...previous.map(row => row.low)) : null;
  return { atr, swingHigh, swingLow, equalHigh, equalLow, bos: previousHigh && current > previousHigh ? "BOS_UP" : previousLow && current < previousLow ? "BOS_DOWN" : "NO_BOS", label: current > (rows.at(-2)?.close || current) ? "HH/HL BIAS" : current < (rows.at(-2)?.close || current) ? "LH/LL BIAS" : "RANGE" };
}

function flowIntelligence(flow) {
  const trades = state.trades.filter(row => row.timestamp >= Date.now() - 60000), volumes = [...state.buckets.values()].map(row => Number(row.buy || 0) + Number(row.sell || 0)).filter(value => value > 0).slice(-21), currentVolume = volumes.at(-1) || 0, history = volumes.slice(0, -1), mean = history.length ? history.reduce((s, v) => s + v, 0) / history.length : 0, sd = history.length > 1 ? Math.sqrt(history.reduce((s, v) => s + (v - mean) ** 2, 0) / history.length) : 0, z = sd ? (currentVolume - mean) / sd : null, threshold = Math.max(250000, trades.length ? trades.reduce((s, row) => s + row.notional, 0) / trades.length * 5 : 250000), whales = trades.filter(row => row.notional >= threshold).reduce((s, row) => s + row.notional * (row.buy ? 1 : -1), 0), first = trades.at(-1)?.price, last = trades.at(0)?.price;
  return { delta: flow.buy - flow.sell, intensity: flow.count / 60, whaleFlow: whales, volumeZ: z, priceResponseBps: first && last ? (last - first) / first * 10000 : null };
}

function estimatedStopZones(metrics, clusters) {
  const current = state.mark || state.price || metrics.mid, oi = Number(state.oi || 0), prior = oi ? oi * .00035 : null, structure = marketStructure();
  const ask = clusters.find(row => row.side === "ASK"), bid = clusters.find(row => row.side === "BID"), above = [ask?.price, structure.equalHigh, structure.swingHigh].filter(price => price && price > current).sort((a, b) => a - b)[0], below = [bid?.price, structure.equalLow, structure.swingLow].filter(price => price && price < current).sort((a, b) => b - a)[0], askSize = ask?.price === above ? ask.btc : prior, bidSize = bid?.price === below ? bid.btc : prior;
  const up = above ? { price: above, btc: askSize, source: `ESTIMATED · ${ask?.price === above ? "visible cluster" : "swing/equal high"} + OI/funding context` } : null;
  const down = below ? { price: below, btc: bidSize, source: `ESTIMATED · ${bid?.price === below ? "visible cluster" : "swing/equal low"} + OI/funding context` } : null;
  return { up, down };
}

function renderIntelligence() {
  if (!$("intel-current-price")) return;
  const metrics = bookMetrics(), shell = $("intel-shell"), stale = !intelligenceFresh(metrics), now = Date.now();
  $("intel-stale").hidden = !stale; shell?.classList.toggle("suppressed", stale);
  if (stale) {
    setText("intel-health-label", metrics.valid ? "STALE DATA · OUTPUT SUPPRESSED" : "WAITING FOR VALIDATED FEEDS"); setText("intel-health-meta", `source BINANCE USD-M FUTURES · age ${ago(state.lastBookAt)} · sequence ${state.book.lastUpdateId || "—"}`); setClass("intel-health-dot", "status-dot bad"); setText("intel-summary-status", "STALE / SUPPRESSED");
    ["intel-current-price","intel-short-zone","intel-long-zone","intel-pressure","intel-short-distance","intel-long-distance","intel-short-liquidity","intel-long-liquidity","intel-pressure-detail","intel-scenario-title","intel-scenario-text","intel-chain","intel-target","intel-invalidation"].forEach(id => setText(id, "—")); setText("intel-confirmation", "STALE"); setText("intel-pressure-pill", "STALE"); setClass("intel-confirmation", "pill warning"); setClass("intel-pressure-pill", "pill warning");
    $("orderbook-summary-table").innerHTML = `<tr><td colspan="8" class="empty-state">STALE DATA — OUTPUT SUPPRESSED</td></tr>`; $("size-buckets-table").innerHTML = `<tr><td colspan="12" class="empty-state">STALE DATA — OUTPUT SUPPRESSED</td></tr>`; $("clusters-table").innerHTML = `<tr><td colspan="8" class="empty-state">STALE DATA — OUTPUT SUPPRESSED</td></tr>`; return;
  }
  const flow = recentFlow(), flowStats = flowIntelligence(flow), clusters = clusterSummaries(metrics), zones = estimatedStopZones(metrics, clusters), pressure = metrics.weightedImbalance, flowDelta = flowStats.delta, aligned = Math.sign(pressure) === Math.sign(flowDelta) && Math.abs(pressure) > 5 && flow.count > 0;
  setClass("intel-health-dot", "status-dot good"); setText("intel-health-label", "VALIDATED FEEDS · LIVE"); setText("intel-health-meta", `source BINANCE USD-M FUTURES · age ${ago(state.lastBookAt)} · sequence ${state.book.lastUpdateId || "—"}`); setText("intel-summary-status", "LIVE / OBSERVED");
  setText("intel-current-price", usd(state.price || metrics.mid)); setText("intel-price-context", `Mark ${usd(state.mark)} · Index ${usd(state.index)}`);
  const pressureLabel = pressure > 5 ? "BID" : pressure < -5 ? "ASK" : "BALANCED"; setText("intel-pressure", pressureLabel); setText("intel-pressure-pill", pressureLabel); setText("intel-pressure-detail", `Weighted imbalance ${pressure >= 0 ? "+" : ""}${pressure.toFixed(1)}% · microprice ${fmt(metrics.microprice)}`); $("intel-pressure-fill").style.left = `${Math.max(2, Math.min(98, 50 + pressure / 2))}%`;
  const confirmation = aligned ? "MIXED" : "UNCONFIRMED"; setText("intel-confirmation", confirmation); setClass("intel-confirmation", `pill ${aligned ? "warning" : "neutral"}`);
  const zoneCard = (zone, priceId, distanceId, liqId) => { const base = state.price || metrics.mid; setText(priceId, zone ? usd(zone.price) : "UNAVAILABLE"); setText(distanceId, zone ? `Distance: ${((zone.price - base) / base * 100).toFixed(2)}% · ${(Math.abs(zone.price - base) / base * 10000).toFixed(1)} bps` : "Distance: UNAVAILABLE"); setText(liqId, zone ? `Estimated liquidity: ${fmt(zone.btc, 2)} BTC · ${usd(zone.price * zone.btc)} · ${zone.source}` : "Estimated liquidity: UNAVAILABLE"); };
  zoneCard(zones.up, "intel-short-zone", "intel-short-distance", "intel-short-liquidity"); zoneCard(zones.down, "intel-long-zone", "intel-long-distance", "intel-long-liquidity");
  const metricsHtml = [["Best bid", usd(metrics.bids[0]?.p), "OBSERVED · bookTicker/L2"],["Best ask", usd(metrics.asks[0]?.p), "OBSERVED · bookTicker/L2"],["Spread", `${fmt(metrics.spread)} · ${fmt(metrics.spreadBps, 2)} bps`, "DERIVED · best bid/ask"],["Visible bid", `${fmt(metrics.bidBtc, 2)} BTC · ${metrics.bidLevels} levels`, "REAL · aggregated L2"],["Visible ask", `${fmt(metrics.askBtc, 2)} BTC · ${metrics.askLevels} levels`, "REAL · aggregated L2"],["Depth notional", `${usd(metrics.bidNotional)} / ${usd(metrics.askNotional)}`, "DERIVED · full visible book"],["Bid/ask ratio", `${(metrics.bidBtc / Math.max(metrics.askBtc, 1)).toFixed(2)}x`, "DERIVED · visible BTC"],["Microprice", fmt(metrics.microprice), "DERIVED · top-of-book"],["Aggression", `${usd(flow.buy)} buy / ${usd(flow.sell)} sell`, "DERIVED · real trades · 60s"],["Delta / CVD", `${usd(flowStats.delta)} / ${usd(state.cvd)}`, "DERIVED · isBuyerMaker rule"],["Intensity / whale", `${fmt(flowStats.intensity, 2)} t/s · ${usd(flowStats.whaleFlow)}`, "DERIVED · thresholded trades"],["Price response / z", `${flowStats.priceResponseBps == null ? "—" : `${flowStats.priceResponseBps.toFixed(1)} bp`} · ${flowStats.volumeZ == null ? "—" : flowStats.volumeZ.toFixed(2)}`, "DERIVED · 60s / volume history"],["Data age", ago(Math.max(state.lastTradeAt || 0, state.lastBookAt || 0)), "RECEIVED · freshness SLA"],["Sequence gap", `${state.book.resyncs} resync(s)`, "VALIDATION · REST resync on gap"],["WebSocket", state.wsConnected || state.backendHealth ? "LIVE / CONNECTED" : "DISCONNECTED", "STATUS · Futures streams"],["Participant identity", "UNAVAILABLE", "UNAVAILABLE — aggregated depth does not identify unique traders"]];
  $("intel-metrics").innerHTML = metricsHtml.map(([label, value, meta]) => `<div class="intel-metric"><span>${label}</span><b>${esc(value)}</b><small>${esc(meta)}</small></div>`).join("");
  const scenarioBull = pressure > 5 && flowDelta >= 0, scenarioBear = pressure < -5 && flowDelta <= 0, title = scenarioBull ? "Bid-side pressure with bullish flow alignment" : scenarioBear ? "Ask-side pressure with bearish flow alignment" : pressure > 5 ? "Bid side is stronger, but flow confirmation is mixed" : pressure < -5 ? "Ask side is stronger, but flow confirmation is mixed" : "Balanced displayed liquidity · no directional confirmation";
  const scenarioText = scenarioBull ? `Bid depth is stronger and aggressive buy flow is aligned. This is a measured displayed-pressure scenario; it is not a directional certainty.` : scenarioBear ? `Ask depth is stronger and aggressive sell flow is aligned. This is a measured displayed-pressure scenario; it is not a directional certainty.` : pressure > 5 ? `Bid side is stronger than ask side, but executed flow is not confirming. Treat current bids as passive support; absorption is not yet confirmed.` : pressure < -5 ? `Ask side is stronger than bid side, but executed flow is not confirming. Treat current asks as passive resistance; initiative selling is not yet confirmed.` : `Visible depth and executed flow are not sufficiently asymmetric for a directional scenario.`;
  setText("intel-scenario-title", title); setText("intel-scenario-text", scenarioText);
  const primary = scenarioBull ? zones.up : zones.down, obstacle = scenarioBull ? clusters.find(row => row.side === "ASK") : clusters.find(row => row.side === "BID");
  $("intel-chain").innerHTML = [["Current state", pressure > 5 ? "Bid liquidity is visible" : pressure < -5 ? "Ask liquidity is visible" : "Depth is balanced"],["Cause", `${flow.count} trades · ${flowDelta >= 0 ? "aggressive buy" : "aggressive sell"} delta ${usd(Math.abs(flowDelta))}`],["Trigger", primary ? `${primary.side === "ASK" ? "Ask" : "Bid"} cluster at ${usd(primary.price)} is consumed / replenished` : "Validated cluster required"],["Immediate result", primary ? `Acceptance or rejection at ${usd(primary.price)}` : "No verified path"],["Next effect", primary ? `Stop pressure is estimated near ${usd(primary.price)}` : "No estimated cascade path"],["Obstacle", obstacle ? `${obstacle.status} at ${usd(obstacle.price)}` : "UNAVAILABLE"]].map(([label, value]) => `<div class="chain-step"><b>${label}</b>${esc(value)}</div>`).join("");
  $("intel-target").innerHTML = primary ? `<div class="target-grid"><div><span>TARGET · ${primary.side === "ASK" ? "UP" : "DOWN"}</span><b>${usd(primary.price)}</b></div><div><span>P(TARGET TOUCH)</span><b>UNTRAINED</b></div><div><span>ETTT</span><b>INSUFFICIENT SAMPLE</b></div><div><span>TRIGGER</span><b>${usd(primary.price)} interaction</b></div><div><span>OBSTACLE</span><b>${obstacle ? usd(obstacle.price) : "UNAVAILABLE"}</b></div><div><span>CASCADE RISK</span><b>${state.oi ? "ESTIMATED · OI CONTEXT" : "UNAVAILABLE"}</b></div></div>` : `<div class="empty-state">TARGET UNAVAILABLE · no validated liquidity path</div>`;
  setText("intel-invalidation", primary ? `If trigger fails: replenishment rises + ${flowDelta >= 0 ? "aggressive buy weakens" : "aggressive sell weakens"} → scenario invalidated. Probability: UNCALIBRATED.` : "Trigger state unavailable.");
  const bidShare = metrics.bidNotional / Math.max(metrics.bidNotional + metrics.askNotional, 1) * 100, askShare = 100 - bidShare;
  $("orderbook-summary-table").innerHTML = [["BID",metrics.bidLevels,metrics.bidBtc,metrics.bidNotional,bidShare,metrics.bids[0]?.p, summarizeLevels(metrics.bids,"bid").added,summarizeLevels(metrics.bids,"bid").removed],["ASK",metrics.askLevels,metrics.askBtc,metrics.askNotional,askShare,metrics.asks[0]?.p,summarizeLevels(metrics.asks,"ask").added,summarizeLevels(metrics.asks,"ask").removed]].map(row => `<tr><td class="${row[0] === "BID" ? "buy-text" : "sell-text"}">${row[0]}</td><td>${fmt(row[1],0)}</td><td>${fmt(row[2],2)}</td><td>${usd(row[3])}</td><td>${fmt(row[4],1)}%</td><td>${usd(row[5])}</td><td>${fmt(row[6],2)}</td><td>${fmt(row[7],2)}</td></tr>`).join("");
  $("size-buckets-table").innerHTML = bucketSummaries(metrics).map(row => { const largest = Math.max(row.bid.largest?.q || 0, row.ask.largest?.q || 0); return `<tr><td>${row.label}</td><td>${row.bid.count}</td><td>${fmt(row.bid.btc,2)}</td><td>${usd(row.bid.notional)}</td><td>${row.ask.count}</td><td>${fmt(row.ask.btc,2)}</td><td>${usd(row.ask.notional)}</td><td class="${row.dominance >= 0 ? "buy-text" : "sell-text"}">${row.dominance >= 0 ? "+" : ""}${fmt(row.dominance,1)}%</td><td>${fmt(row.bid.nearestBps ?? row.ask.nearestBps,1)} / ${fmt(((row.bid.averageDistanceBps || 0) + (row.ask.averageDistanceBps || 0)) / 2,1)} bp</td><td>${fmt(largest,2)} BTC</td><td>${fmt(Math.max(row.bid.averageLifetime,row.ask.averageLifetime) / 1000,1)}s</td><td class="muted-cell">${fmt(row.bid.added + row.ask.added,1)} / ${fmt(row.bid.removed + row.ask.removed,1)} / ${fmt(row.bid.replenished + row.ask.replenished,1)} / ${fmt(row.bid.pulled + row.ask.pulled,1)}</td></tr>`; }).join("");
  $("clusters-table").innerHTML = clusters.slice(0, 8).map(row => `<tr><td class="${row.side === "BID" ? "buy-text" : "sell-text"}">${row.side}</td><td>${usd(row.low)}–${usd(row.high)}</td><td>${fmt(row.distancePct,2)}% / ${fmt(row.distanceBps,1)} bp / ${row.distanceAtr == null ? "—" : `${fmt(row.distanceAtr,2)} ATR`}</td><td>${fmt(row.btc,2)}</td><td>${usd(row.notional)}</td><td>${fmt(row.persistence,1)}s</td><td>${row.replenishment > .25 ? "REPLENISHING" : row.depletion > 0 ? "DEPLETING" : "MIXED"}</td><td class="${row.status === "SPOOF_WATCH" ? "subtle-warning" : ""}">${row.status}</td></tr>`).join("") || `<tr><td colspan="8" class="empty-state">No nearby visible cluster in current book</td></tr>`;
}

function renderAll() { renderHeader(); renderBook(); renderTape(); renderFlow(); renderLiquidations(); renderDerivatives(); renderContext(); renderHealth(); renderDecision(); renderIntelligence(); }

function setupNav() {
  document.querySelectorAll(".nav-item").forEach(button => button.addEventListener("click", () => {
    document.querySelectorAll(".nav-item").forEach(item => item.classList.remove("active")); document.querySelectorAll(".view-panel").forEach(panel => panel.classList.remove("active")); button.classList.add("active"); $("view-" + button.dataset.view).classList.add("active");
    if (button.dataset.view === "execution") setTimeout(() => state.chart?.resize($("price-chart").clientWidth, 405), 50);
  }));
  document.querySelectorAll(".tf-btn").forEach(button => button.addEventListener("click", async () => {
    document.querySelectorAll(".tf-btn").forEach(item => item.classList.remove("active")); button.classList.add("active"); state.interval = button.dataset.interval; setText("chart-interval", state.interval);
    try {
      const rows = await json(`${CONFIG.rest}/klines?symbol=${CONFIG.symbol}&interval=${state.interval}&limit=500`); state.klines = rows.filter(row => Number(row[6]) < Date.now()).map(row => ({ time: Number(row[0]) / 1000, open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), volume: Number(row[7]), closed: true })); renderChart(); renderContext(); log(`Switched to ${state.interval} closed Futures candles`, "good");
    } catch (error) { log(`Interval load failed: ${error.message}`, "warn"); }
  }));
  $("refresh-btn").addEventListener("click", () => { state.book.valid = false; state.book.queue = []; syncBook(); loadDerivatives(); loadHistory(); });
  $("clear-log").addEventListener("click", () => { state.events = []; renderHealthLog(); });
}

async function boot() {
  setupNav(); initChart(); renderAll();
  if (CONFIG.backend) {
    log(`Backend mode enabled: ${CONFIG.backend}`, "good"); await pollBackendSnapshot(); setInterval(pollBackendSnapshot, 1000);
  } else {
    await Promise.all([loadHistory(), loadDerivatives()]); openStream(); setInterval(loadDerivatives, 30000); setInterval(pollKline, 5000); pollKline();
  }
  setInterval(renderHealth, 1000); setInterval(renderHeader, 1000); setInterval(renderDecision, 1000); setInterval(renderIntelligence, 1000); setInterval(renderLiquidations, 5000); renderAll(); log("Terminal booted in read-only research mode", "good");
}

boot();
