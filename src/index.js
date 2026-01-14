require('dotenv').config();
const http = require('http');
const WebSocket = require('ws');
const fetch = require('cross-fetch');

const DEFAULT_SYMBOL_REFRESH_MS = 60_000;
const DEFAULT_WS_RETRY_MS = 5_000;
const MAX_WS_RETRY_MS = 60_000;

const DEFAULT_VOLUME_WINDOW_MS = 15 * 60_000;
const DEFAULT_VOLUME_THRESHOLD_USD = 400_000;
const DEFAULT_FUTURES_VOLUME_THRESHOLD_USD = 600_000;
const DEFAULT_NOTIFICATION_COOLDOWN_MS = 15 * 60_000;

// Major coins to exclude from volume tracking
const EXCLUDED_SYMBOLS = new Set([
  'btcusdt',
  'ethusdt',
  'solusdt',
  'bnbusdt',
  'bchusdt',
  'xrpusdt',
  'linkusdt',
  'dogeusdt',
  'usdcusdt',
  'usdtusdt',
  'wbtcusdt',
  'usd1usdt',
  'fdusdusdt',
]);

function assertEnv(name, optional) {
  const value = process.env[name];

  if (!optional && (value === undefined || value === '')) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function createLogger(level) {
  const levels = ['error', 'warn', 'info', 'debug'];
  const normalized = level && levels.includes(level) ? level : 'info';
  const currentIndex = levels.indexOf(normalized);

  return levels.reduce((acc, name, index) => {
    acc[name] = (...args) => {
      if (index <= currentIndex) {
        const method = name === 'debug' ? 'log' : name;
        console[method](`[${name.toUpperCase()}]`, ...args);
      }
    };
    return acc;
  }, {});
}

function normalizeBaseUrl(url) {
  if (!url) return url;
  return url.endsWith('/') ? url.slice(0, -1) : url;
}

function parseSymbols(raw) {
  if (!raw) return [];
  return String(raw)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => item.toLowerCase())
    .filter((s) => !EXCLUDED_SYMBOLS.has(s));
}

function buildBinanceWsUrl(symbols, customUrl, type = 'spot') {
  const defaultBase = type === 'futures' 
    ? 'wss://fstream.binance.com'
    : 'wss://stream.binance.com:9443';
  const base = customUrl || defaultBase;
  if (!symbols.length) return undefined;

  if (symbols.length === 1) {
    return `${base}/ws/${symbols[0]}@trade`;
  }

  const streams = symbols.map((symbol) => `${symbol}@trade`).join('/');
  return `${base}/stream?streams=${streams}`;
}

function parseTradeMessage(messageBuffer, log) {
  try {
    const payload = JSON.parse(messageBuffer.toString());
    const data = payload.data || payload;

    const symbol = data.s ? String(data.s).toLowerCase() : undefined;
    const tradeTime = Number(data.T ?? data.E ?? Date.now());
    const price = Number(data.p);
    const qty = Number(data.q);

    if (!symbol || !Number.isFinite(price) || !Number.isFinite(qty)) {
      return undefined;
    }

    const quoteUsd = price * qty; // USDT pairs => approx USD
    if (!Number.isFinite(quoteUsd)) {
      return undefined;
    }

    return { symbol, tradeTime, quoteUsd };
  } catch (error) {
    // Silently ignore parse errors
    return undefined;
  }
}

async function fetchTrackedSymbols(baseUrl, token, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/volume-alert/symbols`;

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to fetch symbols (${response.status} ${response.statusText}): ${text}`
    );
  }

  const payload = await response.json();
  const symbols = Array.isArray(payload) ? payload : payload.symbols || [];

  if (!Array.isArray(symbols)) {
    throw new Error('Symbols endpoint returned unexpected payload');
  }

  return symbols
    .map((s) => String(s).toLowerCase())
    .filter(Boolean)
    .filter((s) => s.endsWith('usdt'))
    .filter((s) => !EXCLUDED_SYMBOLS.has(s));
}

async function fetchFuturesSymbols(baseUrl, token, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/volume-alert/futures-symbols`;

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to fetch futures symbols (${response.status} ${response.statusText}): ${text}`
    );
  }

  const payload = await response.json();
  const symbols = Array.isArray(payload) ? payload : payload.symbols || [];

  if (!Array.isArray(symbols)) {
    throw new Error('Futures symbols endpoint returned unexpected payload');
  }

  return symbols
    .map((s) => String(s).toLowerCase())
    .filter(Boolean)
    .filter((s) => s.endsWith('usdt'))
    .filter((s) => !EXCLUDED_SYMBOLS.has(s));
}

async function broadcastVolumeAlert(baseUrl, token, symbol, volumeUsd, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/push/volume`;

  log.info('Broadcasting volume alert', {
    symbol,
    volumeUsd,
  });

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      symbol,
      volumeUsd,
      windowMinutes: 15,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to broadcast volume alert (${response.status} ${response.statusText}): ${text}`
    );
  }

  const result = await response.json();
  const successful = result.successful || result.total || 0;
  const total = result.total || 0;
  const emails = result.successfulEmails || [];
  
  // Console log for sent notifications
  const emailList = emails.length > 0 ? ` | Users: ${emails.join(', ')}` : '';
  console.log(`📢 NOTIFICATION SENT: ${symbol.toUpperCase()} | Volume: $${Math.round(volumeUsd).toLocaleString()} | Sent to: ${successful}/${total} users${emailList}`);

  return result;
}

async function broadcastFuturesVolumeAlert(baseUrl, token, symbol, volumeUsd, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/push/futures-volume`;

  log.info('Broadcasting futures volume alert', {
    symbol,
    volumeUsd,
  });

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      symbol,
      volumeUsd,
      windowMinutes: 15,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to broadcast futures volume alert (${response.status} ${response.statusText}): ${text}`
    );
  }

  const result = await response.json();
  const successful = result.successful || result.total || 0;
  const total = result.total || 0;
  const emails = result.successfulEmails || [];
  
  // Console log for sent notifications
  const emailList = emails.length > 0 ? ` | Users: ${emails.join(', ')}` : '';
  console.log(`📢 FUTURES NOTIFICATION SENT: ${symbol.toUpperCase()} | Volume: $${Math.round(volumeUsd).toLocaleString()} | Sent to: ${successful}/${total} users${emailList}`);

  return result;
}

function createRollingWindow(windowMs) {
  const entries = [];
  let sum = 0;

  function add(ts, value) {
    entries.push({ ts, value });
    sum += value;
  }

  function prune(now) {
    const cutoff = now - windowMs;
    while (entries.length && entries[0].ts < cutoff) {
      const item = entries.shift();
      sum -= item.value;
    }
  }

  function getSum() {
    return sum;
  }

  return { add, prune, getSum };
}

async function startWorker(config) {
  const {
    baseUrl,
    workerApiToken,
    pushTriggerToken,
    log,
    symbolRefreshMs,
    volumeWindowMs,
    volumeThresholdUsd,
    notificationCooldownMs,
    type = 'spot', // 'spot' or 'futures'
  } = config;

  let ws;
  let reconnectAttempts = 0;
  let closedByUser = false;

  let trackedSymbols = parseSymbols(process.env.BINANCE_SYMBOLS);
  const explicitSymbols = trackedSymbols.length > 0;

  const windows = new Map(); // symbol -> rolling window
  const lastBroadcastAt = new Map(); // symbol -> timestamp

  function getWindow(symbol) {
    if (!windows.has(symbol)) {
      windows.set(symbol, createRollingWindow(volumeWindowMs));
    }
    return windows.get(symbol);
  }

  function cleanupWindows(validSymbols) {
    const keep = new Set(validSymbols);
    for (const key of windows.keys()) {
      if (!keep.has(key)) {
        windows.delete(key);
        lastBroadcastAt.delete(key);
      }
    }
  }

  function symbolsEqual(a, b) {
    if (a.length !== b.length) return false;
    const setA = new Set(a);
    for (let i = 0; i < b.length; i += 1) {
      if (!setA.has(b[i])) return false;
    }
    return true;
  }

  async function refreshSymbols() {
    if (explicitSymbols) return;

    try {
      const symbols = type === 'futures' 
        ? await fetchFuturesSymbols(baseUrl, workerApiToken, log)
        : await fetchTrackedSymbols(baseUrl, workerApiToken, log);
      const unique = Array.from(new Set(symbols)).sort();

      log.info(`Tracked ${type} symbols`, { count: unique.length, symbols: unique });

      if (!symbolsEqual(unique, trackedSymbols)) {
        trackedSymbols = unique;
        cleanupWindows(trackedSymbols);
        reconnect(true);
      }
    } catch (error) {
      log.error(`Failed to refresh ${type} symbols`, { error });
    }
  }

  function scheduleSymbolRefresh() {
    setTimeout(async () => {
      await refreshSymbols();
      scheduleSymbolRefresh();
    }, symbolRefreshMs);
  }

  async function handleTrade(symbol, tradeTime, quoteUsd) {
    const now = Date.now();
    const window = getWindow(symbol);
    window.add(tradeTime || now, quoteUsd);
    window.prune(now);

    const sum = window.getSum();

    if (sum < volumeThresholdUsd) {
      return;
    }

    // CRITICAL: Check cooldown FIRST before any processing
    // Aynı coin için 15 dakika içinde tekrar bildirim gönderilmemeli
    const last = lastBroadcastAt.get(symbol);
    if (last) {
      const elapsedMs = now - last;
      if (elapsedMs < notificationCooldownMs) {
        return; // CRITICAL: Exit early, do NOT send notification (cooldown active)
      }
    }

    // CRITICAL: Set cooldown IMMEDIATELY before sending to prevent race conditions
    // Bu sayede aynı anda gelen trade'ler için de cooldown aktif olur
    lastBroadcastAt.set(symbol, now);
    
    try {
      if (type === 'futures') {
        await broadcastFuturesVolumeAlert(baseUrl, pushTriggerToken, symbol, sum, log);
      } else {
        await broadcastVolumeAlert(baseUrl, pushTriggerToken, symbol, sum, log);
      }
    } catch (error) {
      log.error(`Failed to broadcast ${type} volume alert`, { symbol, error });
      // Cooldown already set above, so no need to set again
      // This prevents spam even if API call fails
    }
  }

  function reconnect(dueToSymbolsChange) {
    if (ws) {
      log.info('Closing existing WebSocket connection', {
        reason: dueToSymbolsChange ? 'symbols-changed' : 'reconnect',
      });
      reconnectAttempts = 0;
      ws.close();
    } else if (dueToSymbolsChange) {
      connect();
    }
  }

  function connect() {
    if (!trackedSymbols.length) {
      log.warn(`No ${type} symbols to track. Waiting for symbols.`);
      return;
    }

    const url = buildBinanceWsUrl(trackedSymbols, process.env.BINANCE_WS_URL, type);
    if (!url) {
      log.warn(`Unable to determine Binance ${type} WebSocket URL. Skipping connection.`);
      return;
    }

    log.info(`Connecting to Binance ${type} WebSocket`, {
      url,
      symbolCount: trackedSymbols.length,
    });

    ws = new WebSocket(url);

    ws.on('open', () => {
      log.info('Connected to Binance WebSocket');
      reconnectAttempts = 0;
    });

    ws.on('message', async (message) => {
      const parsed = parseTradeMessage(message, log);
      if (!parsed) return;
      await handleTrade(parsed.symbol, parsed.tradeTime, parsed.quoteUsd);
    });

    ws.on('close', (code, reason) => {
      log.warn('Binance WebSocket closed', { code, reason: reason.toString() });
      if (closedByUser) return;
      scheduleReconnect();
    });

    ws.on('error', (error) => {
      log.error('Binance WebSocket error', { error });
      ws.terminate();
    });
  }

  function scheduleReconnect() {
    reconnectAttempts += 1;
    const delay = Math.min(
      DEFAULT_WS_RETRY_MS * 2 ** (reconnectAttempts - 1),
      MAX_WS_RETRY_MS
    );

    log.info('Reconnecting to Binance WebSocket', { delay });
    setTimeout(connect, delay);
  }

  await refreshSymbols();
  scheduleSymbolRefresh();

  if (!ws && trackedSymbols.length > 0) {
    connect();
  }

  return () => {
    closedByUser = true;
    if (ws) ws.close();
  };
}

async function main() {
  const baseUrl = assertEnv('VERCEL_BASE_URL');
  const workerApiToken = assertEnv('WORKER_API_TOKEN');
  const pushTriggerToken = assertEnv('ALERT_TRIGGER_TOKEN');

  const logLevel = assertEnv('LOG_LEVEL', true) || 'info';
  const symbolRefreshMs =
    Number(assertEnv('SYMBOL_REFRESH_INTERVAL_MS', true)) || DEFAULT_SYMBOL_REFRESH_MS;

  const volumeWindowMs =
    Number(assertEnv('VOLUME_WINDOW_MS', true)) || DEFAULT_VOLUME_WINDOW_MS;

  const volumeThresholdUsd =
    Number(assertEnv('VOLUME_THRESHOLD_USD', true)) || DEFAULT_VOLUME_THRESHOLD_USD;

  const futuresVolumeThresholdUsd =
    Number(assertEnv('FUTURES_VOLUME_THRESHOLD_USD', true)) || DEFAULT_FUTURES_VOLUME_THRESHOLD_USD;

  const notificationCooldownMs =
    Number(assertEnv('VOLUME_NOTIFICATION_COOLDOWN_MS', true)) ||
    DEFAULT_NOTIFICATION_COOLDOWN_MS;

  const port = Number(process.env.PORT) || 8080;
  const log = createLogger(logLevel);

  http
    .createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
    })
    .listen(port, () => {
      log.info('Health server listening', { port });
    })
    .on('error', (error) => {
      log.error('Failed to start health server', { error });
    });

  log.info('Starting Oruba 15m volume workers', {
    baseUrl: normalizeBaseUrl(baseUrl),
    symbolRefreshMs,
    volumeWindowMs,
    spotThresholdUsd: volumeThresholdUsd,
    futuresThresholdUsd: futuresVolumeThresholdUsd,
    notificationCooldownMs,
  });

  // Start spot worker
  const stopSpot = await startWorker({
    baseUrl,
    workerApiToken,
    pushTriggerToken,
    log,
    symbolRefreshMs,
    volumeWindowMs,
    volumeThresholdUsd,
    notificationCooldownMs,
    type: 'spot',
  });

  // Start futures worker
  const stopFutures = await startWorker({
    baseUrl,
    workerApiToken,
    pushTriggerToken,
    log,
    symbolRefreshMs,
    volumeWindowMs,
    volumeThresholdUsd: futuresVolumeThresholdUsd,
    notificationCooldownMs,
    type: 'futures',
  });

  function handleExit(signal) {
    log.info(`Received ${signal}. Shutting down gracefully.`);
    stopSpot();
    stopFutures();
    process.exit(0);
  }

  process.on('SIGINT', handleExit);
  process.on('SIGTERM', handleExit);
}

if (require.main === module) {
  main().catch((error) => {
    console.error('[ERROR]', error);
    process.exitCode = 1;
  });
}
