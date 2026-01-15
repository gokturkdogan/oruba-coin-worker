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

async function fetchVolumeSettings(baseUrl, token, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/volume-alert/settings`;

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to fetch volume settings (${response.status} ${response.statusText}): ${text}`
    );
  }

  const payload = await response.json();
  return {
    spotVolumeThreshold: Number(payload.spotVolumeThreshold) || DEFAULT_VOLUME_THRESHOLD_USD,
    futuresVolumeThreshold: Number(payload.futuresVolumeThreshold) || DEFAULT_FUTURES_VOLUME_THRESHOLD_USD,
    updatedAt: payload.updatedAt || null,
  };
}

async function broadcastVolumeAlert(baseUrl, token, symbol, volumeUsd, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/push/volume`;

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
  
  // Beautiful formatted log for sent notifications
  const notificationLog = {
    type: 'SPOT',
    symbol: symbol.toUpperCase(),
    volume: `$${Math.round(volumeUsd).toLocaleString()}`,
    sent: `${successful}/${total} users`,
    users: emails.length > 0 ? emails : ['No users'],
  };
  
  console.log(`📢 SPOT VOLUME NOTIFICATION SENT | ${JSON.stringify(notificationLog)}`);

  return result;
}

async function broadcastFuturesVolumeAlert(baseUrl, token, symbol, volumeUsd, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/push/futures-volume`;

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
  
  // Beautiful formatted log for sent notifications
  const notificationLog = {
    type: 'FUTURES',
    symbol: symbol.toUpperCase(),
    volume: `$${Math.round(volumeUsd).toLocaleString()}`,
    sent: `${successful}/${total} users`,
    users: emails.length > 0 ? emails : ['No users'],
  };
  
  console.log(`📢 FUTURES VOLUME NOTIFICATION SENT | ${JSON.stringify(notificationLog)}`);

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
    initialVolumeThresholdUsd,
    notificationCooldownMs,
    type = 'spot', // 'spot' or 'futures'
  } = config;

  let ws;
  let reconnectAttempts = 0;
  let closedByUser = false;

  let trackedSymbols = parseSymbols(process.env.BINANCE_SYMBOLS);
  const explicitSymbols = trackedSymbols.length > 0;

  // Dynamic volume threshold (updated from API)
  let volumeThresholdUsd = initialVolumeThresholdUsd;
  let lastSettingsUpdatedAt = null; // Track when settings were last updated

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
    if (explicitSymbols) {
      return;
    }

    try {
      const symbols = type === 'futures' 
        ? await fetchFuturesSymbols(baseUrl, workerApiToken, log)
        : await fetchTrackedSymbols(baseUrl, workerApiToken, log);
      const unique = Array.from(new Set(symbols)).sort();

      if (!symbolsEqual(unique, trackedSymbols)) {
        trackedSymbols = unique;
        cleanupWindows(trackedSymbols);
        reconnect(true);
      }
    } catch (error) {
      log.error(`Failed to refresh ${type} symbols`, { error: error.message });
      throw error; // Re-throw so caller knows it failed
    }
  }

  function scheduleSymbolRefresh() {
    setTimeout(async () => {
      await refreshSymbols();
      scheduleSymbolRefresh();
    }, symbolRefreshMs);
  }

  async function refreshVolumeSettings() {
    console.log(`🔄 ${type.toUpperCase()} Refreshing settings from API...`);
    try {
      const settings = await fetchVolumeSettings(baseUrl, workerApiToken, log);
      const newThreshold = type === 'futures' 
        ? settings.futuresVolumeThreshold 
        : settings.spotVolumeThreshold;
      
      const debugInfo = {
        worker: type.toUpperCase(),
        currentThreshold: `$${volumeThresholdUsd.toLocaleString()}`,
        newThreshold: `$${newThreshold.toLocaleString()}`,
        lastUpdatedAt: lastSettingsUpdatedAt || 'never',
        apiUpdatedAt: settings.updatedAt || 'unknown',
      };
      console.log(`📊 ${type.toUpperCase()} Settings comparison: ${JSON.stringify(debugInfo)}`);
      
      // Only update if settings actually changed (check updatedAt)
      const settingsChanged = !lastSettingsUpdatedAt || 
        (settings.updatedAt && settings.updatedAt !== lastSettingsUpdatedAt);
      
      console.log(`📊 ${type.toUpperCase()} Settings changed: ${settingsChanged}`);
      
      if (settingsChanged) {
        if (newThreshold !== volumeThresholdUsd) {
          const updateInfo = {
            worker: type.toUpperCase(),
            action: 'Threshold updated',
            old: `$${volumeThresholdUsd.toLocaleString()}`,
            new: `$${newThreshold.toLocaleString()}`,
            updatedAt: settings.updatedAt,
          };
          console.log(`⚙️  ${type.toUpperCase()} THRESHOLD UPDATED | ${JSON.stringify(updateInfo)}`);
          volumeThresholdUsd = newThreshold;
        } else {
          console.log(`ℹ️  ${type.toUpperCase()} Threshold unchanged: $${volumeThresholdUsd.toLocaleString()}`);
        }
        lastSettingsUpdatedAt = settings.updatedAt;
      } else {
        console.log(`⏭️  ${type.toUpperCase()} Settings not changed (same updatedAt), skipping update`);
      }
    } catch (error) {
      const errorInfo = {
        worker: type.toUpperCase(),
        action: 'Refresh settings',
        error: error.message,
        status: 'Failed',
      };
      console.log(`❌ ${type.toUpperCase()} SETTINGS REFRESH FAILED | ${JSON.stringify(errorInfo)}`);
      log.error(`Failed to refresh ${type} volume settings`, { error });
    }
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
      reconnectAttempts = 0;
      ws.close();
    } else if (dueToSymbolsChange) {
      connect();
    }
  }

  function connect() {
    if (!trackedSymbols.length) {
      return;
    }

    const url = buildBinanceWsUrl(trackedSymbols, process.env.BINANCE_WS_URL, type);
    if (!url) {
      log.warn(`Unable to determine Binance ${type} WebSocket URL. Skipping connection.`);
      const errorInfo = {
        worker: type.toUpperCase(),
        error: 'Cannot build WebSocket URL',
        status: 'Skipped',
      };
      console.log(`❌ ${type.toUpperCase()} WEBSOCKET URL ERROR | ${JSON.stringify(errorInfo)}`);
      return;
    }

    ws = new WebSocket(url);

    ws.on('open', () => {
      const connectionInfo = {
        worker: type.toUpperCase(),
        status: 'Connected',
        symbolCount: trackedSymbols.length,
        threshold: `$${volumeThresholdUsd.toLocaleString()}`,
      };
      console.log(`✅ ${type.toUpperCase()} WEBSOCKET CONNECTED | ${JSON.stringify(connectionInfo)}`);
      reconnectAttempts = 0;
    });

    ws.on('message', async (message) => {
      const parsed = parseTradeMessage(message, log);
      if (!parsed) return;
      await handleTrade(parsed.symbol, parsed.tradeTime, parsed.quoteUsd);
    });

    ws.on('close', (code, reason) => {
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

    setTimeout(connect, delay);
  }

  // Fetch symbols
  try {
    await refreshSymbols();
    scheduleSymbolRefresh();
  } catch (error) {
    // Don't throw - continue with empty symbols, will retry later
    log.error(`[${type.toUpperCase()}] Failed to fetch symbols`, { error: error.message });
    const errorInfo = {
      worker: type.toUpperCase(),
      action: 'Fetch symbols',
      error: error.message,
      status: 'Will retry',
    };
    console.log('\n⚠️  ========================================');
    console.log(`   ${type.toUpperCase()} SYMBOL FETCH FAILED`);
    console.log('========================================');
    console.log(JSON.stringify(errorInfo, null, 2));
    console.log('========================================\n');
  }

  // Log tracked symbols (only on initial load)
  if (trackedSymbols.length > 0) {
    const symbolsInfo = {
      worker: type.toUpperCase(),
      total: trackedSymbols.length,
      symbols: trackedSymbols.slice(0, 20),
      ...(trackedSymbols.length > 20 && { note: `... and ${trackedSymbols.length - 20} more` }),
    };
    console.log(`📊 ${type.toUpperCase()} SYMBOLS LOADED | ${JSON.stringify(symbolsInfo)}`);
  } else {
    const noSymbolsInfo = {
      worker: type.toUpperCase(),
      status: 'No symbols available',
      action: 'Will retry',
    };
    console.log(`⚠️  ${type.toUpperCase()} NO SYMBOLS | ${JSON.stringify(noSymbolsInfo)}`);
  }

  // Start WebSocket connection
  if (!ws && trackedSymbols.length > 0) {
    connect();
    const wsInfo = {
      worker: type.toUpperCase(),
      status: 'Connecting',
      symbolCount: trackedSymbols.length,
    };
    console.log(`🔌 ${type.toUpperCase()} WEBSOCKET INITIATED | ${JSON.stringify(wsInfo)}`);
  } else if (!trackedSymbols.length) {
    const wsInfo = {
      worker: type.toUpperCase(),
      status: 'Not started',
      reason: 'No symbols available',
    };
    console.log(`⚠️  ${type.toUpperCase()} WEBSOCKET NOT STARTED | ${JSON.stringify(wsInfo)}`);
  }

  // Return control object with stop and refreshSettings methods
  return {
    stop: () => {
      closedByUser = true;
      if (ws) ws.close();
    },
    refreshSettings: refreshVolumeSettings,
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

  // Store worker instances for refresh endpoint
  let spotWorker = null;
  let futuresWorker = null;

  const server = http.createServer((req, res) => {
    // Parse URL path (handle both absolute and relative paths)
    const urlPath = req.url?.split('?')[0] || '/';
    
    // Log all incoming requests
    console.log(`📥 Incoming request: ${req.method} ${urlPath}`);
    
    // Health check endpoint
    if (urlPath === '/' || urlPath === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
      return;
    }

    // Refresh settings endpoint (requires WORKER_API_TOKEN)
    if (urlPath === '/refresh-settings' && req.method === 'POST') {
      console.log(`🔔 POST /refresh-settings received`);
      const authHeader = req.headers.authorization || req.headers.Authorization;
      const token = authHeader?.replace('Bearer ', '').trim();
      
      if (token !== workerApiToken) {
        log.warn('Unauthorized settings refresh attempt');
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }

      const refreshInfo = {
        action: 'Settings refresh',
        triggeredBy: 'API',
        timestamp: new Date().toISOString(),
      };
      console.log(`🔔 SETTINGS REFRESH TRIGGERED | ${JSON.stringify(refreshInfo)}`);
      log.info('Settings refresh triggered via API');
      
      // Check if workers are available
      const spotAvailable = spotWorker && typeof spotWorker.refreshSettings === 'function';
      const futuresAvailable = futuresWorker && typeof futuresWorker.refreshSettings === 'function';
      
      console.log(`📋 Worker availability check: SPOT=${spotAvailable ? 'available' : 'NOT available'}, FUTURES=${futuresAvailable ? 'available' : 'NOT available'}`);
      
      if (!spotAvailable && !futuresAvailable) {
        const errorInfo = {
          action: 'Settings refresh',
          status: 'Failed',
          error: 'No workers available',
          spotAvailable,
          futuresAvailable,
        };
        console.log(`❌ SETTINGS REFRESH FAILED | ${JSON.stringify(errorInfo)}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No workers available' }));
        return;
      }
      
      // Trigger settings refresh for both workers
      Promise.all([
        spotAvailable ? spotWorker.refreshSettings() : Promise.resolve(),
        futuresAvailable ? futuresWorker.refreshSettings() : Promise.resolve(),
      ])
        .then(() => {
          const successInfo = {
            action: 'Settings refresh',
            status: 'Completed',
            workers: [
              ...(spotAvailable ? ['SPOT'] : []),
              ...(futuresAvailable ? ['FUTURES'] : []),
            ],
          };
          console.log(`✅ SETTINGS REFRESH COMPLETED | ${JSON.stringify(successInfo)}`);
          log.info('Settings refresh completed successfully');
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, message: 'Settings refresh triggered' }));
        })
        .catch((error) => {
          const errorInfo = {
            action: 'Settings refresh',
            status: 'Failed',
            error: error.message,
            stack: error.stack,
          };
          console.log(`❌ SETTINGS REFRESH FAILED | ${JSON.stringify(errorInfo)}`);
          log.error('Failed to refresh settings', { error });
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Failed to refresh settings' }));
        });
      return;
    }

    // 404 for unknown endpoints
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  });

  server
    .listen(port, () => {
      log.info('Health server listening', { port });
    })
    .on('error', (error) => {
      log.error('Failed to start health server', { error });
    });

  // Wait a bit for server to be ready
  await new Promise(resolve => setTimeout(resolve, 100));

  // Fetch settings from API first (before starting workers)
  let spotThresholdFromApi = volumeThresholdUsd;
  let futuresThresholdFromApi = futuresVolumeThresholdUsd;
  
  try {
    const settings = await fetchVolumeSettings(baseUrl, workerApiToken, log);
    spotThresholdFromApi = settings.spotVolumeThreshold;
    futuresThresholdFromApi = settings.futuresVolumeThreshold;
  } catch (error) {
    log.warn('Failed to fetch settings from API, using env defaults', { error });
  }

  // Log initial settings from API
  const startupInfo = {
    status: 'Starting',
    settings: {
      spot: {
        threshold: `$${spotThresholdFromApi.toLocaleString()}`,
        source: 'API',
      },
      futures: {
        threshold: `$${futuresThresholdFromApi.toLocaleString()}`,
        source: 'API',
      },
    },
  };
  
  console.log(`🚀 ORUBA VOLUME WORKER STARTING | ${JSON.stringify(startupInfo)}`);

  console.log('🔄 Starting SPOT worker...');
  try {
    spotWorker = await startWorker({
    baseUrl,
    workerApiToken,
    pushTriggerToken,
    log,
    symbolRefreshMs,
    volumeWindowMs,
    initialVolumeThresholdUsd: spotThresholdFromApi,
    notificationCooldownMs,
    type: 'spot',
    });
    console.log('✅ SPOT worker started successfully');
  } catch (error) {
    const errorInfo = {
      worker: 'SPOT',
      error: error.message,
      status: 'Failed',
    };
    console.log(`❌ SPOT WORKER FAILED | ${JSON.stringify(errorInfo)}`);
    log.error('Failed to initialize spot worker', { error });
    throw error;
  }

  console.log('🔄 Starting FUTURES worker...');
  try {
    futuresWorker = await startWorker({
    baseUrl,
    workerApiToken,
    pushTriggerToken,
    log,
    symbolRefreshMs,
    volumeWindowMs,
    initialVolumeThresholdUsd: futuresThresholdFromApi,
    notificationCooldownMs,
    type: 'futures',
    });
    console.log('✅ FUTURES worker started successfully');
  } catch (error) {
    const errorInfo = {
      worker: 'FUTURES',
      error: error.message,
      status: 'Failed',
    };
    console.log(`❌ FUTURES WORKER FAILED | ${JSON.stringify(errorInfo)}`);
    log.error('Failed to initialize futures worker', { error });
    throw error;
  }

  function handleExit(signal) {
    log.info(`Received ${signal}. Shutting down gracefully.`);
    spotWorker?.stop();
    futuresWorker?.stop();
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
