require('dotenv').config();
const http = require('http');
const WebSocket = require('ws');
const fetch = require('cross-fetch');

const DEFAULT_ALERT_REFRESH_INTERVAL_MS = 60_000;
const DEFAULT_WS_RETRY_MS = 5_000;
const MAX_WS_RETRY_MS = 60_000;

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
  if (!url) {
    return url;
  }

  return url.endsWith('/') ? url.slice(0, -1) : url;
}

function parseSymbols(raw) {
  if (!raw) {
    return [];
  }

  return String(raw)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => item.toLowerCase());
}

function resolveAlertId(alert) {
  return alert.id || alert._id || alert.uuid;
}

function resolveAlertThreshold(alert) {
  const candidates = [
    alert.targetPrice,
    alert.price,
    alert.threshold,
    alert.triggerPrice,
    alert.value,
  ];

  for (let i = 0; i < candidates.length; i += 1) {
    const candidate = candidates[i];
    const number = typeof candidate === 'string' ? Number(candidate) : candidate;

    if (Number.isFinite(number)) {
      return number;
    }
  }

  return undefined;
}

function resolveAlertOperator(alert) {
  const raw =
    alert.operator ||
    alert.direction ||
    alert.comparison ||
    alert.condition ||
    alert.type;

  return raw ? String(raw).toLowerCase() : 'above';
}

function resolveCooldown(alert) {
  const raw = alert.cooldownMs || alert.cooldownSeconds || alert.cooldown;

  if (raw === undefined || raw === null) {
    return 0;
  }

  const value = Number(raw);

  if (!Number.isFinite(value)) {
    return 0;
  }

  return raw === alert.cooldownSeconds ? value * 1_000 : value;
}

function resolveSymbol(alert) {
  if (!alert.symbol && alert.asset) {
    return String(alert.asset).toLowerCase();
  }

  return alert.symbol ? String(alert.symbol).toLowerCase() : undefined;
}

function shouldTriggerAlert(alert, currentPrice, previousPrice, log) {
  const threshold = resolveAlertThreshold(alert);

  if (!Number.isFinite(threshold)) {
    log.warn('Skipping alert with invalid threshold', { alert });
    return false;
  }

  const operator = resolveAlertOperator(alert);

  switch (operator) {
    case 'above':
    case 'greater_than':
    case 'gt':
    case 'gte':
    case 'greater_or_equal':
    case '>':
    case '>=':
      return currentPrice >= threshold;
    case 'below':
    case 'less_than':
    case 'lt':
    case 'lte':
    case 'less_or_equal':
    case '<':
    case '<=':
      return currentPrice <= threshold;
    case 'equal':
    case 'equals':
    case 'eq':
    case '=':
    case '==':
      return Math.abs(currentPrice - threshold) <= 1e-8;
    case 'crosses_above':
    case 'cross_above':
      return (
        Number.isFinite(previousPrice) &&
        previousPrice < threshold &&
        currentPrice >= threshold
      );
    case 'crosses_below':
    case 'cross_below':
      return (
        Number.isFinite(previousPrice) &&
        previousPrice > threshold &&
        currentPrice <= threshold
      );
    default:
      log.warn('Unknown alert operator, defaulting to "above"', {
        operator,
        alert,
      });
      return currentPrice >= threshold;
  }
}

async function fetchActiveAlerts(baseUrl, token, log) {
  const url = `${normalizeBaseUrl(baseUrl)}/api/alerts/worker`;

  log.debug('Fetching active alerts', { url });

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to fetch active alerts (${response.status} ${response.statusText}): ${text}`
    );
  }

  const payload = await response.json();
  const alerts = Array.isArray(payload) ? payload : payload.alerts || [];

  if (!Array.isArray(alerts)) {
    throw new Error('Alerts endpoint returned unexpected payload');
  }

  log.info('Fetched alerts', { count: alerts.length });

  return alerts;
}

async function triggerAlert(baseUrl, token, alert, symbol, price, log) {
  const alertId = resolveAlertId(alert);

  if (!alertId) {
    log.warn('Cannot trigger alert without an id', { alert });
    return false;
  }

  const url = `${normalizeBaseUrl(
    baseUrl
  )}/api/alerts/trigger-single`;

  const body = {
    alertId,
    symbol,
    price,
    triggeredAt: new Date().toISOString(),
    alert,
  };

  log.info('Triggering alert', { alertId, symbol, price });

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to trigger alert ${alertId} (${response.status} ${response.statusText}): ${text}`
    );
  }

  log.debug('Alert triggered successfully', { alertId });

  return true;
}

function groupAlertsBySymbol(alerts, log) {
  const grouped = new Map();

  alerts.forEach((alert) => {
    const symbol = resolveSymbol(alert);

    if (!symbol) {
      log.warn('Skipping alert without symbol', { alert });
      return;
    }

    if (!grouped.has(symbol)) {
      grouped.set(symbol, []);
    }

    grouped.get(symbol).push(alert);
  });

  return grouped;
}

function buildBinanceWsUrl(symbols, customUrl) {
  const base = customUrl || 'wss://stream.binance.com:9443';

  if (symbols.length === 0) {
    return undefined;
  }

  if (symbols.length === 1) {
    return `${base}/ws/${symbols[0]}@ticker`;
  }

  const streams = symbols.map((symbol) => `${symbol}@ticker`).join('/');

  return `${base}/stream?streams=${streams}`;
}

function parseTickerMessage(messageBuffer, log) {
  try {
    const payload = JSON.parse(messageBuffer.toString());
    const data = payload.data || payload;

    const symbol = data.s
      ? String(data.s).toLowerCase()
      : payload.stream
      ? String(payload.stream.split('@')[0]).toLowerCase()
      : undefined;

    const priceCandidate =
      data.c || data.p || data.lastPrice || data.price || data.w;

    const price =
      typeof priceCandidate === 'string'
        ? Number(priceCandidate)
        : Number(priceCandidate);

    if (!symbol || !Number.isFinite(price)) {
      log.debug('Received unsupported payload', { payload });
      return undefined;
    }

    return { symbol, price };
  } catch (error) {
    log.warn('Failed to parse Binance message', { error });
    return undefined;
  }
}

async function startWorker(config) {
  const {
    alertsBaseUrl,
    alertsFetchToken,
    alertTriggerToken,
    symbols,
    refreshIntervalMs,
    log,
  } = config;

  let ws;
  let reconnectAttempts = 0;
  let closedByUser = false;
  let groupedAlerts = new Map();
  const triggeredAlerts = new Map();
  const lastPrices = new Map();

  let trackedSymbols = Array.isArray(symbols) && symbols.length > 0 ? symbols : [];
  let explicitSymbols = Array.isArray(symbols) && symbols.length > 0;

  function symbolsEqual(a, b) {
    if (a.length !== b.length) {
      return false;
    }

    const setA = new Set(a);

    for (let i = 0; i < b.length; i += 1) {
      if (!setA.has(b[i])) {
        return false;
      }
    }

    return true;
  }

  async function loadAlerts() {
    try {
      const alerts = await fetchActiveAlerts(
        alertsBaseUrl,
        alertsFetchToken,
        log
      );
      groupedAlerts = groupAlertsBySymbol(alerts, log);
      triggeredAlerts.clear();

       if (!explicitSymbols) {
        const derivedSymbols = Array.from(groupedAlerts.keys());

        if (!symbolsEqual(derivedSymbols, trackedSymbols)) {
          trackedSymbols = derivedSymbols;
          log.info('Updated tracked symbols from alerts', { trackedSymbols });
          reconnect(true);
        }
      }
    } catch (error) {
      log.error('Failed to refresh alerts', { error });
    }
  }

  function scheduleAlertRefresh() {
    setTimeout(async () => {
      await loadAlerts();
      scheduleAlertRefresh();
    }, refreshIntervalMs);
  }

  async function handlePriceUpdate(symbol, price) {
    const symbolAlerts = groupedAlerts.get(symbol);

    if (!symbolAlerts || symbolAlerts.length === 0) {
      return;
    }

    const previousPrice = lastPrices.get(symbol);
    lastPrices.set(symbol, price);

    for (let i = 0; i < symbolAlerts.length; i += 1) {
      const alert = symbolAlerts[i];
      const alertId = resolveAlertId(alert);

      if (!alertId) {
        continue;
      }

      const cooldown = resolveCooldown(alert);
      const lastTriggeredAt = triggeredAlerts.get(alertId);

      if (
        Number.isFinite(cooldown) &&
        cooldown > 0 &&
        lastTriggeredAt &&
        Date.now() - lastTriggeredAt < cooldown
      ) {
        continue;
      }

      if (!shouldTriggerAlert(alert, price, previousPrice, log)) {
        continue;
      }

      try {
        const success = await triggerAlert(
          alertsBaseUrl,
          alertTriggerToken,
          alert,
          symbol,
          price,
          log
        );

        if (success) {
          triggeredAlerts.set(alertId, Date.now());
        }
      } catch (error) {
        log.error('Failed to trigger alert', {
          alertId,
          symbol,
          error,
        });
      }
    }
  }

  function reconnect(dueToSymbolsChange = false) {
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
    if (trackedSymbols.length === 0) {
      log.warn('No symbols to track. Waiting for alerts.');
      return;
    }

    const url = buildBinanceWsUrl(trackedSymbols, process.env.BINANCE_WS_URL);

    if (!url) {
      log.warn('Unable to determine Binance WebSocket URL. Skipping connection.');
      return;
    }

    log.info('Connecting to Binance WebSocket', { url });

    ws = new WebSocket(url);

    ws.on('open', () => {
      log.info('Connected to Binance WebSocket');
      reconnectAttempts = 0;
    });

    ws.on('message', async (message) => {
      const parsed = parseTickerMessage(message, log);

      if (!parsed) {
        return;
      }

      await handlePriceUpdate(parsed.symbol, parsed.price);
    });

    ws.on('close', (code, reason) => {
      log.warn('Binance WebSocket closed', {
        code,
        reason: reason.toString(),
      });

      if (closedByUser) {
        return;
      }

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

  await loadAlerts();
  scheduleAlertRefresh();
  if (!ws && trackedSymbols.length > 0) {
    connect();
  }

  return () => {
    closedByUser = true;

    if (ws) {
      ws.close();
    }
  };
}

async function main() {
  const vercelBaseUrl = assertEnv('VERCEL_BASE_URL');
  const workerApiToken = assertEnv('WORKER_API_TOKEN');
  const alertTriggerToken = assertEnv('ALERT_TRIGGER_TOKEN');
  const logLevel = assertEnv('LOG_LEVEL', true) || 'info';
  const refreshIntervalRaw = assertEnv('ALERT_REFRESH_INTERVAL_MS', true);
  const symbolsRaw = assertEnv('BINANCE_SYMBOLS', true);

  const refreshIntervalMs = Number(refreshIntervalRaw) || DEFAULT_ALERT_REFRESH_INTERVAL_MS;
  const symbols = parseSymbols(symbolsRaw);
  const log = createLogger(logLevel);

  const port = Number(process.env.PORT) || 8080;

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

  log.info('Starting Oruba alerts worker');
  log.debug('Configuration', {
    vercelBaseUrl: normalizeBaseUrl(vercelBaseUrl),
    symbols,
    refreshIntervalMs,
    logLevel,
  });

  const stop = await startWorker({
    alertsBaseUrl: vercelBaseUrl,
    alertsFetchToken: workerApiToken,
    alertTriggerToken,
    symbols,
    refreshIntervalMs,
    log,
  });

  function handleExit(signal) {
    log.info(`Received ${signal}. Shutting down gracefully.`);
    stop();
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

