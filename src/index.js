const WebSocket = require('ws');
const fetch = require('cross-fetch');

function assertEnv(name, optional = false) {
  const value = process.env[name];

  if (!optional && (value === undefined || value === '')) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function createLogger(level = 'info') {
  const levels = ['error', 'warn', 'info', 'debug'];
  const currentIndex = levels.indexOf(level);

  return levels.reduce((logger, name, index) => {
    logger[name] = (...args) => {
      if (index <= currentIndex) {
        console[name === 'debug' ? 'log' : name](`[${name.toUpperCase()}]`, ...args);
      }
    };
    return logger;
  }, {});
}

function main() {
  const vercelBaseUrl = assertEnv('VERCEL_BASE_URL');
  const workerApiToken = assertEnv('WORKER_API_TOKEN');
  const alertTriggerToken = assertEnv('ALERT_TRIGGER_TOKEN');
  const logLevel = assertEnv('LOG_LEVEL', true) || 'info';

  const log = createLogger(logLevel);

  log.info('Starting Oruba alerts worker');
  log.debug('Configuration', {
    vercelBaseUrl,
    workerApiToken: workerApiToken ? '***' : 'missing',
    alertTriggerToken: alertTriggerToken ? '***' : 'missing',
    logLevel,
  });

  // TODO: Connect to Binance WebSocket stream and listen for price updates.
  // const ws = new WebSocket('wss://stream.binance.com:9443/ws/...'); // update stream URL

  // TODO: Fetch active alerts from the Oruba backend using WORKER_API_TOKEN.
  // fetch(`${vercelBaseUrl}/api/alerts/...`, {
  //   headers: { Authorization: `Bearer ${workerApiToken}` },
  // });

  // TODO: Trigger alerts via POST /api/alerts/trigger-single when conditions are met.
  // fetch(`${vercelBaseUrl}/api/alerts/trigger-single`, {
  //   method: 'POST',
  //   headers: {
  //     Authorization: `Bearer ${alertTriggerToken}`,
  //     'Content-Type': 'application/json',
  //   },
  //   body: JSON.stringify({ ...payload }),
  // });

  log.info('Worker initialization complete (logic not yet implemented).');
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error('[ERROR]', error.message);
    process.exitCode = 1;
  }
}

