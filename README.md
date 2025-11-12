# Oruba Alerts Worker

Worker service that listens to Binance market data and triggers alert notifications through the Oruba backend.

## Prerequisites

- Node.js 20+
- npm
- Fly.io CLI (`flyctl`)

## Getting Started

1. Install dependencies:

   ```bash
   npm install
   ```

2. Copy `env.example` to `.env`, fill in secrets, and start the worker locally:

   ```bash
   cp env.example .env
   # edit .env with your credentials
   npm start
   ```

## Environment Variables

- `VERCEL_BASE_URL` (required): Backend base URL. Default production endpoint: `https://orubacoin.com`.
- `WORKER_API_TOKEN` (required): Token used to fetch alerts from the Oruba backend.
- `ALERT_TRIGGER_TOKEN` (required): Token used to trigger alerts via `POST /api/alerts/trigger-single`.
- `BINANCE_SYMBOLS` (optional): Comma-separated list of symbols to watch. Leave empty to auto-derive from backend alerts.
- `BINANCE_WS_URL` (optional): Override default Binance WebSocket base URL.
- `ALERT_REFRESH_INTERVAL_MS` (optional): How often to refresh alerts from backend (default: `60000`).
- `LOG_LEVEL` (optional): Logging level (`error`, `warn`, `info`, `debug`). Defaults to `info`.

## Runtime Overview

`src/index.js` handles:

- Connecting to Binance WebSocket streams for configured symbols.
- Periodically fetching active alerts from `GET /api/alerts/worker`.
- Evaluating price thresholds (above/below/cross logic) and triggering matches through `POST /api/alerts/trigger-single`.

## Fly.io Deployment

1. Initialize Fly.io app (only run once):

   ```bash
   flyctl launch --name oruba-coin-worker --runtime node
   ```

2. Configure secrets:

   ```bash
   flyctl secrets set \
     VERCEL_BASE_URL=https://orubacoin.com \
     WORKER_API_TOKEN=example-token \
     ALERT_TRIGGER_TOKEN=trigger-token \
     ALERT_REFRESH_INTERVAL_MS=15000 \
     LOG_LEVEL=info
   ```

3. Deploy:

   ```bash
   flyctl deploy
   ```

The worker also exposes a lightweight HTTP health endpoint on `PORT` (default 8080) that returns JSON `{ status: "ok" }`, allowing Fly's built-in checks to succeed.

## License

MIT

