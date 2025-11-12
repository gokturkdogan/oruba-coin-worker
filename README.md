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

2. Provide the required environment variables (see below) and start the worker locally:

   ```bash
   VERCEL_BASE_URL=https://orubacoin.com \
   WORKER_API_TOKEN=example-token \
   ALERT_TRIGGER_TOKEN=trigger-token \
   npm start
   ```

## Environment Variables

- `VERCEL_BASE_URL` (required): Backend base URL. Default production endpoint: `https://orubacoin.com`.
- `WORKER_API_TOKEN` (required): Token used to fetch alerts from the Oruba backend.
- `ALERT_TRIGGER_TOKEN` (required): Token used to trigger alerts via `POST /api/alerts/trigger-single`.
- `LOG_LEVEL` (optional): Logging level (`error`, `warn`, `info`, `debug`). Defaults to `info`.

## Development Roadmap

The core logic will be implemented in `src/index.js`:

- Connect to Binance WebSocket streams for relevant trading pairs.
- Fetch alerts from `GET /api/alerts/...`.
- Trigger alerts through `POST /api/alerts/trigger-single`.

## Fly.io Deployment

1. Initialize Fly.io app (only run once):

   ```bash
   flyctl launch --name oruba-alerts-worker --runtime node
   ```

2. Configure secrets:

   ```bash
   flyctl secrets set \
     VERCEL_BASE_URL=https://orubacoin.com \
     WORKER_API_TOKEN=example-token \
     ALERT_TRIGGER_TOKEN=trigger-token \
     LOG_LEVEL=info
   ```

3. Deploy:

   ```bash
   flyctl deploy
   ```

## License

MIT

