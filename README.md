🚀 **Oruba Alerts Worker**

Real-time background service that listens to Binance **spot trade streams**, keeps a **rolling 15-minute volume (USD)** per symbol, and triggers **push notifications** for all Oruba Coin users when the volume crosses a threshold. 🔔

### 🔧 Technologies
- Node.js
- ws (WebSocket)
- cross-fetch 
- dotenv

### 🌐 Platforms
- Binance WebSocket API
- Vercel (Oruba backend)
- Fly.io (deployment)

