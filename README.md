# ACWS

Order and balance service for Agora/eCash, exposed through a Hono API, an Express proxy layer, and WebSocket presence tracking.

## Run

```bash
npm install
cp .env.example .env
npm start
```

Default recommendation for VPS deployment:

- Run Node on `127.0.0.1:3000`
- Put Nginx or Caddy in front
- Terminate TLS at the reverse proxy for `acws.alitayin.com`

## Scripts

```bash
npm start
npm test
```

## Notes

- Order keys are expected to use `tokenId|address`
- `order-server-db` is a local LevelDB directory and should not be committed
- Built-in TLS is still supported through environment variables when needed
