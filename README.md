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

## PM2

```bash
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

Useful commands:

```bash
pm2 status
pm2 logs acws
pm2 restart acws
pm2 stop acws
```

## Notes

- Order keys are expected to use `tokenId|address`
- `order-server-db` is a local LevelDB directory and should not be committed
- Built-in TLS is still supported through environment variables when needed
