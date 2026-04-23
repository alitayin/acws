import express from 'express';
import cors from 'cors';
import https from 'https';
import http from 'http';
import fs from 'fs';
import httpProxy from 'http-proxy';
import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import crypto from 'crypto';
import { ChronikClient } from 'chronik-client';
import { Level } from 'level';
import { WebSocketServer } from 'ws';
import {
  buildCorsOptions,
  buildOrderPayload,
  delay,
  isOrderKeyForAddress,
  normalizeOptionalString,
  parseBoolean,
  parseNumber,
  parseOrderKey,
  sortObjectDeep,
  validateOrdersData,
} from './src/utils.js';

// ---------------------- TLS / Proxy 配置 ----------------------
const runtimeConfig = {
  host: process.env.HOST || '0.0.0.0',
  internalHost: process.env.INTERNAL_HOST || '127.0.0.1',
  proxyPort: parseNumber(process.env.PROXY_PORT, parseBoolean(process.env.TLS_ENABLED, false) ? 443 : 3000),
  httpRedirectPort: parseNumber(process.env.HTTP_REDIRECT_PORT, 80),
  honoPort: parseNumber(process.env.HONO_PORT, 3043),
  wsPort: parseNumber(process.env.WS_PORT, 3044),
  dbPath: process.env.DB_PATH || './order-server-db',
  tlsEnabled: parseBoolean(process.env.TLS_ENABLED, false),
  httpRedirectEnabled: parseBoolean(process.env.HTTP_REDIRECT_ENABLED, false),
  tlsKeyPath: normalizeOptionalString(process.env.TLS_KEY_PATH),
  tlsCertPath: normalizeOptionalString(process.env.TLS_CERT_PATH),
  tlsCaPath: normalizeOptionalString(process.env.TLS_CA_PATH),
  redirectHost: normalizeOptionalString(process.env.REDIRECT_HOST),
  corsOrigin:
    normalizeOptionalString(process.env.CORS_ORIGIN) || 'https://agora.cash,https://www.agora.cash',
  balanceCacheMs: parseNumber(process.env.BALANCE_CACHE_MS, 5 * 60 * 1000),
  balanceRefreshMs: parseNumber(process.env.BALANCE_REFRESH_MS, 60 * 1000),
  requestBodyLimit: normalizeOptionalString(process.env.JSON_BODY_LIMIT) || '1mb',
};

const FORWARDED_REQUEST_HEADERS = [
  'accept',
  'content-type',
  'authorization',
  'origin',
  'referer',
  'user-agent',
  'x-request-id',
  'x-forwarded-for',
  'x-forwarded-host',
  'x-forwarded-proto',
  'x-real-ip',
];

const runtimeState = {
  dbReady: false,
  shuttingDown: false,
  honoServer: null,
  proxyServer: null,
  redirectServer: null,
  wsService: null,
  balanceRefreshInterval: null,
};

const pendingBalanceRequests = new Map();

function getRequestIp(headers) {
  const forwardedFor = headers['x-forwarded-for'] || headers.get?.('x-forwarded-for');
  if (typeof forwardedFor === 'string' && forwardedFor.length > 0) {
    return forwardedFor.split(',')[0].trim();
  }

  return (
    headers['x-real-ip'] ||
    headers.get?.('x-real-ip') ||
    headers['cf-connecting-ip'] ||
    headers.get?.('cf-connecting-ip') ||
    'unknown'
  );
}

function logApiEvent(message, details = {}) {
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      service: 'acws',
      message,
      ...details,
    }),
  );
}

function loadTlsCredentials() {
  if (!runtimeConfig.tlsEnabled) {
    return null;
  }

  if (!runtimeConfig.tlsKeyPath || !runtimeConfig.tlsCertPath) {
    throw new Error('TLS_ENABLED=true 时必须提供 TLS_KEY_PATH 和 TLS_CERT_PATH');
  }

  const credentials = {
    key: fs.readFileSync(runtimeConfig.tlsKeyPath, 'utf8'),
    cert: fs.readFileSync(runtimeConfig.tlsCertPath, 'utf8'),
  };

  if (runtimeConfig.tlsCaPath) {
    credentials.ca = fs.readFileSync(runtimeConfig.tlsCaPath, 'utf8');
  }

  return credentials;
}

const proxyApp = express();
proxyApp.set('trust proxy', true);
proxyApp.use(express.json({ limit: runtimeConfig.requestBodyLimit }));
proxyApp.use(express.urlencoded({ extended: true }));

proxyApp.use((req, _res, next) => {
  if (req.path.startsWith('/orders/push/') || req.path.startsWith('/orders/check-hash/')) {
    logApiEvent('proxy.request.received', {
      route: req.path,
      method: req.method,
      origin: req.headers.origin || 'unknown',
      ip: getRequestIp(req.headers),
      accessControlRequestMethod: req.headers['access-control-request-method'] || null,
    });
  }

  next();
});

proxyApp.use(cors(buildCorsOptions(runtimeConfig.corsOrigin)));

const HONO_API = `http://${runtimeConfig.internalHost}:${runtimeConfig.honoPort}`;

// ---------------------- Hono 业务服务 ----------------------
const parseEndpointList = (value, fallback) => {
  const normalized = normalizeOptionalString(value);
  if (!normalized) return fallback;
  return normalized
    .split(',')
    .map((endpoint) => endpoint.trim())
    .filter(Boolean);
};

const config = {
  chronikEndpoints: parseEndpointList(process.env.CHRONIK_ENDPOINTS, [
    'https://chronik.e.cash',
    'https://chronik-native2.fabien.cash',
    'https://chronik-native3.fabien.cash',
    'https://chronik-native1.fabien.cash',
  ]),
  chronikEndpointsAgora: parseEndpointList(process.env.CHRONIK_ENDPOINTS_AGORA, [
    'https://chronik-native2.fabien.cash',
    'https://chronik-native3.fabien.cash',
    'https://chronik-native1.fabien.cash',
  ]),
};

const chronik = new ChronikClient(config.chronikEndpointsAgora);
const app = new Hono();
const onlineClients = new Map();

function createWebSocketServer(wsPort, addressBalances, getAddressBalance) {
  const wsServer = http.createServer();
  const wss = new WebSocketServer({ server: wsServer });

  wss.on('connection', (ws, req) => {
    const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
    onlineClients.set(ws, {
      id: clientId,
      addresses: [],
      lastSeen: Date.now(),
    });

    ws.on('message', async (data) => {
      try {
        const message = JSON.parse(data.toString());
        if (message.type === 'client_online' && Array.isArray(message.addresses)) {
          const clientInfo = onlineClients.get(ws);
          if (clientInfo) {
            clientInfo.addresses = message.addresses;
            clientInfo.lastSeen = Date.now();
            for (const [index, address] of message.addresses.entries()) {
              if (index > 0) await delay(500);
              const balanceInfo = addressBalances.get(address);
              if (!balanceInfo || Date.now() - balanceInfo.lastUpdated > runtimeConfig.balanceCacheMs) {
                await getAddressBalance(address);
              }
            }
          }
        }

        if (message.type === 'update_addresses' && Array.isArray(message.addresses)) {
          const clientInfo = onlineClients.get(ws);
          if (clientInfo) {
            clientInfo.addresses = message.addresses;
            clientInfo.lastSeen = Date.now();
            for (const [index, address] of message.addresses.entries()) {
              if (index > 0) await delay(500);
              const balanceInfo = addressBalances.get(address);
              if (!balanceInfo || Date.now() - balanceInfo.lastUpdated > runtimeConfig.balanceCacheMs) {
                await getAddressBalance(address);
              }
            }
          }
        }

        if (message.type === 'ping') {
          const clientInfo = onlineClients.get(ws);
          if (clientInfo) {
            clientInfo.lastSeen = Date.now();
            const balanceInfo = {};
            for (const address of clientInfo.addresses) {
              const info = addressBalances.get(address);
              if (info) {
                balanceInfo[address] = {
                  balance: info.balance,
                  utxoCount: info.utxoCount,
                  lastUpdated: info.lastUpdated,
                };
              }
            }
            ws.send(
              JSON.stringify({
                type: 'pong',
                timestamp: Date.now(),
                balances: balanceInfo,
              }),
            );
          }
        }
      } catch (error) {
        console.error('Invalid WebSocket message:', error);
      }
    });

    ws.on('close', () => {
      onlineClients.delete(ws);
    });

    ws.on('error', () => {
      onlineClients.delete(ws);
    });
  });

  wsServer.listen(wsPort, runtimeConfig.internalHost);

  const cleanupInterval = setInterval(() => {
    const now = Date.now();
    const timeout = 2 * 60 * 1000;
    for (const [ws, info] of onlineClients.entries()) {
      if (now - info.lastSeen > timeout) {
        ws.terminate();
        onlineClients.delete(ws);
      }
    }
  }, 60000);

  return {
    server: wsServer,
    wss,
    getOnlineClients: () => onlineClients,
    close: async () => {
      clearInterval(cleanupInterval);
      for (const [ws] of onlineClients.entries()) {
        ws.terminate();
        onlineClients.delete(ws);
      }

      await new Promise((resolve) => {
        wss.close(() => resolve());
      });

      await new Promise((resolve, reject) => {
        wsServer.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
    },
  };
}

function getOnlineAddresses() {
  const onlineAddresses = new Set();
  for (const info of onlineClients.values()) {
    for (const address of info.addresses) {
      onlineAddresses.add(address);
    }
  }
  return Array.from(onlineAddresses);
}

function isAddressOnline(address) {
  for (const info of onlineClients.values()) {
    if (info.addresses.includes(address)) return true;
  }
  return false;
}

const db = new Level(runtimeConfig.dbPath, { valueEncoding: 'json' });
const addressBalances = new Map();

function setupBalanceRoutes(appInstance, balancesMap, getBalance, getAddresses) {
  appInstance.get('/api/balance/:address', async (c) => {
    const address = c.req.param('address');
    const cachedInfo = balancesMap.get(address);
    if (cachedInfo && Date.now() - cachedInfo.lastUpdated < runtimeConfig.balanceCacheMs) {
      return c.json({
        address,
        balance: cachedInfo.balance,
        utxoCount: cachedInfo.utxoCount,
        cached: true,
        lastUpdated: cachedInfo.lastUpdated,
      });
    }

    try {
      const balance = await getBalance(address);
      const info = balancesMap.get(address);
      if (balance == null || !info) {
        return c.json(
          {
            address,
            error: '获取余额失败',
            message: '上游 Chronik 服务不可用',
          },
          502,
        );
      }
      return c.json({
        address,
        balance: info.balance,
        utxoCount: info.utxoCount,
        cached: false,
        lastUpdated: info.lastUpdated,
      });
    } catch (error) {
      return c.json(
        {
          address,
          error: '获取余额失败',
          message: error.message,
        },
        500,
      );
    }
  });

  appInstance.get('/api/balances', (c) => {
    const balances = {};
    const onlineAddresses = getAddresses();
    for (const address of onlineAddresses) {
      const balanceInfo = balancesMap.get(address);
      if (balanceInfo) {
        balances[address] = {
          balance: balanceInfo.balance,
          utxoCount: balanceInfo.utxoCount,
          lastUpdated: balanceInfo.lastUpdated,
        };
      }
    }
    return c.json({
      balances,
      count: Object.keys(balances).length,
      timestamp: Date.now(),
    });
  });
}

async function getAddressBalance(address, maxRetries = 3, retryDelay = 2000) {
  if (pendingBalanceRequests.has(address)) {
    return pendingBalanceRequests.get(address);
  }

  const requestPromise = (async () => {
    let retries = 0;
    let lastError = null;
    let nextDelay = retryDelay;

    while (retries <= maxRetries) {
      try {
        const utxos = await chronik.address(address).utxos();
        let totalBalance = 0;
        for (const utxo of utxos.utxos) {
          totalBalance += utxo.value;
        }
        addressBalances.set(address, {
          balance: totalBalance,
          utxoCount: utxos.utxos.length,
          lastUpdated: Date.now(),
        });
        return totalBalance;
      } catch (error) {
        lastError = error;
        retries++;
        if (retries <= maxRetries) {
          await delay(nextDelay);
          nextDelay *= 1.5;
        } else {
          console.error(`Failed to fetch balance for ${address}:`, lastError);
          return null;
        }
      }
    }

    return null;
  })();

  pendingBalanceRequests.set(address, requestPromise);

  try {
    return await requestPromise;
  } finally {
    pendingBalanceRequests.delete(address);
  }
}

async function updateAllBalances() {
  const onlineAddresses = getOnlineAddresses();
  for (const [index, address] of onlineAddresses.entries()) {
    if (index > 0) await delay(1000);
    await getAddressBalance(address);
  }
}

setupBalanceRoutes(app, addressBalances, getAddressBalance, getOnlineAddresses);

app.get('/health', (c) => {
  return c.json({
    status: 'ok',
    service: 'acws',
    timestamp: Date.now(),
  });
});

app.get('/ready', (c) => {
  return c.json(
    {
      status: runtimeState.dbReady ? 'ready' : 'starting',
      dbReady: runtimeState.dbReady,
      timestamp: Date.now(),
    },
    runtimeState.dbReady ? 200 : 503,
  );
});

app.get('/orders/offline', async (c) => {
  const ordersData = await readOrdersData();
  const offlineOrders = Object.entries(ordersData)
    .filter(([, value]) => value.orderType === 'offline')
    .map(([key, value]) => ({ key, ...buildOrderPayload(key, value) }));
  return c.json({ orders: offlineOrders, count: offlineOrders.length });
});

app.put('/orders/offline/:orderKey', async (c) => {
  const orderKey = c.req.param('orderKey');
  const updateData = await c.req.json();
  const ordersData = await readOrdersData();
  if (!ordersData[orderKey] || ordersData[orderKey].orderType !== 'offline') {
    return c.json({ success: false, error: '离线订单不存在' }, 404);
  }
  ordersData[orderKey] = { ...ordersData[orderKey], ...updateData };
  const writeSuccess = await writeOrdersData(ordersData);
  if (writeSuccess) {
    return c.json({ success: true, message: '离线订单更新成功', order: ordersData[orderKey] });
  }
  return c.json({ success: false, error: '写入数据库失败' }, 500);
});

app.delete('/orders/offline/:orderKey', async (c) => {
  const orderKey = c.req.param('orderKey');
  const ordersData = await readOrdersData();
  if (!ordersData[orderKey] || ordersData[orderKey].orderType !== 'offline') {
    return c.json({ success: false, error: '离线订单不存在' }, 404);
  }
  delete ordersData[orderKey];
  const writeSuccess = await writeOrdersData(ordersData);
  if (writeSuccess) {
    return c.json({ success: true, message: '离线订单删除成功' });
  }
  return c.json({ success: false, error: '写入数据库失败' }, 500);
});

app.get('/api/online/:address', (c) => {
  const address = c.req.param('address');
  const online = isAddressOnline(address);
  return c.json({ address, online, timestamp: Date.now() });
});

app.get('/api/online', (c) => {
  const onlineAddresses = getOnlineAddresses();
  return c.json({ online: onlineAddresses, count: onlineAddresses.length, timestamp: Date.now() });
});

async function readOrdersData() {
  try {
    const ordersData = {};
    for await (const [key, value] of db.iterator()) {
      ordersData[key] = value;
    }
    return ordersData;
  } catch (error) {
    console.error('Failed to read orders from database:', error);
    return {};
  }
}

async function writeOrdersData(ordersData) {
  try {
    const batch = db.batch();
    const existingKeys = new Set();
    for await (const key of db.keys()) {
      if (key.includes('|')) existingKeys.add(key);
    }
    for (const [key, value] of Object.entries(ordersData)) {
      batch.put(key, value);
      existingKeys.delete(key);
    }
    for (const key of existingKeys) {
      batch.del(key);
    }
    await batch.write();
    return true;
  } catch (error) {
    console.error('Failed to write orders to database:', error);
    return false;
  }
}

app.get('/orders', async (c) => {
  const ordersData = await readOrdersData();
  const ordersList = Object.entries(ordersData).map(([key, value]) => buildOrderPayload(key, value));
  return c.json({ orders: ordersList });
});

app.get('/orders/token/:tokenId', async (c) => {
  const ordersData = await readOrdersData();
  const tokenId = c.req.param('tokenId');
  const filteredOrders = Object.entries(ordersData)
    .filter(([key, value]) => {
      const { tokenId: orderTokenId, buyerAddress } = parseOrderKey(key);
      if (orderTokenId !== tokenId) return false;
      if (value.orderType === 'offline') return value.status !== 'completed';
      if (!isAddressOnline(buyerAddress)) return false;
      const balanceInfo = addressBalances.get(buyerAddress);
      if (!balanceInfo) return false;
      const requiredAmount = value.maxPrice * value.remainingAmount;
      const requiredAmountSats = requiredAmount * 100;
      return balanceInfo.balance >= requiredAmountSats;
    })
    .map(([key, value]) => {
      const { buyerAddress } = parseOrderKey(key);
      const balanceInfo = addressBalances.get(buyerAddress);
      const isOffline = value.orderType === 'offline';
      return {
        ...value,
        tokenId,
        buyerAddress,
        currentBalance: isOffline ? null : balanceInfo ? balanceInfo.balance / 100 : null,
        requiredAmount: value.maxPrice * value.remainingAmount,
      };
    });
  if (!filteredOrders || filteredOrders.length === 0) {
    return c.json(
      {
        error: 'No valid orders found',
        message: '没有找到有效的订单（地址可能离线或余额不足）',
      },
      404,
    );
  }
  return c.json({ orders: filteredOrders, count: filteredOrders.length, timestamp: Date.now() });
});

app.get('/orders/address/:address', async (c) => {
  const ordersData = await readOrdersData();
  const address = c.req.param('address');
  const filteredOrders = Object.entries(ordersData)
    .filter(([key]) => isOrderKeyForAddress(key, address))
    .map(([key, value]) => buildOrderPayload(key, value));
  if (!filteredOrders || filteredOrders.length === 0) {
    return c.json({ error: 'Orders not found' }, 404);
  }
  return c.json({ orders: filteredOrders });
});

app.get('/orders/token/:tokenId/address/:address', async (c) => {
  const ordersData = await readOrdersData();
  const tokenId = c.req.param('tokenId');
  const address = c.req.param('address');
  const filteredOrders = Object.entries(ordersData)
    .filter(([key]) => {
      const parsedKey = parseOrderKey(key);
      return parsedKey.tokenId === tokenId && parsedKey.buyerAddress === address;
    })
    .map(([key, value]) => buildOrderPayload(key, value));
  if (!filteredOrders || filteredOrders.length === 0) {
    return c.json({ error: 'Orders not found' }, 404);
  }
  return c.json({ orders: filteredOrders });
});

app.post('/orders/push/:address', async (c) => {
  try {
    const address = c.req.param('address');
    const rawBody = await c.req.text();
    const requestOrigin = c.req.header('origin') || 'unknown';
    const requestIp = getRequestIp(c.req.raw.headers);

    logApiEvent('orders.push.received', {
      route: '/orders/push/:address',
      address,
      origin: requestOrigin,
      ip: requestIp,
      bodyLength: rawBody.length,
    });

    let newOrdersData;
    try {
      newOrdersData = JSON.parse(rawBody);
    } catch (parseError) {
      logApiEvent('orders.push.invalid_json', {
        route: '/orders/push/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
        error: parseError.message,
      });
      return c.json(
        { success: false, error: '无效的 JSON 数据', details: parseError.message },
        400,
      );
    }

    const orderCount = Object.keys(newOrdersData).length;
    if (orderCount === 0) {
      logApiEvent('orders.push.empty_payload', {
        route: '/orders/push/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
      });
      return c.json(
        {
          success: false,
          error: '收到空订单数据集',
          message: '不能提交空的订单数据集，这会导致删除所有现有订单',
          rawData: rawBody,
        },
        400,
      );
    }

    const validationResult = validateOrdersData(newOrdersData);
    if (!validationResult.valid) {
      logApiEvent('orders.push.validation_failed', {
        route: '/orders/push/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
        errorCount: validationResult.errors.length,
      });
      return c.json(
        { success: false, error: '订单数据验证失败', details: validationResult.errors },
        400,
      );
    }

    const serverOrdersData = await readOrdersData();
    const addressOrderKeys = Object.keys(serverOrdersData).filter((key) => isOrderKeyForAddress(key, address));
    const hasExistingOrders = addressOrderKeys.length > 0;
    const invalidKeys = Object.keys(newOrdersData).filter((key) => !isOrderKeyForAddress(key, address));
    if (invalidKeys.length > 0) {
      logApiEvent('orders.push.invalid_keys', {
        route: '/orders/push/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
        invalidKeyCount: invalidKeys.length,
      });
      return c.json(
        { success: false, error: '订单数据包含不属于该地址的订单', invalidKeys },
        400,
      );
    }

    if (!hasExistingOrders) {
      const mergedData = { ...serverOrdersData, ...newOrdersData };
      try {
        await writeOrdersData(mergedData);
        logApiEvent('orders.push.created', {
          route: '/orders/push/:address',
          address,
          origin: requestOrigin,
          ip: requestIp,
          orderCount,
        });
        return c.json({ success: true, message: '订单数据已完全更新' });
      } catch (writeError) {
        logApiEvent('orders.push.write_failed', {
          route: '/orders/push/:address',
          address,
          origin: requestOrigin,
          ip: requestIp,
          error: writeError.message,
        });
        return c.json({ success: false, error: writeError.message }, 500);
      }
    } else {
      let updated = false;
      const updatedOrdersData = { ...serverOrdersData };
      const removedOrders = [];
      addressOrderKeys.forEach((key) => {
        if (!newOrdersData[key]) {
          delete updatedOrdersData[key];
          removedOrders.push(key);
          updated = true;
        }
      });
      Object.entries(newOrdersData).forEach(([key, newOrder]) => {
        if (isOrderKeyForAddress(key, address)) {
          if (!updatedOrdersData[key]) {
            updatedOrdersData[key] = newOrder;
            updated = true;
          } else {
            const existingOrder = updatedOrdersData[key];
            if (existingOrder.orderType === 'offline' && existingOrder.status !== 'pending') {
              return;
            }
            if (JSON.stringify(existingOrder) !== JSON.stringify(newOrder)) {
              updatedOrdersData[key] = newOrder;
              updated = true;
            }
          }
        }
      });
      if (updated) {
        try {
          await writeOrdersData(updatedOrdersData);
          logApiEvent('orders.push.updated', {
            route: '/orders/push/:address',
            address,
            origin: requestOrigin,
            ip: requestIp,
            orderCount,
            removedOrderCount: removedOrders.length,
          });
          return c.json({ success: true, message: '订单数据已更新', removedOrders });
        } catch (writeError) {
          logApiEvent('orders.push.write_failed', {
            route: '/orders/push/:address',
            address,
            origin: requestOrigin,
            ip: requestIp,
            error: writeError.message,
          });
          return c.json({ success: false, error: writeError.message }, 500);
        }
      }
      logApiEvent('orders.push.no_change', {
        route: '/orders/push/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
        orderCount,
      });
      return c.json({ success: true, message: '无需更新，数据未变化' });
    }
  } catch (error) {
    logApiEvent('orders.push.unhandled_error', {
      route: '/orders/push/:address',
      address: c.req.param('address'),
      origin: c.req.header('origin') || 'unknown',
      ip: getRequestIp(c.req.raw.headers),
      error: error.message,
    });
    return c.json({ success: false, error: error.message }, 500);
  }
});

app.post('/orders/check-hash/:address', async (c) => {
  try {
    const address = c.req.param('address');
    const requestOrigin = c.req.header('origin') || 'unknown';
    const requestIp = getRequestIp(c.req.raw.headers);
    const { orderHashes: clientOrderHashes } = await c.req.json();

    logApiEvent('orders.check_hash.received', {
      route: '/orders/check-hash/:address',
      address,
      origin: requestOrigin,
      ip: requestIp,
      orderHashCount:
        clientOrderHashes && typeof clientOrderHashes === 'object' && !Array.isArray(clientOrderHashes)
          ? Object.keys(clientOrderHashes).length
          : 0,
    });

    if (!clientOrderHashes || typeof clientOrderHashes !== 'object' || Array.isArray(clientOrderHashes)) {
      logApiEvent('orders.check_hash.invalid_payload', {
        route: '/orders/check-hash/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
      });
      return c.json({ match: false, error: 'orderHashes 必须是对象' }, 400);
    }
    const serverOrdersData = await readOrdersData();
    const addressOrders = {};
    Object.entries(serverOrdersData).forEach(([key, value]) => {
      if (isOrderKeyForAddress(key, address)) {
        addressOrders[key] = value;
      }
    });
    if (Object.keys(addressOrders).length === 0) {
      logApiEvent('orders.check_hash.no_server_orders', {
        route: '/orders/check-hash/:address',
        address,
        origin: requestOrigin,
        ip: requestIp,
      });
      return c.json({ match: false, message: '服务器没有该地址的订单数据' });
    }
    const serverOrderHashes = {};
    for (const [key, value] of Object.entries(addressOrders)) {
      const sortedOrder = sortObjectDeep(value);
      const orderHash = crypto.createHash('md5').update(JSON.stringify(sortedOrder)).digest('hex');
      serverOrderHashes[key] = orderHash;
    }
    const diffKeys = [];
    let allMatch = true;
    for (const [key, clientHash] of Object.entries(clientOrderHashes)) {
      if (!serverOrderHashes[key]) {
        diffKeys.push(key);
        allMatch = false;
      } else if (serverOrderHashes[key] !== clientHash) {
        diffKeys.push(key);
        allMatch = false;
      }
    }
    for (const key of Object.keys(serverOrderHashes)) {
      if (!clientOrderHashes[key]) {
        diffKeys.push(key);
        allMatch = false;
      }
    }
    logApiEvent('orders.check_hash.completed', {
      route: '/orders/check-hash/:address',
      address,
      origin: requestOrigin,
      ip: requestIp,
      match: allMatch,
      diffKeyCount: diffKeys.length,
    });
    return c.json({
      match: allMatch,
      message: allMatch ? '数据完全一致' : '数据不一致，需要更新',
      diffKeys,
      serverHashes: serverOrderHashes,
    });
  } catch (error) {
    logApiEvent('orders.check_hash.unhandled_error', {
      route: '/orders/check-hash/:address',
      address: c.req.param('address'),
      origin: c.req.header('origin') || 'unknown',
      ip: getRequestIp(c.req.raw.headers),
      error: error.message,
    });
    return c.json({ match: false, error: error.message }, 500);
  }
});

async function initializeDatabase() {
  try {
    await db.open();
    runtimeState.dbReady = true;
    return true;
  } catch (error) {
    console.error('Failed to open database:', error);
    return false;
  }
}

function buildRedirectLocation(req) {
  const hostHeader = runtimeConfig.redirectHost || req.headers.host || '';
  const host = String(hostHeader).replace(/:\d+$/, '');
  return `https://${host}${req.url || '/'}`;
}

function hasRequestBody(method) {
  return !['GET', 'HEAD'].includes(String(method).toUpperCase());
}

function buildForwardHeaders(req) {
  const headers = {};
  for (const name of FORWARDED_REQUEST_HEADERS) {
    const value = req.headers[name];
    if (value) {
      headers[name] = value;
    }
  }
  return headers;
}

async function relayFetchResponse(response, res) {
  const contentType = response.headers.get('content-type') || '';
  const responseBody = await response.text();

  if (contentType) {
    res.set('Content-Type', contentType);
  }

  if (!responseBody) {
    return res.status(response.status).end();
  }

  if (contentType.includes('application/json')) {
    try {
      return res.status(response.status).json(JSON.parse(responseBody));
    } catch (error) {
      console.error('Failed to parse upstream JSON response:', error);
    }
  }

  return res.status(response.status).send(responseBody);
}

async function forwardToHono(req, res) {
  try {
    if (req.path.startsWith('/orders/push/') || req.path.startsWith('/orders/check-hash/')) {
      logApiEvent('proxy.route.received', {
        route: req.path,
        method: req.method,
        origin: req.headers.origin || 'unknown',
        ip: getRequestIp(req.headers),
      });
    }

    const response = await fetch(`${HONO_API}${req.originalUrl}`, {
      method: req.method,
      headers: buildForwardHeaders(req),
      body: hasRequestBody(req.method) ? JSON.stringify(req.body ?? {}) : undefined,
    });

    return await relayFetchResponse(response, res);
  } catch (error) {
    console.error('Error forwarding to Hono service:', error);
    return res.status(502).json({ error: 'Upstream Hono service unavailable' });
  }
}

function startProxyLayer() {
  const wsProxy = httpProxy.createProxyServer({
    target: `ws://${runtimeConfig.internalHost}:${runtimeConfig.wsPort}`,
    ws: true,
  });

  const proxyServer = runtimeConfig.tlsEnabled
    ? https.createServer(loadTlsCredentials(), proxyApp)
    : http.createServer(proxyApp);

  proxyServer.on('upgrade', (req, socket, head) => {
    console.log('WebSocket 连接升级请求');
    wsProxy.ws(req, socket, head);
  });

  wsProxy.on('error', (err, req, res) => {
    console.error('WebSocket 代理错误:', err);
    if (res && res.writeHead) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('WebSocket 代理错误');
    }
  });

  proxyServer.listen(runtimeConfig.proxyPort, runtimeConfig.host, () => {
    const protocol = runtimeConfig.tlsEnabled ? 'HTTPS' : 'HTTP';
    console.log(`${protocol} proxy server running on ${runtimeConfig.host}:${runtimeConfig.proxyPort}`);
  });

  runtimeState.proxyServer = proxyServer;

  if (runtimeConfig.tlsEnabled && runtimeConfig.httpRedirectEnabled) {
    runtimeState.redirectServer = http
      .createServer((req, res) => {
        res.writeHead(301, { Location: buildRedirectLocation(req) });
        res.end();
      })
      .listen(runtimeConfig.httpRedirectPort, runtimeConfig.host, () => {
        console.log(
          `HTTP redirect server running on ${runtimeConfig.host}:${runtimeConfig.httpRedirectPort}`,
        );
      });
  }
}

async function closeServer(server) {
  if (!server) return;

  await new Promise((resolve, reject) => {
    server.close((error) => {
      if (!error || error.code === 'ERR_SERVER_NOT_RUNNING') {
        resolve();
        return;
      }
      reject(error);
    });
  });
}

async function startServers() {
  if (runtimeState.dbReady) {
    return runtimeState;
  }

  const success = await initializeDatabase();
  if (!success) {
    throw new Error('数据库初始化失败');
  }

  runtimeState.wsService = createWebSocketServer(
    runtimeConfig.wsPort,
    addressBalances,
    getAddressBalance,
  );

  runtimeState.balanceRefreshInterval = setInterval(updateAllBalances, runtimeConfig.balanceRefreshMs);

  if (!process.env.BUN) {
    runtimeState.honoServer = serve({
      fetch: app.fetch,
      port: runtimeConfig.honoPort,
      hostname: runtimeConfig.internalHost,
    });
    console.log(`Hono service running on ${runtimeConfig.internalHost}:${runtimeConfig.honoPort}`);
  }

  logApiEvent('service.starting', {
    proxyPort: runtimeConfig.proxyPort,
    honoPort: runtimeConfig.honoPort,
    wsPort: runtimeConfig.wsPort,
    corsOrigin: runtimeConfig.corsOrigin,
    tlsEnabled: runtimeConfig.tlsEnabled,
  });

  startProxyLayer();
  return runtimeState;
}

async function stopServers() {
  if (runtimeState.shuttingDown) {
    return;
  }

  runtimeState.shuttingDown = true;

  if (runtimeState.balanceRefreshInterval) {
    clearInterval(runtimeState.balanceRefreshInterval);
    runtimeState.balanceRefreshInterval = null;
  }

  pendingBalanceRequests.clear();

  if (runtimeState.redirectServer) {
    await closeServer(runtimeState.redirectServer);
    runtimeState.redirectServer = null;
  }

  if (runtimeState.proxyServer) {
    await closeServer(runtimeState.proxyServer);
    runtimeState.proxyServer = null;
  }

  if (runtimeState.honoServer) {
    await closeServer(runtimeState.honoServer);
    runtimeState.honoServer = null;
  }

  if (runtimeState.wsService) {
    await runtimeState.wsService.close();
    runtimeState.wsService = null;
  }

  if (runtimeState.dbReady) {
    await db.close();
    runtimeState.dbReady = false;
  }

  runtimeState.shuttingDown = false;
}

const proxyRouteDefinitions = [
  ['get', '/health'],
  ['get', '/ready'],
  ['get', '/orders'],
  ['get', '/orders/offline'],
  ['get', '/orders/token/:tokenId'],
  ['get', '/orders/address/:address'],
  ['get', '/orders/token/:tokenId/address/:address'],
  ['post', '/orders/push/:address'],
  ['post', '/orders/check-hash/:address'],
  ['put', '/orders/offline/:orderKey'],
  ['delete', '/orders/offline/:orderKey'],
  ['get', '/api/online'],
  ['get', '/api/online/:address'],
  ['get', '/api/balance/:address'],
  ['get', '/api/balances'],
];

for (const [method, routePath] of proxyRouteDefinitions) {
  proxyApp[method](routePath, forwardToHono);
}

function registerSignalHandlers() {
  const handleShutdown = async (signal) => {
    try {
      console.log(`Received ${signal}, shutting down...`);
      await stopServers();
      process.exit(0);
    } catch (error) {
      console.error('Failed during shutdown:', error);
      process.exit(1);
    }
  };

  process.once('SIGINT', () => {
    void handleShutdown('SIGINT');
  });

  process.once('SIGTERM', () => {
    void handleShutdown('SIGTERM');
  });
}

if (parseBoolean(process.env.ACWS_AUTOSTART, process.env.NODE_ENV !== 'test')) {
  registerSignalHandlers();
  startServers().catch((error) => {
    console.error('Failed to start ACWS service:', error);
    process.exit(1);
  });
}

export { app, runtimeConfig, startServers, stopServers };
export default app;
