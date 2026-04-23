export const parseBoolean = (value, fallback = false) => {
  if (value == null) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
};

export const parseNumber = (value, fallback) => {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
};

export const normalizeOptionalString = (value) => {
  if (value == null) return null;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
};

export const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function buildCorsOptions(rawOrigin) {
  if (!rawOrigin || rawOrigin === '*') {
    return {};
  }

  const origins = rawOrigin
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

  if (origins.length === 0) {
    return {};
  }

  return {
    origin: origins.length === 1 ? origins[0] : origins,
  };
}

export function parseOrderKey(orderKey) {
  const [tokenId = '', buyerAddress = ''] = String(orderKey).split('|');
  return { tokenId, buyerAddress };
}

export function isOrderKeyForAddress(orderKey, address) {
  return parseOrderKey(orderKey).buyerAddress === address;
}

export function buildOrderPayload(orderKey, order) {
  const { tokenId, buyerAddress } = parseOrderKey(orderKey);
  return { ...order, tokenId, buyerAddress };
}

export function sortObjectDeep(obj) {
  if (typeof obj !== 'object' || obj === null) return obj;
  if (Array.isArray(obj)) return obj.map(sortObjectDeep);
  const sortedObj = {};
  Object.keys(obj)
    .sort()
    .forEach((key) => {
      sortedObj[key] = sortObjectDeep(obj[key]);
    });
  return sortedObj;
}

export function validateOrdersData(ordersData) {
  const errors = [];
  const keys = Object.keys(ordersData);
  if (keys.length !== new Set(keys).size) {
    errors.push('存在重复的订单键名');
  }
  for (const [key, order] of Object.entries(ordersData)) {
    const { tokenId, buyerAddress } = parseOrderKey(key);
    if (!tokenId || !buyerAddress) {
      errors.push(`订单 ${key} 键名格式无效，应为 tokenId|address`);
      continue;
    }
    if (!order || typeof order !== 'object' || Array.isArray(order)) {
      errors.push(`订单 ${key} 数据格式无效`);
      continue;
    }
    if (order.status === 'completed' && order.remainingAmount !== 0 && order.orderType !== 'offline') {
      errors.push(`订单 ${key} 状态为 completed 但 remainingAmount 不为 0`);
    }
    if (order.status === 'pending' && order.transactions && order.transactions.length > 0) {
      errors.push(`订单 ${key} 状态为 pending 但存在 transactions 记录`);
    }
    if (order.status === 'partial' && order.remainingAmount === 0) {
      errors.push(`订单 ${key} 状态为 partial 但 remainingAmount 为 0`);
    }
  }
  return { valid: errors.length === 0, errors };
}
