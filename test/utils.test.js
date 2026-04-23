import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildCorsOptions,
  buildOrderPayload,
  isOrderKeyForAddress,
  normalizeOptionalString,
  parseBoolean,
  parseNumber,
  parseOrderKey,
  sortObjectDeep,
  validateOrdersData,
} from '../src/utils.js';

test('parseBoolean handles common truthy and falsy values', () => {
  assert.equal(parseBoolean('true'), true);
  assert.equal(parseBoolean('YES'), true);
  assert.equal(parseBoolean('0', true), false);
  assert.equal(parseBoolean(undefined, true), true);
});

test('parseNumber falls back for invalid input', () => {
  assert.equal(parseNumber('42', 1), 42);
  assert.equal(parseNumber('not-a-number', 7), 7);
});

test('normalizeOptionalString trims empty values to null', () => {
  assert.equal(normalizeOptionalString('  hello  '), 'hello');
  assert.equal(normalizeOptionalString('   '), null);
});

test('buildCorsOptions supports wildcard and multi-origin config', () => {
  assert.deepEqual(buildCorsOptions('*'), {});
  assert.deepEqual(buildCorsOptions('https://a.com, https://b.com'), {
    origin: ['https://a.com', 'https://b.com'],
  });
});

test('order key helpers parse and match tokenId|address keys', () => {
  const key = 'token123|ecash:qptest';
  assert.deepEqual(parseOrderKey(key), {
    tokenId: 'token123',
    buyerAddress: 'ecash:qptest',
  });
  assert.equal(isOrderKeyForAddress(key, 'ecash:qptest'), true);
  assert.equal(isOrderKeyForAddress(key, 'ecash:other'), false);
  assert.deepEqual(buildOrderPayload(key, { status: 'pending' }), {
    status: 'pending',
    tokenId: 'token123',
    buyerAddress: 'ecash:qptest',
  });
});

test('sortObjectDeep sorts nested object keys deterministically', () => {
  assert.deepEqual(sortObjectDeep({ b: 1, a: { d: 2, c: 3 } }), {
    a: { c: 3, d: 2 },
    b: 1,
  });
});

test('validateOrdersData accepts valid offline and pending orders', () => {
  const result = validateOrdersData({
    'token1|ecash:qptest': {
      orderType: 'offline',
      status: 'completed',
      remainingAmount: 10,
      transactions: [],
    },
    'token2|ecash:qptest2': {
      orderType: 'online',
      status: 'pending',
      remainingAmount: 5,
      transactions: [],
    },
  });

  assert.equal(result.valid, true);
  assert.deepEqual(result.errors, []);
});

test('validateOrdersData rejects invalid keys and invalid state transitions', () => {
  const result = validateOrdersData({
    invalid: {
      orderType: 'online',
      status: 'partial',
      remainingAmount: 0,
      transactions: [{ txid: '123' }],
    },
    'token3|ecash:qptest': {
      orderType: 'online',
      status: 'pending',
      remainingAmount: 1,
      transactions: [{ txid: '456' }],
    },
  });

  assert.equal(result.valid, false);
  assert.match(result.errors.join('\n'), /键名格式无效/);
  assert.match(result.errors.join('\n'), /pending 但存在 transactions 记录/);
});
