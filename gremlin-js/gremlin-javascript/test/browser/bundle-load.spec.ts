/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { test, expect } from '@playwright/test';

test('the bundle loads and exposes the public API', async ({ page }) => {
  await page.goto('/');

  const hasApi = await page.evaluate(() => {
    const { gremlin } = (window as any).__gremlinBrowserSmoke;
    return (
      typeof gremlin.driver.DriverRemoteConnection === 'function' &&
      typeof gremlin.process.AnonymousTraversalSource === 'function' &&
      typeof gremlin.structure.Vertex === 'function'
    );
  });

  expect(hasApi).toBe(true);
});

test('a traversal source can be built and a traversal serializes to GremlinLang', async ({ page }) => {
  await page.goto('/');

  const gremlinLang = await page.evaluate(() => {
    const { gremlin } = (window as any).__gremlinBrowserSmoke;
    const connection = new gremlin.driver.DriverRemoteConnection('http://localhost:1/gremlin');
    const g = gremlin.process.AnonymousTraversalSource.traversal().with_(connection);
    return g.V().has('name', 'marko').toString();
  });

  expect(gremlinLang).toBe("g.V().has('name','marko')");
});

test('DriverRemoteConnection uses the browser dispatcher, not the Node/undici one', async ({ page }) => {
  await page.goto('/');

  const message = await page.evaluate(() => {
    const { gremlin } = (window as any).__gremlinBrowserSmoke;
    try {
      // maxConnections is Node-only; browser dispatcher should reject it.
      new gremlin.driver.DriverRemoteConnection('http://localhost:1/gremlin', { maxConnections: 5 });
      return 'no error thrown';
    } catch (err) {
      return (err as Error).message;
    }
  });

  expect(message).toContain('maxConnections');
  expect(message).toContain("managed by the browser's HTTP stack");
});

test('sigv4 auth requires a credentialsProvider (no default chain in the browser)', async ({ page }) => {
  await page.goto('/');

  const message = await page.evaluate(() => {
    const { gremlin } = (window as any).__gremlinBrowserSmoke;
    try {
      gremlin.driver.auth.sigv4('us-east-1', 'neptune-db');
      return 'no error thrown';
    } catch (err) {
      return (err as Error).message;
    }
  });

  expect(message).toContain('requires a credentialsProvider');
});

test('sigv4 auth signs a request using SubtleCrypto, matching the Node build byte-for-byte', async ({ page }) => {
  await page.goto('/');

  const authHeader = await page.evaluate(async () => {
    const { gremlin, Buffer } = (window as any).__gremlinBrowserSmoke;

    const RealDate = Date;
    class FixedDate extends RealDate {
      constructor(...args: []) {
        if (args.length === 0) {
          super('2024-01-01T00:00:00Z');
        } else {
          super(...args);
        }
      }
      static now() {
        return new RealDate('2024-01-01T00:00:00Z').getTime();
      }
    }
    (window as any).Date = FixedDate;

    const request = new gremlin.driver.HttpRequest(
      'POST',
      'https://example.neptune.amazonaws.com/gremlin',
      {},
      Buffer.from('{"gremlin":"g.V()"}'),
    );
    const interceptor = gremlin.driver.auth.sigv4('us-east-1', 'neptune-db', () => ({
      accessKeyId: 'MOCK_ACCESS_KEY',
      secretAccessKey: 'MOCK_SECRET_KEY',
    }));
    await interceptor(request);
    return request.headers['authorization'];
  });

  // Computed independently via lib/driver/auth.ts (the Node build, @smithy/hash-node) for the same
  // fixed date/credentials/body - see test/unit/auth-browser-test.js for that derivation.
  expect(authHeader).toBe(
    'AWS4-HMAC-SHA256 Credential=MOCK_ACCESS_KEY/20240101/us-east-1/neptune-db/aws4_request, ' +
    'SignedHeaders=host;x-amz-content-sha256;x-amz-date, ' +
    'Signature=08b4bbdacdbc6603946dbdcc8465e1a87caa517f38fe21f14cb7cfaaf64dd824',
  );
});

test('the language translator subpath works standalone in the browser', async ({ page }) => {
  await page.goto('/');

  const translated = await page.evaluate(() => {
    const { gremlinLanguage } = (window as any).__gremlinBrowserSmoke;
    return gremlinLanguage.GremlinTranslator.translate("g.V().has('name','marko')", 'g', 'PYTHON').toString();
  });

  expect(translated).toBe("g.V().has('name', 'marko')");
});
