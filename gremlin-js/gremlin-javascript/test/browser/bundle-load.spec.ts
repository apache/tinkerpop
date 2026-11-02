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

test('sigv4 auth throws a clear browser-specific error instead of pulling in the AWS SDK', async ({ page }) => {
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

  expect(message).toContain('not supported in the browser');
});

test('the language translator subpath works standalone in the browser', async ({ page }) => {
  await page.goto('/');

  const translated = await page.evaluate(() => {
    const { gremlinLanguage } = (window as any).__gremlinBrowserSmoke;
    return gremlinLanguage.GremlinTranslator.translate("g.V().has('name','marko')", 'g', 'PYTHON').toString();
  });

  expect(translated).toBe("g.V().has('name', 'marko')");
});
