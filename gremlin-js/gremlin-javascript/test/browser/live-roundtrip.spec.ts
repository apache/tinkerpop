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
 *
 * Tier 2 smoke test: needs a running Gremlin Server (the same one the Node integration tests use;
 * see test/helper.js and docker-compose.yml, port 45940). Bring one up with
 * `docker compose up -d gremlin-server-test-js` before running `npm run browser-test-live`.
 *
 * This proves the *transport* actually round-trips from inside a real browser engine: a plain
 * `fetch()`, no undici, issued from the bundle built by build-bundle.mjs. It does not prove
 * gremlin-javascript is safe to point directly at a production Gremlin Server from a browser: this
 * test server does not send CORS headers, so the run disables the browser's same-origin checks for
 * this project only (see playwright.config.ts's "chromium-live" project). A real browser deployment
 * needs the server (or a proxy in front of it) to send appropriate CORS headers, or needs a
 * same-origin proxy in front of Gremlin Server - that is a deployment concern, not something this
 * driver can supply on its own.
 */
import { test, expect } from '@playwright/test';

const serverUrl = process.env.GREMLIN_SERVER_URL ?? 'http://localhost:45940/gremlin';

test('a traversal built in the browser round-trips over HTTP to a real Gremlin Server', async ({ page }) => {
  await page.goto('/');

  const counts = await page.evaluate(async (url) => {
    const { gremlin } = (window as any).__gremlinBrowserSmoke;
    const connection = new gremlin.driver.DriverRemoteConnection(url);
    await connection.open();
    const g = gremlin.process.AnonymousTraversalSource.traversal().with_(connection);
    try {
      // Data-independent: works against any server, regardless of what graph (if any) it has loaded.
      return await g.inject(1, 2, 3).count().toList();
    } finally {
      await connection.close();
    }
  }, serverUrl);

  expect(counts).toEqual([3]);
});
