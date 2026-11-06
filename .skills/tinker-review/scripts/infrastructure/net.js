/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Shared networking helpers for the two Gremlin Servers a review stands up: the
// Phase-1 knowledge-graph server (docker.js) and the Phase-2 functional-test
// server built from PR source (functional/setup.js). Both pick a free port and
// poll the same HTTP readiness endpoint, so that logic lives here once.

import { createServer } from "node:net";
import { get } from "node:http";

const POLL_INTERVAL_MS = 500;

/**
 * Ask the OS for a free TCP port by binding to 0 and reading it back.
 * @returns {Promise<number>}
 */
export function findAvailablePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

/**
 * Poll an HTTP endpoint until it responds or the timeout elapses. Gremlin
 * Server answers a plain GET on `/gremlin` once its WebSocket listener is up,
 * so a successful response of any status means "ready".
 *
 * @param {number} port - localhost port to poll
 * @param {number} timeoutMs - max wait before rejecting
 * @param {object} [options]
 * @param {string} [options.path] - request path (default "/gremlin")
 * @returns {Promise<void>}
 */
export function waitForHttp(port, timeoutMs, options = {}) {
  const path = options.path || "/gremlin";
  const deadline = Date.now() + timeoutMs;

  return new Promise((resolve, reject) => {
    function poll() {
      if (Date.now() > deadline) {
        reject(new Error(`Server on port ${port} did not become ready within ${timeoutMs}ms`));
        return;
      }

      const req = get(`http://localhost:${port}${path}`, (res) => {
        res.resume();
        resolve();
      });
      req.on("error", () => {
        setTimeout(poll, POLL_INTERVAL_MS);
      });
    }

    poll();
  });
}
