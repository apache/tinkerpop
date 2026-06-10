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

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createServer } from "node:net";
import { get } from "node:http";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const exec = promisify(execFile);

const DEFAULT_IMAGE = "tinkerpop/gremlin-server:3.8.1";
const DEFAULT_TIMEOUT_MS = 30000;
const POLL_INTERVAL_MS = 500;

const INIT_GROOVY = `def globals = [:]
globals << [g : traversal().withEmbedded(graph)]
globals << [a : traversal().withEmbedded(graph).withComputer()]
`;

/**
 * Start a Gremlin Server Docker container with TinkerGraph.
 * Configures two traversal sources:
 *   'g' — standard traversal
 *   'a' — withComputer() for OLAP steps like connectedComponent()
 *
 * Uses the stock 3.8.1 image and config. Only overrides the init script
 * to add the 'a' binding.
 *
 * @param {object} options
 * @param {string} [options.image] - Docker image (default: "tinkerpop/gremlin-server:3.8.1")
 * @param {number} [options.timeoutMs] - Max wait for readiness (default: 30000)
 * @returns {Promise<ServerHandle>}
 */
export async function startServer(options = {}) {
  const image = options.image || DEFAULT_IMAGE;
  const timeoutMs = options.timeoutMs || DEFAULT_TIMEOUT_MS;

  const port = await findAvailablePort();
  const configDir = `/tmp/gremlin-review-${port}`;

  await mkdir(join(configDir, "scripts"), { recursive: true });
  await writeFile(join(configDir, "scripts", "init.groovy"), INIT_GROOVY);

  const { stdout } = await exec("docker", [
    "run", "-d",
    "--name", `pr-review-graph-${port}`,
    "-p", `${port}:8182`,
    "-v", `${configDir}/scripts/init.groovy:/opt/gremlin-server/scripts/empty-sample.groovy`,
    image,
  ]);

  const containerId = stdout.trim();
  const url = `ws://localhost:${port}/gremlin`;

  const handle = { port, containerId, url, configDir };

  try {
    await waitForReady(port, timeoutMs);
  } catch (err) {
    await stopServer(handle).catch(() => {});
    throw err;
  }

  return handle;
}

/**
 * Stop and remove the Gremlin Server container.
 *
 * @param {ServerHandle} handle - Handle returned by startServer
 * @returns {Promise<void>}
 */
export async function stopServer(handle) {
  await exec("docker", ["stop", handle.containerId]).catch(() => {});
  await exec("docker", ["rm", handle.containerId]).catch(() => {});
}

function findAvailablePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

function waitForReady(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  return new Promise((resolve, reject) => {
    function poll() {
      if (Date.now() > deadline) {
        reject(new Error(`Gremlin Server did not become ready within ${timeoutMs}ms`));
        return;
      }

      const req = get(`http://localhost:${port}/gremlin`, (res) => {
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
