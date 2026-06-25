/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * Browser counterpart of {@link ./dispatcher.ts}. The Node build configures the underlying HTTP
 * transport through an `undici` dispatcher, but `undici` is built on `node:net`/`node:tls` and
 * cannot run in a browser. Bundlers pick this file up via the `"browser"` field in `package.json`,
 * so a browser bundle never imports `undici`.
 *
 * The browser's HTTP stack manages the connection-pool options that the Node dispatcher carries,
 * so {@link buildDispatcher} throws if any is set explicitly (rather than silently ignoring it)
 * and otherwise returns `undefined`, leaving the connection to issue a plain `fetch`. It is the
 * only export the connection imports through the swapped `./dispatcher.js` path.
 */

/** Transport options accepted by {@link buildDispatcher}; mirrors the Node dispatcher's options. */
type DispatcherOptions = {
  maxConnections?: number;
  readTimeoutMillis?: number;
  maxResponseHeaderBytes?: number;
  keepAliveTimeMillis?: number;
  proxy?: string;
};

/**
 * Connection-pool / transport options that only the Node (`undici`) dispatcher can honor. The
 * browser's HTTP stack owns these concerns and exposes no `fetch` API to configure them, so they
 * are rejected here rather than silently ignored. `readTimeoutMillis` is intentionally not in this list:
 * it has a browser analog (an `AbortSignal`-based timeout) and is handled by the connection.
 */
const NODE_ONLY_OPTIONS: (keyof DispatcherOptions)[] = [
  'maxConnections',
  'keepAliveTimeMillis',
  'maxResponseHeaderBytes',
  'proxy',
];

/**
 * In the browser there is no `undici` dispatcher, so `fetch` is issued without one and the browser
 * manages the transport. Any {@link NODE_ONLY_OPTIONS} set explicitly cannot be honored and throws,
 * so a misconfiguration surfaces immediately instead of being silently dropped. With none set this
 * returns `undefined` and the connection omits the `dispatcher` field.
 */
export function buildDispatcher(options: DispatcherOptions = {}): undefined {
  const unsupported = NODE_ONLY_OPTIONS.filter((key) => options[key] !== undefined);
  if (unsupported.length > 0) {
    throw new Error(
      `The following connection options are not supported in the browser and are managed by the ` +
      `browser's HTTP stack: ${unsupported.join(', ')}. Remove them when running in a browser ` +
      `(they are supported in Node).`,
    );
  }
  return undefined;
}
