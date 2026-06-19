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

import { Agent, ProxyAgent, buildConnector, Dispatcher } from 'undici';

/** Default concurrent-connections-per-origin cap (Node's global fetch is uncapped). */
export const DEFAULT_MAX_CONNECTIONS = 128;

/** Canonical TinkerPop 4.x default TCP keep-alive idle time (30s), shared across the GLVs. */
export const DEFAULT_KEEP_ALIVE_TIME = 30000;

/** Options for the default undici {@link Dispatcher}; unset fields use undici defaults. */
export type DispatcherOptions = {
  /** Max concurrent connections per origin. Defaults to {@link DEFAULT_MAX_CONNECTIONS}. */
  maxConnections?: number;
  /** Idle-read (body) timeout in ms. Maps to undici `bodyTimeout`. */
  readTimeout?: number;
  /** Max response header size in bytes. Maps to undici `maxHeaderSize`. */
  maxResponseHeaderBytes?: number;
  /** Idle ms before TCP keep-alive probes begin. Defaults to 30s; `0` disables keep-alive. */
  keepAliveTime?: number;
  /** HTTP proxy URI. When set, a {@link ProxyAgent} is built instead of an {@link Agent}. */
  proxy?: string;
};

/**
 * Resolves the effective keep-alive idle delay: unset falls back to {@link DEFAULT_KEEP_ALIVE_TIME},
 * a non-positive value disables keep-alive (returns `null`).
 */
export function resolveKeepAliveTime(keepAliveTime?: number): number | null {
  const effective = keepAliveTime ?? DEFAULT_KEEP_ALIVE_TIME;
  return effective > 0 ? effective : null;
}

/**
 * Builds an undici connector that sets `SO_KEEPALIVE` with the given idle delay on each new socket.
 * `baseConnector` is parameterised so the wiring can be unit-tested with a fake connector.
 */
export function buildKeepAliveConnector(
  delay: number,
  baseConnector: ReturnType<typeof buildConnector> = buildConnector({}),
): ReturnType<typeof buildConnector> {
  return (connectOptions: buildConnector.Options, callback: buildConnector.Callback) => {
    baseConnector(connectOptions, (err, socket) => {
      if (err) {
        callback(err, null);
        return;
      }
      if (socket) {
        socket.setKeepAlive(true, delay);
      }
      callback(null, socket);
    });
  };
}

/**
 * Maps the discrete driver options onto undici {@link Agent.Options} (the single source of truth
 * for that mapping). Exported as a seam so the mapping can be asserted in unit tests.
 */
export function buildAgentOptions(options: DispatcherOptions = {}): Agent.Options {
  const maxConnections = options.maxConnections ?? DEFAULT_MAX_CONNECTIONS;

  const agentOptions: Agent.Options = {
    connections: maxConnections,
  };

  // Connect/idle timeouts are intentionally left to undici defaults (the GLV spec marks the JS
  // connect/idle timeout as N/A), not exposed as driver options.
  if (options.readTimeout !== undefined) {
    agentOptions.bodyTimeout = options.readTimeout;
  }
  if (options.maxResponseHeaderBytes !== undefined) {
    agentOptions.maxHeaderSize = options.maxResponseHeaderBytes;
  }

  const keepAliveDelay = resolveKeepAliveTime(options.keepAliveTime);
  if (keepAliveDelay !== null) {
    agentOptions.connect = buildKeepAliveConnector(keepAliveDelay);
  }

  return agentOptions;
}

/**
 * Builds the default undici {@link Dispatcher}: a {@link ProxyAgent} when `proxy` is set, otherwise
 * a plain {@link Agent}. (Agent and ProxyAgent share one dispatcher slot, so only one is built.)
 */
export function buildDispatcher(options: DispatcherOptions = {}): Dispatcher {
  const agentOptions = buildAgentOptions(options);

  if (options.proxy) {
    return new ProxyAgent({ uri: options.proxy, ...agentOptions });
  }

  return new Agent(agentOptions);
}
