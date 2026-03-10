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

import { Context, Effect } from 'effect';
import type { GremlinConnectionError } from '../errors.js';
import type { ConnectionState } from './types.js';

/**
 * Gremlin client service providing lazy, reconnect-capable connection management.
 *
 * `getConnection` returns the existing connection from the cache, or creates a new
 * one if none exists. Fails with `GremlinConnectionError` if no endpoint is configured
 * or if the connection attempt fails.
 *
 * `invalidate` closes the current connection (if any) and clears the cache so the
 * next `getConnection` call creates a fresh connection.
 */
export class GremlinClient extends Context.Tag('GremlinClient')<
  GremlinClient,
  {
    readonly getConnection: Effect.Effect<ConnectionState, GremlinConnectionError>;
    readonly invalidate: Effect.Effect<void, never>;
  }
>() {}
