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
 * Shared types and interfaces for Gremlin service modules
 */

import type { driver, process } from 'gremlin';
import type { GraphSchema } from './models/index.js';

export type { GraphSchema };

export type GremlinClientType = driver.Client;
export type GremlinResultSet = driver.ResultSet;
export type GremlinConnection = driver.DriverRemoteConnection;
export type GraphTraversalSource = process.GraphTraversalSource;

/**
 * Internal connection state - represents a fully initialized connection
 */
export interface ConnectionState {
  readonly client: GremlinClientType;
  readonly connection: GremlinConnection;
  readonly g: GraphTraversalSource;
  readonly lastUsed: number;
}

/**
 * Configuration for schema generation
 */
export interface SchemaConfig {
  includeSampleValues: boolean;
  maxEnumValues: number;
  includeCounts: boolean;
  enumCardinalityThreshold: number;
  enumPropertyDenyList: string[];
  timeoutMs?: number;
  batchSize?: number;
}

/**
 * Schema cache entry
 */
export interface SchemaCacheEntry {
  schema: GraphSchema;
  timestamp: number;
}

/**
 * Gremlin service status information
 */
export interface ServiceStatus {
  /** Overall connection status */
  status: 'connected' | 'disconnected' | 'error';
}
