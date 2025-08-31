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
 * @fileoverview Schema caching with Effect.ts patterns and intelligent TTL management.
 *
 * This module provides a set of functions for creating and managing a schema cache.
 * It uses `Effect.Ref` for concurrent state management and `Effect.Duration` for
 * time-to-live (TTL) based cache validation. The cache can be manually invalidated
 * or automatically refreshed.
 */

import { Effect, Ref, Option, Duration } from 'effect';
import type { GraphSchema } from './models/index.js';
import type { SchemaCacheEntry } from './types.js';
import type { GremlinConnectionError, GremlinQueryError } from '../errors.js';

const SCHEMA_CACHE_TTL = Duration.minutes(5);

/**
 * Creates a new schema cache as a `Ref`.
 *
 * The cache is initialized as `Option.none()`, indicating that it is empty.
 *
 * @returns An `Effect` that resolves to a `Ref` containing an `Option<SchemaCacheEntry>`.
 */
export const createSchemaCache = () => Ref.make<Option.Option<SchemaCacheEntry>>(Option.none());

/**
 * Internal helper to check if cache entry is valid (not exported)
 */
const isCacheValid = (cacheEntry: SchemaCacheEntry): boolean => {
  const now = Date.now();
  const ttlMs = Duration.toMillis(SCHEMA_CACHE_TTL);
  return now - cacheEntry.timestamp < ttlMs;
};

/**
 * Retrieves the schema from the cache.
 *
 * If the cache contains a valid entry, it is returned. Otherwise, a new schema
 * is generated using the provided `generateSchema` effect, and the cache is updated.
 *
 * @param cacheRef A `Ref` to the schema cache.
 * @param generateSchema An `Effect` that generates a new `GraphSchema`.
 * @returns An `Effect` that resolves to the `GraphSchema` or fails with an error.
 */
export const getCachedSchema = (
  cacheRef: Ref.Ref<Option.Option<SchemaCacheEntry>>,
  generateSchema: Effect.Effect<GraphSchema, GremlinConnectionError | GremlinQueryError>
) =>
  Effect.gen(function* () {
    const cacheEntry = yield* Ref.get(cacheRef);

    // Check if we have a valid cached schema
    const validCachedSchema = Option.flatMap(cacheEntry, entry =>
      isCacheValid(entry) ? Option.some(entry.schema) : Option.none()
    );

    if (Option.isSome(validCachedSchema)) {
      yield* Effect.logDebug('Using cached schema');
      return Option.getOrThrow(validCachedSchema);
    }

    // Generate fresh schema and cache it
    yield* Effect.logInfo('Generating fresh schema');
    const schema = yield* generateSchema;

    yield* Ref.set(
      cacheRef,
      Option.some({
        schema,
        timestamp: Date.now(),
      })
    );

    return schema;
  });

/**
 * Retrieves the cached schema without generating a new one if it's missing or invalid.
 *
 * @param cacheRef A `Ref` to the schema cache.
 * @returns An `Effect` that resolves to the `GraphSchema` or `null` if the cache is empty.
 */
export const peekCachedSchema = (cacheRef: Ref.Ref<Option.Option<SchemaCacheEntry>>) =>
  Effect.gen(function* () {
    const cacheEntry = yield* Ref.get(cacheRef);
    return Option.match(cacheEntry, {
      onNone: () => null,
      onSome: entry => entry.schema,
    });
  });

/**
 * Invalidates the schema cache by setting it to `Option.none()`.
 *
 * @param cacheRef A `Ref` to the schema cache.
 * @returns An `Effect` that completes when the cache is invalidated.
 */
export const invalidateSchemaCache = (cacheRef: Ref.Ref<Option.Option<SchemaCacheEntry>>) =>
  Effect.gen(function* () {
    yield* Effect.logInfo('Invalidating schema cache');
    yield* Ref.set(cacheRef, Option.none());
  });

/**
 * Refreshes the schema cache by invalidating it and then regenerating the schema.
 *
 * @param cacheRef A `Ref` to the schema cache.
 * @param generateSchema An `Effect` that generates a new `GraphSchema`.
 * @returns An `Effect` that completes when the cache is refreshed.
 */
export const refreshSchemaCache = (
  cacheRef: Ref.Ref<Option.Option<SchemaCacheEntry>>,
  generateSchema: Effect.Effect<GraphSchema, GremlinConnectionError | GremlinQueryError>
) =>
  Effect.gen(function* () {
    yield* Effect.logInfo('Refreshing schema cache');
    yield* invalidateSchemaCache(cacheRef);
    yield* getCachedSchema(cacheRef, generateSchema);
  });
