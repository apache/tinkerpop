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
 * @fileoverview Shared utilities for Gremlin query execution and batched processing.
 *
 * Provides common patterns for Effect-TS based Gremlin query execution with
 * standardized error handling, batching, and timeout management.
 */

import { Effect } from 'effect';
import gremlin from 'gremlin';
import { Errors, type GremlinQueryError } from '../errors.js';
import type { process } from 'gremlin';

type GraphTraversalSource = process.GraphTraversalSource;

const { label } = gremlin.process.statics;

/**
 * Processes items in controlled batches with concurrency limiting.
 *
 * @param items - Array of items to process
 * @param batchSize - Number of items per batch
 * @param processor - Effect to run on each item
 * @returns Effect with all processed results
 *
 * Critical for large graphs to avoid overwhelming the database with concurrent queries.
 * Processes batches sequentially but allows controlled concurrency within each batch.
 */
export const processBatched = <T, R, E>(
  items: T[],
  batchSize: number,
  processor: (item: T) => Effect.Effect<R, E>
): Effect.Effect<R[], E> =>
  Effect.gen(function* () {
    const batches: T[][] = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }

    const results: R[] = [];
    for (const batch of batches) {
      const batchResults = yield* Effect.all(batch.map(processor), {
        concurrency: Math.min(batchSize, 10),
      });
      results.push(...batchResults);
    }

    return results;
  });

/**
 * Executes a Gremlin query with standardized error handling.
 *
 * @param queryFn - Function that returns a Promise from a Gremlin traversal
 * @param errorMessage - Human-readable error message for failures
 * @param queryDescription - Technical description of the query for debugging
 * @returns Effect with query results or typed error
 */
export const executeGremlinQuery = <T>(
  queryFn: () => Promise<T>,
  errorMessage: string,
  queryDescription: string
): Effect.Effect<T, GremlinQueryError> =>
  Effect.tryPromise({
    try: queryFn,
    catch: (error: unknown) => Errors.query(errorMessage, queryDescription, { error }),
  });

/**
 * Gets all vertex labels from the graph.
 *
 * @param g - Gremlin traversal source
 * @returns Effect with array of vertex label strings
 */
export const getVertexLabels = (
  g: GraphTraversalSource
): Effect.Effect<string[], GremlinQueryError> =>
  executeGremlinQuery(
    () => g.V().label().dedup().toList(),
    'Failed to get vertex labels',
    'g.V().label().dedup().toList()'
  ).pipe(Effect.map(labels => labels as string[]));

/**
 * Gets all edge labels from the graph.
 *
 * @param g - Gremlin traversal source
 * @returns Effect with array of edge label strings
 */
export const getEdgeLabels = (
  g: GraphTraversalSource
): Effect.Effect<string[], GremlinQueryError> =>
  executeGremlinQuery(
    () => g.E().label().dedup().toList(),
    'Failed to get edge labels',
    'g.E().label().dedup().toList()'
  ).pipe(Effect.map(labels => labels as string[]));

/**
 * Gets vertex counts grouped by label.
 *
 * @param g - Gremlin traversal source
 * @returns Effect with vertex count data
 */
export const getVertexCountsPerLabel = (g: GraphTraversalSource) =>
  executeGremlinQuery(
    () => g.V().groupCount().by(label()).next(),
    'Failed to get vertex counts',
    'g.V().groupCount().by(label()).next()'
  ).pipe(
    Effect.map(result => {
      const map = (result as any)?.value as Map<string, number> | undefined;
      return { value: map ? (Object.fromEntries(map) as Record<string, number>) : undefined };
    })
  );

/**
 * Gets edge counts grouped by label.
 *
 * @param g - Gremlin traversal source
 * @returns Effect with edge count data
 */
export const getEdgeCountsPerLabel = (g: GraphTraversalSource) =>
  executeGremlinQuery(
    () => g.E().groupCount().by(label()).next(),
    'Failed to get edge counts',
    'g.E().groupCount().by(label()).next()'
  ).pipe(
    Effect.map(result => {
      const map = (result as any)?.value as Map<string, number> | undefined;
      return { value: map ? (Object.fromEntries(map) as Record<string, number>) : undefined };
    })
  );

/**
 * Gets property keys for a specific vertex label.
 *
 * @param g - Gremlin traversal source
 * @param vertexLabel - Label to analyze
 * @param sampleLimit - Maximum number of vertices to sample for properties
 * @returns Effect with array of property key strings
 */
export const getVertexPropertyKeys = (
  g: GraphTraversalSource,
  vertexLabel: string,
  sampleLimit: number = 100
): Effect.Effect<string[], GremlinQueryError> =>
  executeGremlinQuery(
    () => g.V().hasLabel(vertexLabel).limit(sampleLimit).properties().key().dedup().toList(),
    `Failed to get properties for vertex ${vertexLabel}`,
    `g.V().hasLabel('${vertexLabel}').limit(${sampleLimit}).properties().key().dedup().toList()`
  ).pipe(Effect.map(keys => keys as string[]));

/**
 * Gets property keys for a specific edge label.
 *
 * @param g - Gremlin traversal source
 * @param edgeLabel - Label to analyze
 * @param sampleLimit - Maximum number of edges to sample for properties
 * @returns Effect with array of property key strings
 */
export const getEdgePropertyKeys = (
  g: GraphTraversalSource,
  edgeLabel: string,
  sampleLimit: number = 100
): Effect.Effect<string[], GremlinQueryError> =>
  executeGremlinQuery(
    () => g.E().hasLabel(edgeLabel).limit(sampleLimit).properties().key().dedup().toList(),
    `Failed to get properties for edge ${edgeLabel}`,
    `g.E().hasLabel('${edgeLabel}').limit(${sampleLimit}).properties().key().dedup().toList()`
  ).pipe(Effect.map(keys => keys as string[]));

/**
 * Gets sample property values for analysis.
 *
 * @param g - Gremlin traversal source
 * @param elementLabel - Vertex or edge label
 * @param propertyKey - Property to sample
 * @param isVertex - Whether this is a vertex (true) or edge (false)
 * @param sampleLimit - Maximum number of elements to sample
 * @param maxValues - Maximum number of distinct values to collect
 * @returns Effect with array of sample values
 */
export const getSamplePropertyValues = (
  g: GraphTraversalSource,
  elementLabel: string,
  propertyKey: string,
  isVertex: boolean,
  sampleLimit: number = 50,
  maxValues: number = 10
): Effect.Effect<unknown[], GremlinQueryError> => {
  const traversal = isVertex ? g.V().hasLabel(elementLabel) : g.E().hasLabel(elementLabel);

  return executeGremlinQuery(
    () =>
      traversal
        .limit(sampleLimit)
        .values(propertyKey)
        .dedup()
        .limit(maxValues + 1)
        .toList(),
    `Failed to get values for property ${propertyKey}`,
    `g.${isVertex ? 'V' : 'E'}().hasLabel('${elementLabel}').limit(${sampleLimit}).values('${propertyKey}').dedup().limit(${maxValues + 1}).toList()`
  ).pipe(Effect.map(values => values as unknown[]));
};
