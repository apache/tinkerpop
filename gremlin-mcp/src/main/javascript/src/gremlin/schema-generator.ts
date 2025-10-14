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
 * @fileoverview Streamlined graph schema generation orchestrator.
 *
 * Coordinates schema generation using modular components for query execution,
 * property analysis, relationship patterns, and schema assembly. Provides
 * timeout protection and comprehensive error handling.
 */

import { Effect, Duration } from 'effect';
import { type GraphSchema, type Vertex, type Edge } from './models/index.js';
import { Errors, type GremlinConnectionError, type GremlinQueryError } from '../errors.js';
import type { ConnectionState, SchemaConfig } from './types.js';
import {
  getVertexLabels,
  getEdgeLabels,
  getVertexCounts,
  getEdgeCounts,
  getVertexPropertyKeys,
  getEdgePropertyKeys,
} from './query-utils.js';
import { analyzeElementProperties, withElementCounts } from './property-analyzer.js';
import { generateEdgePatterns } from './edge-patterns.js';
import { assembleGraphSchema } from './schema-assembly.js';
import type { process } from 'gremlin';

type GraphTraversalSource = process.GraphTraversalSource;
type SchemaCountData = { value?: Record<string, number> } | null;

const DEFAULT_SCHEMA_TIMEOUT_MS = 30000;

/**
 * Default schema configuration
 */
export const DEFAULT_SCHEMA_CONFIG: SchemaConfig = {
  includeSampleValues: false,
  maxEnumValues: 10,
  includeCounts: true,
  enumCardinalityThreshold: 10,
  enumPropertyDenyList: ['id', 'timestamp'],
  timeoutMs: DEFAULT_SCHEMA_TIMEOUT_MS,
  batchSize: 10,
};

/**
 * Core schema generation orchestrator.
 *
 * @param g - Gremlin traversal source
 * @param config - Schema configuration
 * @param startTime - Generation start timestamp for metrics
 * @returns Effect with complete schema
 */
const executeSchemaGeneration = (
  g: GraphTraversalSource,
  config: SchemaConfig,
  startTime: number
): Effect.Effect<GraphSchema, GremlinQueryError> =>
  Effect.gen(function* () {
    // Step 1: Discover graph structure
    yield* Effect.logInfo('Discovering graph structure');
    const [vertexLabels, edgeLabels] = yield* Effect.all([getVertexLabels(g), getEdgeLabels(g)]);

    yield* Effect.logInfo(
      `Found ${vertexLabels.length} vertex labels and ${edgeLabels.length} edge labels`
    );

    // Step 2: Get counts if enabled
    const [vertexCounts, edgeCounts] = yield* getElementCounts(g, config);

    // Step 3: Analyze properties and patterns in parallel
    yield* Effect.logInfo('Analyzing properties and relationship patterns');
    const [rawVertices, rawEdges, patterns] = yield* Effect.all(
      [
        analyzeElementProperties(g, vertexLabels, getVertexPropertyKeys, config, true),
        analyzeElementProperties(g, edgeLabels, getEdgePropertyKeys, config, false),
        generateEdgePatterns(g),
      ],
      { concurrency: 3 }
    );

    // Step 4: Add count information
    const vertices = addElementCounts<Vertex>(rawVertices, vertexCounts, config);
    const edges = addElementCounts<Edge>(rawEdges, edgeCounts, config);

    // Step 5: Assemble final schema
    return yield* assembleGraphSchema(vertices, edges, patterns, config, startTime);
  });

/**
 * Gets vertex and edge counts if enabled in configuration.
 */
const getElementCounts = (
  g: GraphTraversalSource,
  config: SchemaConfig
): Effect.Effect<[SchemaCountData, SchemaCountData], GremlinQueryError> => {
  if (!config.includeCounts) {
    return Effect.succeed([null, null]);
  }

  return Effect.all([getVertexCounts(g), getEdgeCounts(g)]);
};

/**
 * Adds count information to analysis results.
 */
const addElementCounts = <T extends Vertex | Edge>(
  rawElements: unknown[],
  counts: SchemaCountData,
  config: SchemaConfig
): T[] => {
  const addCounts = withElementCounts<T>(counts, config);

  return rawElements.map(element => {
    const elementData = element as Record<string, unknown>;
    const label = elementData['label'] as string;
    return addCounts(label, elementData as Omit<T, 'count'>);
  });
};

/**
 * Applies timeout protection to schema generation.
 *
 * @param schemaGeneration - Effect performing schema generation
 * @param config - Configuration with timeout settings
 * @returns Effect with timeout protection applied
 */
const applySchemaTimeout = (
  schemaGeneration: Effect.Effect<GraphSchema, GremlinQueryError>,
  config: SchemaConfig
): Effect.Effect<GraphSchema, GremlinQueryError> => {
  const timeoutDuration = Duration.millis(config.timeoutMs || DEFAULT_SCHEMA_TIMEOUT_MS);
  const timeoutEffect = Effect.timeout(schemaGeneration, timeoutDuration);

  return Effect.catchTag(timeoutEffect, 'TimeoutException', () =>
    Effect.fail(
      Errors.query(
        `Schema generation timed out after ${config.timeoutMs || DEFAULT_SCHEMA_TIMEOUT_MS}ms`,
        'schema-generation'
      )
    )
  );
};

/**
 * Main entry point for graph schema generation.
 *
 * @param connectionState - Active Gremlin connection with traversal source
 * @param config - Schema generation configuration options
 * @returns Effect with complete GraphSchema or connection error
 *
 * Orchestrates the complete schema generation pipeline:
 * 1. Validates connection state
 * 2. Applies timeout protection
 * 3. Delegates to core generation logic
 *
 * Will timeout if generation exceeds configured time limit (default 30s).
 */
export const generateGraphSchema = (
  connectionState: ConnectionState,
  config: SchemaConfig = DEFAULT_SCHEMA_CONFIG
): Effect.Effect<GraphSchema, GremlinConnectionError | GremlinQueryError> =>
  Effect.gen(function* () {
    if (!connectionState.g) {
      return yield* Effect.fail(Errors.connection('Graph traversal source not available'));
    }

    const g = connectionState.g;
    const startTime = Date.now();

    yield* Effect.logInfo('Starting graph schema generation', {
      config: {
        includeCounts: config.includeCounts,
        includeSampleValues: config.includeSampleValues,
        maxEnumValues: config.maxEnumValues,
        timeoutMs: config.timeoutMs,
        batchSize: config.batchSize,
      },
    });

    const schemaGeneration = executeSchemaGeneration(g, config, startTime);
    return yield* applySchemaTimeout(schemaGeneration, config);
  });
