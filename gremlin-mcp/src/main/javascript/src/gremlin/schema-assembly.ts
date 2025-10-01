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
 * @fileoverview Schema assembly and validation utilities.
 *
 * Handles the final assembly of graph schema data from analyzed components,
 * validation against schema definitions, and metadata generation.
 */

import { Effect } from 'effect';
import {
  GraphSchemaSchema,
  type GraphSchema,
  type Vertex,
  type Edge,
  type EdgePattern,
} from './models/index.js';
import { Errors, type GremlinQueryError } from '../errors.js';
import type { SchemaConfig } from './types.js';

/**
 * Schema metadata for tracking generation performance and settings.
 */
export interface SchemaMetadata {
  generated_at: string;
  generation_time_ms: number;
  vertex_count: number;
  edge_count: number;
  pattern_count: number;
  optimization_settings: {
    sample_values_included: boolean;
    max_enum_values: number;
    counts_included: boolean;
    enum_cardinality_threshold: number;
    timeout_ms: number;
    batch_size: number;
  };
}

/**
 * Assembles and validates the final graph schema from analyzed components.
 *
 * @param vertices - Analyzed vertex types
 * @param edges - Analyzed edges
 * @param patterns - Edge patterns
 * @param config - Schema generation configuration
 * @param startTime - Generation start timestamp for performance metrics
 * @returns Effect with validated graph schema
 */
export const assembleGraphSchema = (
  vertices: Vertex[],
  edges: Edge[],
  patterns: EdgePattern[],
  config: SchemaConfig,
  startTime: number
): Effect.Effect<GraphSchema, GremlinQueryError> =>
  Effect.gen(function* () {
    // Create metadata
    const metadata = createSchemaMetadata(vertices, edges, patterns, config, startTime);

    // Assemble schema data
    const schemaData = {
      vertices,
      edges,
      edge_patterns: patterns,
      metadata,
    };

    yield* Effect.logDebug('Schema assembly completed', {
      vertexCount: vertices.length,
      edgeCount: edges.length,
      patternCount: patterns.length,
      generationTimeMs: metadata.generation_time_ms,
    });

    // Validate against schema
    return yield* validateSchemaData(schemaData);
  });

/**
 * Creates schema metadata with generation statistics and configuration.
 *
 * @param vertices - Analyzed vertices
 * @param relationships - Analyzed relationships
 * @param patterns - Relationship patterns
 * @param config - Configuration used for generation
 * @param startTime - Start timestamp
 * @returns Schema metadata object
 */
const createSchemaMetadata = (
  vertices: Vertex[],
  edges: Edge[],
  patterns: EdgePattern[],
  config: SchemaConfig,
  startTime: number
): SchemaMetadata => ({
  generated_at: new Date().toISOString(),
  generation_time_ms: Date.now() - startTime,
  vertex_count: vertices.length,
  edge_count: edges.length,
  pattern_count: patterns.length,
  optimization_settings: {
    sample_values_included: config.includeSampleValues,
    max_enum_values: config.maxEnumValues,
    counts_included: config.includeCounts,
    enum_cardinality_threshold: config.enumCardinalityThreshold,
    timeout_ms: config.timeoutMs || 30000,
    batch_size: config.batchSize || 10,
  },
});

/**
 * Validates schema data against the GraphSchema specification.
 *
 * @param schemaData - Assembled schema data
 * @returns Effect with validated GraphSchema or error
 */
const validateSchemaData = (schemaData: unknown): Effect.Effect<GraphSchema, GremlinQueryError> =>
  Effect.try({
    try: () => GraphSchemaSchema.parse(schemaData),
    catch: (error: unknown) => {
      // Use Effect logging instead of console.error for better observability
      Effect.runSync(
        Effect.logError('Schema validation failed', {
          error: error instanceof Error ? error.message : String(error),
          schemaData: safeStringify(schemaData),
        })
      );

      return Errors.query('Schema validation failed', 'schema-validation', { error });
    },
  });

/**
 * Safely stringifies objects for logging, handling circular references.
 *
 * @param obj - Object to stringify
 * @returns JSON string or error representation
 */
const safeStringify = (obj: unknown): string => {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return `[Object: ${typeof obj}]`;
  }
};

/**
 * Validates vertex data consistency and completeness.
 *
 * @param vertices - Array of vertices to validate
 * @returns Effect with validation result
 */
export const validateVertices = (vertices: Vertex[]): Effect.Effect<void, GremlinQueryError> =>
  Effect.gen(function* () {
    const issues: string[] = [];

    vertices.forEach((vertex, index) => {
      if (!vertex.labels || typeof vertex.labels !== 'string') {
        issues.push(`Vertex ${index}: Invalid or missing labels`);
      }

      if (!Array.isArray(vertex.properties)) {
        issues.push(`Vertex ${index}: Properties must be an array`);
      }

      vertex.properties?.forEach((prop, propIndex) => {
        if (!prop.name || typeof prop.name !== 'string') {
          issues.push(`Vertex ${index}, Property ${propIndex}: Invalid or missing name`);
        }
        if (!Array.isArray(prop.type)) {
          issues.push(`Vertex ${index}, Property ${propIndex}: Type must be an array`);
        }
      });
    });

    if (issues.length > 0) {
      return yield* Effect.fail(
        Errors.query('Vertex validation failed', 'vertex-validation', { issues })
      );
    }
  });

/**
 * Validates edge data consistency and completeness.
 *
 * @param edges - Array of edges to validate
 * @returns Effect with validation result
 */
export const validateEdges = (edges: Edge[]): Effect.Effect<void, GremlinQueryError> =>
  Effect.gen(function* () {
    const issues: string[] = [];

    edges.forEach((edge, index) => {
      if (!edge.type || typeof edge.type !== 'string') {
        issues.push(`Edge ${index}: Invalid or missing type`);
      }

      if (!Array.isArray(edge.properties)) {
        issues.push(`Edge ${index}: Properties must be an array`);
      }

      edge.properties?.forEach((prop, propIndex) => {
        if (!prop.name || typeof prop.name !== 'string') {
          issues.push(`Edge ${index}, Property ${propIndex}: Invalid or missing name`);
        }
        if (!Array.isArray(prop.type)) {
          issues.push(`Edge ${index}, Property ${propIndex}: Type must be an array`);
        }
      });
    });

    if (issues.length > 0) {
      return yield* Effect.fail(
        Errors.query('Edge validation failed', 'edge-validation', { issues })
      );
    }
  });

/**
 * Validates edge patterns for consistency.
 *
 * @param patterns - Array of edge patterns to validate
 * @returns Effect with validation result
 */
export const validateEdgePatterns = (
  patterns: EdgePattern[]
): Effect.Effect<void, GremlinQueryError> =>
  Effect.gen(function* () {
    const issues: string[] = [];

    patterns.forEach((pattern, index) => {
      if (!pattern.left_vertex || typeof pattern.left_vertex !== 'string') {
        issues.push(`Pattern ${index}: Invalid or missing left_vertex`);
      }
      if (!pattern.right_vertex || typeof pattern.right_vertex !== 'string') {
        issues.push(`Pattern ${index}: Invalid or missing right_vertex`);
      }
      if (!pattern.relation || typeof pattern.relation !== 'string') {
        issues.push(`Pattern ${index}: Invalid or missing relation`);
      }
    });

    if (issues.length > 0) {
      return yield* Effect.fail(
        Errors.query('Edge pattern validation failed', 'pattern-validation', { issues })
      );
    }
  });

/**
 * Performs comprehensive validation of all schema components.
 *
 * @param vertices - Vertices to validate
 * @param relationships - Relationships to validate
 * @param patterns - Patterns to validate
 * @returns Effect with validation result
 */
export const validateAllComponents = (
  vertices: Vertex[],
  edges: Edge[],
  patterns: EdgePattern[]
): Effect.Effect<void, GremlinQueryError> =>
  Effect.gen(function* () {
    yield* validateVertices(vertices);
    yield* validateEdges(edges);
    yield* validateEdgePatterns(patterns);

    yield* Effect.logInfo('All schema components validated successfully');
  });
