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
 * @fileoverview Gremlin result parsing with type-safe transformations.
 *
 * Handles the complex task of converting raw Gremlin driver results into standardized,
 * serializable objects. Manages various result formats from different Gremlin
 * implementations and normalizes them for MCP consumption.
 */

import { z } from 'zod';
import { GremlinResultItemSchema } from '../gremlin/models/index.js';
import { calculateResultMetadata } from './result-metadata.js';
import { Effect } from 'effect';
import { Errors } from '../errors.js';

/**
 * Type guard for objects with specific constructor names.
 *
 * @param obj - Object to check
 * @param name - Expected constructor name
 * @returns True if object has the specified constructor name
 *
 * Critical for identifying native Gremlin driver types (Vertex, Edge, etc.)
 * which need special handling during result transformation.
 */
function hasConstructorName(
  obj: unknown,
  name: 'Vertex' | 'Edge' | 'Path' | 'Property' | 'VertexProperty'
): boolean {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    'constructor' in obj &&
    typeof obj.constructor === 'function' &&
    obj.constructor.name === name
  );
}

/**
 * Type guard and interface for raw vertex objects
 */
interface RawVertex {
  id: unknown;
  label: string;
  properties?: unknown;
}

function isRawVertex(obj: unknown): obj is RawVertex {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    'id' in obj &&
    'label' in obj &&
    typeof obj.label === 'string'
  );
}

/**
 * Type guard and interface for raw edge objects
 */
interface RawEdge {
  id: unknown;
  label: string;
  inV?: unknown;
  outV?: unknown;
  properties?: unknown;
}

function isRawEdge(obj: unknown): obj is RawEdge {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    'id' in obj &&
    'label' in obj &&
    typeof obj.label === 'string'
  );
}

/**
 * Type guard and interface for raw path objects
 */
interface RawPath {
  labels?: unknown;
  objects?: unknown;
}

function isRawPath(obj: unknown): obj is RawPath {
  return obj !== null && typeof obj === 'object';
}

/**
 * Type guard and interface for raw property objects
 */
interface RawProperty {
  key?: string;
  label?: string;
  value: unknown;
}

function isRawProperty(obj: unknown): obj is RawProperty {
  return obj !== null && typeof obj === 'object' && 'value' in obj;
}

/**
 * Zod schema with preprocessing for raw Gremlin results.
 *
 * @description Transforms native Gremlin driver types into standardized objects before validation.
 *
 * Preprocessing pipeline:
 * 1. Detects and transforms native driver types (Vertex, Edge, Path, Property)
 * 2. Converts ES6 Maps to plain objects
 * 3. Adds type discriminators for schema validation
 * 4. Handles various result formats from different Gremlin implementations
 *
 * Critical for cross-driver compatibility - different Gremlin servers return
 * results in slightly different formats.
 */
const GremlinPreprocessedResultSchema = z.preprocess((arg: unknown) => {
  // Pass through primitives and null/undefined, which Zod can handle directly.
  if (arg === null || typeof arg !== 'object') {
    return arg;
  }

  // Handle native Gremlin structure types by checking their constructor name
  // and transforming them into plain objects with a 'type' discriminator.
  if (hasConstructorName(arg, 'Vertex') && isRawVertex(arg)) {
    return { ...arg, type: 'vertex' };
  }

  if (hasConstructorName(arg, 'Edge') && isRawEdge(arg)) {
    return { ...arg, type: 'edge' };
  }

  if (hasConstructorName(arg, 'Path') && isRawPath(arg)) {
    return {
      labels: Array.isArray(arg.labels) ? arg.labels.map(String) : [],
      objects: arg.objects,
      type: 'path',
    };
  }

  if (
    (hasConstructorName(arg, 'Property') || hasConstructorName(arg, 'VertexProperty')) &&
    isRawProperty(arg)
  ) {
    return {
      key: arg.key || arg.label || '',
      value: arg.value,
      type: 'property',
    };
  }

  // Convert ES6 Maps to plain objects so Zod can parse them.
  if (arg instanceof Map) {
    return Object.fromEntries(arg.entries());
  }

  // If a generic object looks like a vertex or edge (e.g., from a different driver
  // or a simple JSON response), add the 'type' discriminator to help Zod parse it.
  if ('id' in arg && 'label' in arg) {
    if ('properties' in arg && !('inV' in arg) && !('outV' in arg) && isRawVertex(arg)) {
      return { ...arg, type: 'vertex' };
    }
    if ('inV' in arg && 'outV' in arg && isRawEdge(arg)) {
      return { ...arg, type: 'edge' };
    }
  }

  // Let arrays and other plain objects pass through for Zod to handle.
  return arg;
}, GremlinResultItemSchema);

/**
 * Parses a single raw Gremlin result into a typed, serializable object.
 *
 * @param rawResult - Raw result item from Gremlin query
 * @returns Validated and typed result item
 *
 * Uses the preprocessing schema to handle driver-specific transformations.
 */
export const parseGremlinResultItem = (rawResult: unknown) =>
  Effect.try({
    try: () => GremlinPreprocessedResultSchema.parse(rawResult),
    catch: error => Errors.parse('Failed to parse Gremlin result item', rawResult, error),
  });

/**
 * Parses an array of raw Gremlin results into typed objects.
 */
export const parseGremlinResults = (rawResults: unknown[]) =>
  Effect.all(rawResults.map(parseGremlinResultItem));

/**
 * Enhanced result parser with comprehensive metadata generation.
 *
 * @param rawResults - Array of raw Gremlin query results
 * @returns Parsed results with detailed type and structure metadata
 *
 * Provides additional context about result composition, useful for
 * understanding query output patterns and debugging.
 */
export const parseGremlinResultsWithMetadata = (rawResults: unknown[]) =>
  Effect.gen(function* () {
    const results = yield* parseGremlinResults(rawResults);
    const metadata = calculateResultMetadata(results);
    return { results, metadata };
  });
