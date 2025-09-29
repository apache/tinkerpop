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
 * @fileoverview Relationship pattern analysis for graph schemas.
 *
 * Analyzes edge connectivity patterns between vertex types to generate
 * relationship pattern metadata for schema documentation.
 */

import { Effect } from 'effect';
import gremlin from 'gremlin';
import type { RelationshipPattern } from './models/index.js';
import { executeGremlinQuery } from './query-utils.js';
import type { GremlinQueryError } from '../errors.js';
import type { process } from 'gremlin';

type GraphTraversalSource = process.GraphTraversalSource;

const { inV, outV, label } = gremlin.process.statics;

/**
 * Raw pattern data from Gremlin project() query.
 */
interface RawPatternData {
  from: unknown;
  to: unknown;
  label: unknown;
}

/**
 * Generates relationship patterns by analyzing edge connectivity.
 *
 * @param g - Gremlin traversal source
 * @param maxPatterns - Maximum number of patterns to retrieve (default: 1000)
 * @returns Effect with array of relationship patterns
 */
export const generateRelationshipPatterns = (
  g: GraphTraversalSource,
  maxPatterns: number = 1000
): Effect.Effect<RelationshipPattern[], GremlinQueryError> =>
  Effect.gen(function* () {
    yield* Effect.logInfo('Generating relationship patterns from edge connectivity');

    // Get all patterns in a single optimized query
    const allPatterns = yield* executeGremlinQuery(
      () =>
        g
          .E()
          .project('from', 'to', 'label')
          .by(outV().label())
          .by(inV().label())
          .by(label())
          .dedup()
          .limit(maxPatterns)
          .toList(),
      'Failed to get relationship patterns',
      `g.E().project('from', 'to', 'label').by(outV().label()).by(inV().label()).by(label()).dedup().limit(${maxPatterns}).toList()`
    );

    yield* Effect.logInfo(
      `Retrieved ${(allPatterns as unknown[]).length} raw patterns from database`
    );

    // Process the results into structured patterns
    const processedPatterns = yield* processRawPatterns(allPatterns as unknown[]);

    yield* Effect.logInfo(`Processed ${processedPatterns.length} valid relationship patterns`);

    return processedPatterns;
  });

/**
 * Processes raw Gremlin pattern results into structured relationship patterns.
 *
 * @param rawPatterns - Raw pattern data from Gremlin query
 * @returns Effect with processed relationship patterns
 */
const processRawPatterns = (
  rawPatterns: unknown[]
): Effect.Effect<RelationshipPattern[], never> => {
  // Extract pattern data handling both Map and object formats
  const extractedPatterns = rawPatterns.map(extractPatternData);

  // Filter out invalid patterns and convert to final format
  const validPatterns = extractedPatterns.filter(isValidPattern).map(convertToRelationshipPattern);

  return Effect.succeed(validPatterns);
};

/**
 * Extracts pattern data from various Gremlin result formats.
 *
 * @param item - Raw pattern item from Gremlin
 * @returns Extracted pattern data
 */
const extractPatternData = (item: unknown): RawPatternData => {
  // Handle Map format (common in some Gremlin implementations)
  if (item instanceof Map) {
    return {
      from: item.get('from'),
      to: item.get('to'),
      label: item.get('label'),
    };
  }

  // Handle plain object format
  if (item && typeof item === 'object') {
    const obj = item as Record<string, unknown>;
    return {
      from: obj['from'],
      to: obj['to'],
      label: obj['label'],
    };
  }

  // Fallback for unexpected formats
  return { from: null, to: null, label: null };
};

/**
 * Validates that pattern data contains required string values.
 *
 * @param pattern - Pattern data to validate
 * @returns True if pattern is valid
 */
const isValidPattern = (pattern: RawPatternData): pattern is Required<RawPatternData> =>
  pattern.from !== null &&
  pattern.to !== null &&
  pattern.label !== null &&
  typeof pattern.from === 'string' &&
  typeof pattern.to === 'string' &&
  typeof pattern.label === 'string';

/**
 * Converts validated pattern data to final RelationshipPattern format.
 *
 * @param pattern - Validated pattern data
 * @returns Relationship pattern object
 */
const convertToRelationshipPattern = (pattern: Required<RawPatternData>): RelationshipPattern => ({
  left_vertex: pattern.from as string,
  right_vertex: pattern.to as string,
  relation: pattern.label as string,
});

/**
 * Analyzes relationship density and provides pattern statistics.
 *
 * @param patterns - Array of relationship patterns
 * @returns Pattern analysis statistics
 */
export const analyzePatternStatistics = (patterns: RelationshipPattern[]) => {
  const vertexTypes = new Set<string>();
  const edgeTypes = new Set<string>();
  const connections = new Map<string, number>();

  patterns.forEach(pattern => {
    vertexTypes.add(pattern.left_vertex);
    vertexTypes.add(pattern.right_vertex);
    edgeTypes.add(pattern.relation);

    const connectionKey = `${pattern.left_vertex}->${pattern.right_vertex}`;
    connections.set(connectionKey, (connections.get(connectionKey) || 0) + 1);
  });

  return {
    totalPatterns: patterns.length,
    uniqueVertexTypes: vertexTypes.size,
    uniqueEdgeTypes: edgeTypes.size,
    averageConnectionsPerVertexType: patterns.length / Math.max(vertexTypes.size, 1),
    vertexTypes: Array.from(vertexTypes).sort(),
    edgeTypes: Array.from(edgeTypes).sort(),
    connectionFrequencies: Object.fromEntries(connections),
  };
};
