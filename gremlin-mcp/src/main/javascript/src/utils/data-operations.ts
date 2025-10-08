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
 * @fileoverview Graph data export operations with Effect-based composition.
 *
 * Provides high-level operations for exporting subgraphs based on traversal queries. Handles
 * format validation and error recovery.
 */

import { Effect } from 'effect';
import { type ExportSubgraphInput } from '../gremlin/models/index.js';
import { GremlinService } from '../gremlin/service.js';
import {
  Errors,
  ResourceError,
  GremlinConnectionError,
  GremlinQueryError,
  ParseError,
} from '../errors.js';

/**
 * Type guard to check if a value is a record object
 */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Exports subgraph data based on traversal queries.
 *
 * @param service - Gremlin service instance
 * @param input - Export configuration and traversal query
 * @returns Effect with exported data in requested format
 *
 * Supports multiple output formats:
 * - GraphSON: Native Gremlin JSON format
 * - JSON: Simplified JSON structure
 * - CSV: Tabular format for vertices and edges
 *
 * Can filter properties and limit traversal depth.
 */
export const exportSubgraph = (
  service: typeof GremlinService.Service,
  input: ExportSubgraphInput
): Effect.Effect<string, ResourceError | GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `Starting export operation: format=${input.format}, query=${input.traversal_query}`
    );

    // Execute the traversal query to get the subgraph data
    const queryResult = yield* service.executeQuery(input.traversal_query);

    switch (input.format) {
      case 'graphson':
        return yield* exportToGraphSON(queryResult.results);

      case 'json':
        return yield* exportToJSON(queryResult.results, input);

      case 'csv':
        return yield* exportToCSV(queryResult.results);

      default:
        return yield* Effect.fail(
          Errors.resource(`Unsupported export format: ${input.format}`, 'export_operation')
        );
    }
  });

/**
 * Export to GraphSON format
 */
const exportToGraphSON = (results: unknown[]): Effect.Effect<string, ResourceError> =>
  Effect.gen(function* () {
    const graphsonData = {
      vertices: results.filter(
        r => typeof r === 'object' && r !== null && 'type' in r && r.type === 'vertex'
      ),
      edges: results.filter(
        r => typeof r === 'object' && r !== null && 'type' in r && r.type === 'edge'
      ),
    };

    return yield* Effect.try({
      try: () => JSON.stringify(graphsonData, null, 2),
      catch: error =>
        Errors.resource('Failed to serialize GraphSON data', 'graphson_export', error),
    });
  });

/**
 * Apply property filters to results
 */
const applyPropertyFilters = (result: unknown, input: ExportSubgraphInput): unknown => {
  if (typeof result !== 'object' || result === null || !isRecord(result)) {
    return result;
  }

  if (input.include_properties) {
    const filtered: Record<string, unknown> = {};
    input.include_properties.forEach(prop => {
      if (prop in result) {
        filtered[prop] = result[prop];
      }
    });
    return filtered;
  }

  if (input.exclude_properties) {
    const filtered = { ...result };
    input.exclude_properties.forEach(prop => {
      delete filtered[prop];
    });
    return filtered;
  }

  return result;
};

/**
 * Export to JSON format
 */
const exportToJSON = (
  results: unknown[],
  input: ExportSubgraphInput
): Effect.Effect<string, ResourceError> =>
  Effect.gen(function* () {
    const filteredResults =
      input.include_properties || input.exclude_properties
        ? results.map(result => applyPropertyFilters(result, input))
        : results;

    return yield* Effect.try({
      try: () => JSON.stringify(filteredResults, null, 2),
      catch: error => Errors.resource('Failed to serialize JSON data', 'json_export', error),
    });
  });

/**
 * Export to CSV format
 */
const exportToCSV = (results: unknown[]): Effect.Effect<string, ResourceError> =>
  Effect.gen(function* () {
    if (results.length === 0) {
      return '';
    }

    return yield* Effect.try({
      try: () => {
        // Extract all unique property keys
        const allKeys = new Set<string>();
        results.forEach(result => {
          if (typeof result === 'object' && result !== null) {
            Object.keys(result).forEach(key => allKeys.add(key));
          }
        });

        const headers = Array.from(allKeys);
        const csvLines = [headers.join(',')];

        results.forEach(result => {
          if (isRecord(result)) {
            const row = headers.map(header => {
              const value = result[header];
              return value !== undefined ? String(value) : '';
            });
            csvLines.push(row.join(','));
          }
        });

        return csvLines.join('\n');
      },
      catch: error => Errors.resource('Failed to process CSV data', 'csv_export', error),
    });
  });

/**
 * Helper functions for query building
 */
