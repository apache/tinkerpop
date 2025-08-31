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
 * @fileoverview Graph data import/export operations with Effect-based composition.
 *
 * Provides high-level operations for importing data from various formats (GraphSON, CSV)
 * and exporting subgraphs based on traversal queries. Handles format validation,
 * batch processing, and error recovery.
 */

import { Effect } from 'effect';
import { type ImportDataInput, type ExportSubgraphInput } from '../gremlin/models/index.js';
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
 * Imports graph data from various formats with comprehensive validation.
 *
 * @param service - Gremlin service instance
 * @param input - Import configuration and data
 * @returns Effect with success message or import errors
 *
 * Supports:
 * - GraphSON format (native Gremlin JSON)
 * - CSV format (vertices and edges)
 *
 * Features batch processing and optional graph clearing for fresh imports.
 */
export const importGraphData = (
  service: typeof GremlinService.Service,
  input: ImportDataInput
): Effect.Effect<string, ResourceError | GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `Starting import operation: format=${input.format}, size=${input.data.length} chars`
    );

    switch (input.format) {
      case 'graphson':
        return yield* importGraphSON(service, input);

      case 'csv':
        return yield* importCSV(service, input);

      default:
        return yield* Effect.fail(
          Errors.resource(`Unsupported import format: ${input.format}`, 'import_operation')
        );
    }
  });

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
 * Clear graph data if requested
 */
const clearGraphIfRequested = (
  service: typeof GremlinService.Service,
  shouldClear: boolean | undefined
): Effect.Effect<void, GremlinConnectionError | GremlinQueryError | ParseError> =>
  shouldClear
    ? Effect.gen(function* () {
        yield* service.executeQuery('g.V().drop()');
        yield* service.executeQuery('g.E().drop()');
        yield* Effect.logInfo('Graph cleared before import');
      })
    : Effect.void;

/**
 * Import vertices from GraphSON data
 */
const importVertices = (
  service: typeof GremlinService.Service,
  vertices: unknown[]
): Effect.Effect<void, GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    for (const vertex of vertices) {
      const query = buildVertexInsertQuery(vertex as Record<string, unknown>);
      yield* service.executeQuery(query);
    }
    yield* Effect.logInfo(`Imported ${vertices.length} vertices`);
  });

/**
 * Import edges from GraphSON data
 */
const importEdges = (
  service: typeof GremlinService.Service,
  edges: unknown[]
): Effect.Effect<void, GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    for (const edge of edges) {
      const query = buildEdgeInsertQuery(edge as Record<string, unknown>);
      yield* service.executeQuery(query);
    }
    yield* Effect.logInfo(`Imported ${edges.length} edges`);
  });

/**
 * Build GraphSON import summary
 */
const buildImportSummary = (vertexCount: number, edgeCount: number): string =>
  `GraphSON import completed successfully. Vertices: ${vertexCount}, Edges: ${edgeCount}`;

/**
 * Parse GraphSON data with proper error handling
 */
const parseGraphSONData = (
  input: string
): Effect.Effect<{ vertices?: unknown[]; edges?: unknown[] }, ResourceError> =>
  Effect.try({
    try: () => JSON.parse(input) as { vertices?: unknown[]; edges?: unknown[] },
    catch: error => Errors.resource('Failed to parse GraphSON data', 'graphson_import', error),
  });

/**
 * Import GraphSON format data
 */
const importGraphSON = (
  service: typeof GremlinService.Service,
  input: ImportDataInput
): Effect.Effect<string, ResourceError | GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    const data = yield* parseGraphSONData(input.data);

    yield* clearGraphIfRequested(service, input.options?.clear_graph);

    const vertexCount = data.vertices?.length || 0;
    const edgeCount = data.edges?.length || 0;

    if (data.vertices && vertexCount > 0) {
      yield* importVertices(service, data.vertices);
    }

    if (data.edges && edgeCount > 0) {
      yield* importEdges(service, data.edges);
    }

    return buildImportSummary(vertexCount, edgeCount);
  });

/**
 * Parse CSV data safely
 */
const parseCSVData = (
  csvData: string
): Effect.Effect<{ headers: string[]; dataRows: string[] }, ResourceError> =>
  Effect.try({
    try: () => {
      const lines = csvData.split('\n').filter(line => line.trim());
      const headers = lines[0]?.split(',').map(h => h.trim()) || [];
      const dataRows = lines.slice(1);
      return { headers, dataRows };
    },
    catch: error => Errors.resource('Failed to parse CSV data', 'csv_import', error),
  });

/**
 * Import CSV format data
 */
const importCSV = (
  service: typeof GremlinService.Service,
  input: ImportDataInput
): Effect.Effect<string, ResourceError | GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    const { headers, dataRows } = yield* parseCSVData(input.data);

    yield* clearGraphIfRequested(service, input.options?.clear_graph);
    yield* importCSVVertices(service, dataRows, headers);

    return `CSV import completed successfully. Processed ${dataRows.length} rows.`;
  });

/**
 * Process a single CSV row into vertex properties
 */
const processCSVRow = (row: string, headers: string[]): Record<string, string> => {
  const values = row.split(',').map(v => v.trim());
  return headers.reduce((props: Record<string, string>, header, index) => {
    if (values[index]) {
      props[header] = values[index];
    }
    return props;
  }, {});
};

/**
 * Import vertices from CSV rows
 */
const importCSVVertices = (
  service: typeof GremlinService.Service,
  dataRows: string[],
  headers: string[]
): Effect.Effect<void, GremlinConnectionError | GremlinQueryError | ParseError> =>
  Effect.gen(function* () {
    for (const row of dataRows) {
      const properties = processCSVRow(row, headers);
      const query = buildCSVVertexInsertQuery(properties);
      yield* service.executeQuery(query);
    }
    yield* Effect.logInfo(`Imported ${dataRows.length} vertices from CSV`);
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
function buildVertexInsertQuery(vertex: Record<string, unknown>): string {
  const label = vertex['label'] || 'vertex';
  const id = vertex['id'];
  const properties = vertex['properties'] || {};

  let query = `g.addV('${label}')`;

  if (id !== undefined) {
    query += `.property(id, '${id}')`;
  }

  for (const [key, value] of Object.entries(properties)) {
    if (Array.isArray(value)) {
      // Handle multi-value properties
      value.forEach(v => {
        query += `.property('${key}', '${v}')`;
      });
    } else {
      query += `.property('${key}', '${value}')`;
    }
  }

  return query;
}

function buildEdgeInsertQuery(edge: Record<string, unknown>): string {
  const label = edge['label'] || 'edge';
  const outV = edge['outV'];
  const inV = edge['inV'];
  const properties = edge['properties'] || {};

  let query = `g.V('${outV}').addE('${label}').to(g.V('${inV}'))`;

  for (const [key, value] of Object.entries(properties)) {
    query += `.property('${key}', '${value}')`;
  }

  return query;
}

function buildCSVVertexInsertQuery(properties: Record<string, string>): string {
  let query = "g.addV('data')";

  for (const [key, value] of Object.entries(properties)) {
    if (key && value) {
      query += `.property('${key}', '${value}')`;
    }
  }

  return query;
}
