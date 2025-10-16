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
 * @fileoverview Property analysis for graph schema generation.
 *
 * Handles analysis of vertex and edge properties including type detection,
 * enum discovery, cardinality analysis, and sample value collection.
 */

import { Effect } from 'effect';
import type { Property } from './models/index.js';
import type { SchemaConfig } from './types.js';
import type { process } from 'gremlin';
import { getSamplePropertyValues, processBatched } from './query-utils.js';
import type { GremlinQueryError } from '../errors.js';

type GraphTraversalSource = process.GraphTraversalSource;

/**
 * Analyzes property characteristics from collected values.
 *
 * @param propertyKey - Name of the property
 * @param values - Array of sample values
 * @param config - Schema configuration
 * @returns Property analysis result
 */
export const analyzePropertyFromValues = (
  propertyKey: string,
  values: unknown[],
  config: SchemaConfig
): Property => {
  // Skip denylisted properties
  if (config.enumPropertyDenyList.includes(propertyKey)) {
    return {
      name: propertyKey,
      type: ['unknown'],
    };
  }

  // Deduplicate and limit values
  const uniqueValues = Array.from(new Set(values)).slice(0, config.maxEnumValues + 1);

  // Determine types from sample values
  const types = Array.from(new Set(uniqueValues.map((val: unknown) => typeof val).filter(Boolean)));

  const property: Property = {
    name: propertyKey,
    type: types.length > 0 ? types : ['unknown'],
  };

  // Add sample values if requested
  if (config.includeSampleValues && uniqueValues.length > 0) {
    property.sample_values = uniqueValues.slice(0, 5);
  }

  // Determine if this should be treated as an enum
  if (uniqueValues.length <= config.enumCardinalityThreshold && uniqueValues.length > 0) {
    property.enum = uniqueValues;
    property.cardinality = 'single';
  }

  return property;
};

/**
 * Analyzes a single property for an element (vertex or edge).
 *
 * @param g - Gremlin traversal source
 * @param elementLabel - Label of the vertex or edge
 * @param propertyKey - Property to analyze
 * @param config - Schema configuration
 * @param isVertex - Whether analyzing vertex (true) or edge (false) property
 * @returns Effect with property analysis
 */
export const analyzeSingleProperty = (
  g: GraphTraversalSource,
  elementLabel: string,
  propertyKey: string,
  config: SchemaConfig,
  isVertex: boolean
): Effect.Effect<Property, GremlinQueryError> =>
  Effect.gen(function* () {
    // Skip denylisted properties early
    if (config.enumPropertyDenyList.includes(propertyKey)) {
      return {
        name: propertyKey,
        type: ['unknown'],
      };
    }

    // Get sample values for analysis
    const sampleValues = yield* getSamplePropertyValues(
      g,
      elementLabel,
      propertyKey,
      isVertex,
      50, // sample limit
      config.maxEnumValues
    );

    return analyzePropertyFromValues(propertyKey, sampleValues, config);
  });

/**
 * Analyzes all properties for a list of elements using batched processing.
 *
 * @param g - Gremlin traversal source
 * @param elementLabels - Array of vertex or edge labels
 * @param propertyKeysFn - Function to get property keys for an element
 * @param config - Schema configuration
 * @param isVertex - Whether analyzing vertices (true) or edges (false)
 * @returns Effect with array of property analyses
 */
export const analyzeElementProperties = (
  g: GraphTraversalSource,
  elementLabels: string[],
  propertyKeysFn: (
    g: GraphTraversalSource,
    label: string
  ) => Effect.Effect<string[], GremlinQueryError>,
  config: SchemaConfig,
  isVertex: boolean
): Effect.Effect<unknown[], GremlinQueryError> =>
  Effect.gen(function* () {
    const batchSize = config.batchSize || 10;

    yield* Effect.logInfo(
      `Analyzing ${elementLabels.length} ${isVertex ? 'vertex' : 'edge'} labels with batch size ${batchSize}`
    );

    return yield* processBatched(elementLabels, batchSize, (elementLabel: string) =>
      analyzePropertiesForLabel(g, elementLabel, propertyKeysFn, config, isVertex)
    );
  });

// TODO: https://github.com/apache/tinkerpop/pull/3238#discussion_r2429664697

/**
 * Analyzes properties for a single element label.
 *
 * @param g - Gremlin traversal source
 * @param elementLabel - Label to analyze
 * @param propertyKeysFn - Function to get property keys
 * @param config - Schema configuration
 * @param isVertex - Whether analyzing vertex or edge
 * @returns Effect with property analysis
 */
const analyzePropertiesForLabel = (
  g: GraphTraversalSource,
  elementLabel: string,
  propertyKeysFn: (
    g: GraphTraversalSource,
    label: string
  ) => Effect.Effect<string[], GremlinQueryError>,
  config: SchemaConfig,
  isVertex: boolean
): Effect.Effect<unknown, GremlinQueryError> =>
  Effect.gen(function* () {
    // Get all property keys for this label
    const propertyKeys = yield* propertyKeysFn(g, elementLabel);

    // Analyze properties with controlled concurrency
    const properties = yield* Effect.all(
      propertyKeys.map(key => analyzeSingleProperty(g, elementLabel, key, config, isVertex)),
      { concurrency: 5 }
    );

    // Return the analysis
    return {
      ['label']: elementLabel,
      properties,
    };
  });

/**
 * Creates a property analyzer with count information.
 *
 * @param counts - Count data for elements
 * @param config - Schema configuration
 * @returns Function that adds count information to analysis
 */
export const withElementCounts =
  <T extends { count?: number }>(
    counts: { value?: Record<string, number> } | null,
    config: SchemaConfig
  ) =>
  (elementLabel: string, analysis: Omit<T, 'count'>): T => {
    const count = config.includeCounts ? counts?.value?.[elementLabel] : undefined;

    return {
      ...analysis,
      ...(count !== undefined && { count }),
    } as T;
  };
