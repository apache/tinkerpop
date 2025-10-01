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
 * Utilities for calculating metadata from Gremlin query results.
 */

import { Effect } from 'effect';
import type { GremlinResultItem } from '../gremlin/models/index.js';

export interface ResultMetadata {
  totalCount: number;
  vertexCount: number;
  edgeCount: number;
  pathCount: number;
  propertyCount: number;
  propertyMapCount: number;
  primitiveCount: number;
  types: string[];
}

/**
 * Calculate comprehensive metadata from parsed Gremlin results.
 */
export const calculateResultMetadata = (
  results: GremlinResultItem[]
): Effect.Effect<ResultMetadata, never> =>
  Effect.sync(() => {
    let vertexCount = 0;
    let edgeCount = 0;
    let pathCount = 0;
    let propertyCount = 0;
    let propertyMapCount = 0;
    let primitiveCount = 0;
    const types = new Set<string>();

    results.forEach(result => {
      if (result && typeof result === 'object') {
        if ('type' in result) {
          if (result['type'] === 'vertex') {
            vertexCount++;
            types.add('vertex');
          } else if (result['type'] === 'edge') {
            edgeCount++;
            types.add('edge');
          } else if (result['type'] === 'path') {
            pathCount++;
            types.add('path');
          } else if (result['type'] === 'property') {
            propertyCount++;
            types.add('property');
          }
        } else if (Array.isArray(result)) {
          types.add('array');
        } else {
          // Likely a property map or generic object
          propertyMapCount++;
          types.add('property_map');
        }
      } else {
        primitiveCount++;
        types.add(typeof result);
      }
    });

    return {
      totalCount: results.length,
      vertexCount,
      edgeCount,
      pathCount,
      propertyCount,
      propertyMapCount,
      primitiveCount,
      types: Array.from(types),
    };
  });
