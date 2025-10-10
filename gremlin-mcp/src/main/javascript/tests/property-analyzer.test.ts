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
 * @fileoverview Tests for the property analyzer module.
 *
 * Tests property analysis functions including type detection, enum discovery,
 * and batched property processing for vertex and edge elements.
 */

import { Effect } from 'effect';
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import {
  analyzePropertyFromValues,
  analyzeSingleProperty,
  analyzeElementProperties,
} from '../src/gremlin/property-analyzer';
import { Errors } from '../src/errors';

// Mock Gremlin traversal source and query utilities
jest.mock('../src/gremlin/query-utils', () => ({
  processBatched: jest.fn(),
  getSamplePropertyValues: jest.fn(),
}));

import { processBatched, getSamplePropertyValues } from '../src/gremlin/query-utils';

const mockProcessBatched = processBatched as jest.MockedFunction<typeof processBatched>;
const mockGetSamplePropertyValues = getSamplePropertyValues as jest.MockedFunction<
  typeof getSamplePropertyValues
>;

const mockConfig = {
  enumPropertyDenyList: [],
  maxEnumValues: 10,
  includeSampleValues: false,
  includeCounts: false,
  enumCardinalityThreshold: 5,
  batchSize: 5,
};

describe('property-analyzer', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('analyzePropertyFromValues', () => {
    it('should detect types and create enums correctly', () => {
      const stringValues = ['active', 'inactive', 'pending'];
      const result = analyzePropertyFromValues('status', stringValues, mockConfig);

      expect(result.type).toEqual(['string']);
      expect(result.enum).toEqual(['active', 'inactive', 'pending']);
    });

    it('should handle mixed types', () => {
      const mixedValues = ['hello', 42, true];
      const result = analyzePropertyFromValues('mixed', mixedValues, mockConfig);

      expect(result.type).toContain('string');
      expect(result.type).toContain('number');
      expect(result.type).toContain('boolean');
    });

    it('should respect enum cardinality threshold', () => {
      const manyValues = Array.from({ length: 15 }, (_, i) => `value${i}`);
      const result = analyzePropertyFromValues('description', manyValues, mockConfig);

      expect(result.enum).toBeUndefined();
    });

    it('should handle denylisted properties', () => {
      const denyListConfig = { ...mockConfig, enumPropertyDenyList: ['id'] };
      const result = analyzePropertyFromValues('id', ['a', 'b'], denyListConfig);

      expect(result.type).toEqual(['unknown']);
    });
  });

  describe('analyzeSingleProperty', () => {
    const mockTraversalSource = {} as any;

    it('should analyze property successfully', async () => {
      const mockValues = ['John', 'Jane', 'Bob'];
      mockGetSamplePropertyValues.mockReturnValue(Effect.succeed(mockValues));

      const result = await Effect.runPromise(
        analyzeSingleProperty(mockTraversalSource, 'person', 'name', mockConfig, true)
      );

      expect(result.name).toBe('name');
      expect(result.type).toEqual(['string']);
    });

    it('should handle query failures', async () => {
      const error = Errors.query('Connection failed', 'test query', new Error('Connection failed'));
      mockGetSamplePropertyValues.mockReturnValue(Effect.fail(error));

      const result = await Effect.runPromiseExit(
        analyzeSingleProperty(mockTraversalSource, 'person', 'name', mockConfig, true)
      );

      expect(result._tag).toBe('Failure');
    });
  });

  describe('analyzeElementProperties', () => {
    const mockTraversalSource = {} as any;
    const mockPropertyKeysFn = jest.fn<(g: any, label: string) => Effect.Effect<string[], any>>();

    it('should analyze properties for multiple labels', async () => {
      const mockLabels = ['person', 'company'];
      const mockResults = [
        { name: 'name', type: ['string'] },
        { name: 'companyName', type: ['string'] },
      ];

      mockProcessBatched.mockReturnValue(Effect.succeed(mockResults));

      const result = await Effect.runPromise(
        analyzeElementProperties(
          mockTraversalSource,
          mockLabels,
          mockPropertyKeysFn,
          mockConfig,
          true
        )
      );

      expect(result).toEqual(mockResults);
    });
  });
});
