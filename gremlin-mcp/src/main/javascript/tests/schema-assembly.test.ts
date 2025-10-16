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
 * @fileoverview Tests for the schema assembly module.
 *
 * Tests schema assembly, validation, metadata generation, and error handling
 * for the final graph schema construction process.
 */

import { Effect } from 'effect';
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import {
  assembleGraphSchema,
  validateVertices,
  validateEdges,
  validateEdgePatterns,
  validateAllComponents,
} from '../src/gremlin/schema-assembly';
import type { Vertex, Edge, EdgePattern } from '../src/gremlin/models';
import type { SchemaConfig } from '../src/gremlin/types';

describe('schema-assembly', () => {
  const mockConfig: SchemaConfig = {
    includeSampleValues: true,
    maxEnumValues: 10,
    includeCounts: true,
    enumCardinalityThreshold: 5,
    enumPropertyDenyList: [],
    timeoutMs: 30000,
    batchSize: 10,
  };

  const sampleVertices: Vertex[] = [
    {
      label: 'person',
      properties: [
        { name: 'name', type: ['string'] },
        { name: 'age', type: ['number'] },
      ],
      count: 100,
    },
    {
      label: 'company',
      properties: [
        { name: 'name', type: ['string'] },
        { name: 'founded', type: ['number'] },
      ],
      count: 101,
    },
  ];

  const sampleEdges: Edge[] = [
    {
      label: 'worksAt',
      properties: [
        { name: 'since', type: ['string'] },
        { name: 'position', type: ['string'] },
      ],
      count: 50,
    },
    {
      label: 'knows',
      properties: [{ name: 'since', type: ['string'] }],
      count: 50,
    },
  ];

  const samplePatterns: EdgePattern[] = [
    { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' },
    { left_vertex: 'person', right_vertex: 'person', relation: 'knows' },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('assembleGraphSchema', () => {
    it('should assemble a valid graph schema successfully', async () => {
      const startTime = Date.now();

      const result = await Effect.runPromise(
        assembleGraphSchema(sampleVertices, sampleEdges, samplePatterns, mockConfig, startTime)
      );

      // Verify schema structure
      expect(result.vertices).toEqual(sampleVertices);
      expect(result.edges).toEqual(sampleEdges);
      expect(result.edge_patterns).toEqual(samplePatterns);

      // Verify metadata
      expect(result.metadata).toBeDefined();
      expect(result.metadata!.pattern_count).toBe(2);
      expect(result.metadata!.generated_at).toBeDefined();
      expect(result.metadata!.generation_time_ms).toBeGreaterThanOrEqual(0);

      // Verify optimization settings
      expect(result.metadata!.optimization_settings).toEqual({
        sample_values_included: true,
        max_enum_values: 10,
        counts_included: true,
        enum_cardinality_threshold: 5,
        timeout_ms: 30000,
        batch_size: 10,
      });
    });

    it('should handle empty components correctly', async () => {
      const startTime = Date.now();

      const result = await Effect.runPromise(
        assembleGraphSchema([], [], [], mockConfig, startTime)
      );

      expect(result.vertices).toEqual([]);
      expect(result.edges).toEqual([]);
      expect(result.edge_patterns).toEqual([]);
      expect(result.metadata!.pattern_count).toBe(0);
    });

    it('should calculate generation time correctly', async () => {
      const startTime = Date.now() - 1000; // 1 second ago

      const result = await Effect.runPromise(
        assembleGraphSchema(sampleVertices, sampleEdges, samplePatterns, mockConfig, startTime)
      );

      expect(result.metadata!.generation_time_ms).toBeGreaterThanOrEqual(1000);
      expect(result.metadata!.generation_time_ms).toBeLessThan(2000); // Should be reasonable
    });
  });

  describe('validateVertices', () => {
    it('should validate correct vertices successfully', async () => {
      const result = await Effect.runPromise(validateVertices(sampleVertices));

      expect(result).toBeUndefined(); // Void return on success
    });

    it('should detect missing labels', async () => {
      const invalidVertices: Vertex[] = [
        {
          label: '', // Invalid empty label
          properties: [{ name: 'test', type: ['string'] }],
        },
      ];

      await expect(() =>
        Effect.runPromise(validateVertices(invalidVertices))
      ).rejects.toBeDefined();
    });

    it('should detect invalid labels type', async () => {
      const invalidVertices: Vertex[] = [
        {
          label: null as any, // Invalid null label
          properties: [],
        },
      ];

      await expect(() =>
        Effect.runPromise(validateVertices(invalidVertices))
      ).rejects.toBeDefined();
    });

    it('should detect missing properties', async () => {
      const invalidVertices: Vertex[] = [
        {
          label: 'person',
          properties: undefined as any, // Missing properties
        },
      ];

      await expect(() =>
        Effect.runPromise(validateVertices(invalidVertices))
      ).rejects.toBeDefined();
    });

    it('should detect invalid property structure', async () => {
      const invalidVertices: Vertex[] = [
        {
          label: 'person',
          properties: [
            { name: '', type: ['string'] }, // Empty name
            { name: 'age', type: [] }, // Empty type array
          ],
        },
      ];

      await expect(() =>
        Effect.runPromise(validateVertices(invalidVertices))
      ).rejects.toBeDefined();
    });
  });

  describe('validateEdges', () => {
    it('should validate correct relationships successfully', async () => {
      const result = await Effect.runPromise(validateEdges(sampleEdges));

      expect(result).toBeUndefined(); // Void return on success
    });

    it('should detect missing labels', async () => {
      const invalidEdges: Edge[] = [
        {
          label: '', // Invalid empty type
          properties: [],
        },
      ];

      await expect(() => Effect.runPromise(validateEdges(invalidEdges))).rejects.toBeDefined();
    });

    it('should detect invalid properties', async () => {
      const invalidEdges: Edge[] = [
        {
          label: 'knows',
          properties: [
            { name: '', type: ['string'] }, // Empty name
          ],
        },
      ];

      await expect(() => Effect.runPromise(validateEdges(invalidEdges))).rejects.toBeDefined();
    });

    it('should handle missing properties array', async () => {
      const invalidEdges: Edge[] = [
        {
          label: 'knows',
          properties: undefined as any, // Missing properties
        },
      ];

      await expect(() => Effect.runPromise(validateEdges(invalidEdges))).rejects.toBeDefined();
    });
  });

  describe('validateEdgePatterns', () => {
    it('should validate correct patterns successfully', async () => {
      const result = await Effect.runPromise(validateEdgePatterns(samplePatterns));

      expect(result).toBeUndefined(); // Void return on success
    });

    it('should detect missing pattern fields', async () => {
      const invalidPatterns: EdgePattern[] = [
        {
          left_vertex: '', // Empty left vertex
          right_vertex: 'company',
          relation: 'worksAt',
        },
        {
          left_vertex: 'person',
          right_vertex: '', // Empty right vertex
          relation: 'knows',
        },
        {
          left_vertex: 'person',
          right_vertex: 'company',
          relation: '', // Empty relation
        },
      ];

      await expect(() =>
        Effect.runPromise(validateEdgePatterns(invalidPatterns))
      ).rejects.toBeDefined();
    });

    it('should detect invalid pattern field types', async () => {
      const invalidPatterns: EdgePattern[] = [
        {
          left_vertex: null as any, // Invalid type
          right_vertex: 'company',
          relation: 'worksAt',
        },
      ];

      await expect(() =>
        Effect.runPromise(validateEdgePatterns(invalidPatterns))
      ).rejects.toBeDefined();
    });
  });

  describe('validateAllComponents', () => {
    it('should validate all components successfully', async () => {
      const result = await Effect.runPromise(
        validateAllComponents(sampleVertices, sampleEdges, samplePatterns)
      );

      expect(result).toBeUndefined(); // Void return on success
    });

    it('should detect any invalid component', async () => {
      const invalidVertices: Vertex[] = [
        {
          label: '', // Invalid
          properties: [],
        },
      ];

      await expect(() =>
        Effect.runPromise(validateAllComponents(invalidVertices, sampleEdges, samplePatterns))
      ).rejects.toBeDefined();
    });

    it('should validate each component type independently', async () => {
      // Test with valid vertices but invalid edges
      const invalidEdges: Edge[] = [
        {
          label: '', // Invalid
          properties: [],
        },
      ];

      await expect(() =>
        Effect.runPromise(validateAllComponents(sampleVertices, invalidEdges, samplePatterns))
      ).rejects.toBeDefined();
    });
  });

  describe('metadata generation', () => {
    it('should include all required metadata fields', async () => {
      const startTime = Date.now() - 500;

      const result = await Effect.runPromise(
        assembleGraphSchema(sampleVertices, sampleEdges, samplePatterns, mockConfig, startTime)
      );

      const metadata = result.metadata!;

      // Check required fields
      expect(metadata.generated_at).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/); // ISO format
      expect(metadata.generation_time_ms).toBeGreaterThanOrEqual(500);
      expect(metadata.pattern_count).toBe(samplePatterns.length);

      // Check optimization settings
      const settings = metadata.optimization_settings;
      expect(settings.sample_values_included).toBe(mockConfig.includeSampleValues);
      expect(settings.max_enum_values).toBe(mockConfig.maxEnumValues);
      expect(settings.counts_included).toBe(mockConfig.includeCounts);
      expect(settings.enum_cardinality_threshold).toBe(mockConfig.enumCardinalityThreshold);
      expect(settings.timeout_ms).toBe(mockConfig.timeoutMs);
      expect(settings.batch_size).toBe(mockConfig.batchSize);
    });

    it('should handle missing optional config values', async () => {
      const minimalConfig: SchemaConfig = {
        includeSampleValues: false,
        maxEnumValues: 5,
        includeCounts: false,
        enumCardinalityThreshold: 3,
        enumPropertyDenyList: [],
        // timeoutMs and batchSize are optional
      };

      const result = await Effect.runPromise(
        assembleGraphSchema(sampleVertices, sampleEdges, samplePatterns, minimalConfig, Date.now())
      );

      const settings = result.metadata!.optimization_settings;
      // These get default values in the implementation
      expect(settings.timeout_ms).toBe(30000); // Default value
      expect(settings.batch_size).toBe(10); // Default value
    });
  });
});
