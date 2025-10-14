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
 * Tests for Zod schemas and model validation.
 */

import {
  PropertySchema,
  VertexSchema,
  EdgeSchema,
  GraphSchemaSchema,
  GremlinConfigSchema,
  GremlinQueryResultSchema,
  SchemaMetadataSchema,
} from '../src/gremlin/models';

describe('Models and Schemas', () => {
  describe('PropertySchema', () => {
    it('should validate a valid property', () => {
      const validProperty = {
        name: 'age',
        type: ['number'],
        cardinality: 'single',
        enum: [25, 30, 35],
      };

      expect(() => PropertySchema.parse(validProperty)).not.toThrow();
    });

    it('should reject invalid property', () => {
      const invalidProperty = {
        name: 123, // Should be string, not number
        type: 'string', // Should be array, not string
      };

      expect(() => PropertySchema.parse(invalidProperty)).toThrow();
    });
  });

  describe('VertexSchema', () => {
    it('should validate a valid vertex', () => {
      const validVertex = {
        label: 'person',
        properties: [
          {
            name: 'name',
            type: ['string'],
            cardinality: 'single',
          },
        ],
        count: 100,
      };

      expect(() => VertexSchema.parse(validVertex)).not.toThrow();
    });
  });

  describe('EdgeSchema', () => {
    it('should validate a valid edge', () => {
      const validEdge = {
        label: 'knows',
        properties: [],
        count: 50,
      };

      expect(() => EdgeSchema.parse(validEdge)).not.toThrow();
    });
  });

  describe('GraphSchemaSchema', () => {
    it('should validate a complete graph schema with edge_patterns', () => {
      const validSchema = {
        vertices: [
          {
            label: 'person',
            properties: [
              {
                name: 'name',
                type: ['string'],
                cardinality: 'single',
              },
            ],
          },
        ],
        edges: [
          {
            label: 'knows',
            properties: [],
          },
        ],
        edge_patterns: [
          {
            left_vertex: 'person',
            right_vertex: 'person',
            relation: 'knows',
          },
        ],
        metadata: {
          vertex_count: 1,
          edge_count: 1,
          pattern_count: 1,
          schema_size_bytes: 1024,
          optimization_settings: {
            sample_values_included: false,
            max_enum_values: 10,
            counts_included: true,
            enum_cardinality_threshold: 50,
          },
          generated_at: '2024-01-01T00:00:00.000Z',
        },
      };

      expect(() => GraphSchemaSchema.parse(validSchema)).not.toThrow();

      // Verify edge_patterns is present and adjacency_list is not
      const parsed = GraphSchemaSchema.parse(validSchema);
      expect('edge_patterns' in parsed).toBe(true);
      expect('adjacency_list' in parsed).toBe(false);
    });
  });

  describe('GremlinConfigSchema', () => {
    it('should validate valid minimal config', () => {
      const validConfig = {
        host: 'localhost',
        port: 8182,
        traversalSource: 'g',
        useSSL: false,
        idleTimeoutSeconds: 300,
      };

      expect(() => GremlinConfigSchema.parse(validConfig)).not.toThrow();
    });

    it('should reject invalid port', () => {
      const invalidConfig = {
        host: 'localhost',
        port: -1, // Invalid port
        traversalSource: 'g',
        useSSL: false,
        idleTimeoutSeconds: 300,
      };

      expect(() => GremlinConfigSchema.parse(invalidConfig)).toThrow();
    });
  });

  describe('GremlinQueryResultSchema', () => {
    it('should validate query result', () => {
      const validResult = {
        results: [{ id: 1, label: 'person' }],
        message: 'Query executed successfully',
      };

      expect(() => GremlinQueryResultSchema.parse(validResult)).not.toThrow();
    });
  });

  describe('SchemaMetadataSchema', () => {
    it('should validate schema metadata', () => {
      const validMetadata = {
        vertex_count: 10,
        edge_count: 5,
        pattern_count: 8,
        schema_size_bytes: 2048,
        optimization_settings: {
          sample_values_included: false,
          max_enum_values: 10,
          counts_included: true,
          enum_cardinality_threshold: 50,
        },
        generated_at: '2024-01-01T00:00:00.000Z',
      };

      expect(() => SchemaMetadataSchema.parse(validMetadata)).not.toThrow();
    });
  });
});
