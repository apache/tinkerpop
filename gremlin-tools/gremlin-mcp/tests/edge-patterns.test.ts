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
 * @fileoverview Tests for the edge patterns module.
 *
 * Tests edge pattern analysis including edge connectivity patterns,
 * pattern processing, and edge statistics generation.
 */

import { Effect } from 'effect';
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import {
  generateEdgePatterns as generateEdgePatterns,
  analyzePatternStatistics,
} from '../src/gremlin/edge-patterns.js';
import { Errors } from '../src/errors.js';
import type { EdgePattern } from '../src/gremlin/models.js';

// Mock Gremlin query utilities
jest.mock('../src/gremlin/query-utils.js', () => ({
  executeGremlinQuery: jest.fn(),
}));

import { executeGremlinQuery } from '../src/gremlin/query-utils.js';

const mockExecuteGremlinQuery = executeGremlinQuery as jest.MockedFunction<
  typeof executeGremlinQuery
>;

describe('edge-patterns', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('generateEdgePatterns', () => {
    const mockTraversalSource = {
      E: jest.fn(),
    } as any;

    it('should generate edge patterns successfully', async () => {
      const mockRawPatterns = [
        { from: 'person', to: 'company', label: 'worksAt' },
        { from: 'person', to: 'person', label: 'knows' },
        { from: 'company', to: 'project', label: 'sponsors' },
      ];

      const expectedPatterns: EdgePattern[] = [
        { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' },
        { left_vertex: 'person', right_vertex: 'person', relation: 'knows' },
        { left_vertex: 'company', right_vertex: 'project', relation: 'sponsors' },
      ];

      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed(mockRawPatterns));

      const result = await Effect.runPromise(generateEdgePatterns(mockTraversalSource));

      expect(result).toEqual(expectedPatterns);
      expect(mockExecuteGremlinQuery).toHaveBeenCalledWith(
        expect.any(Function),
        'Failed to get edge patterns',
        expect.stringContaining('project')
      );
    });

    it('should handle custom maxPatterns limit', async () => {
      const mockRawPatterns = [{ from: 'person', to: 'company', label: 'worksAt' }];

      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed(mockRawPatterns));

      const result = await Effect.runPromise(
        generateEdgePatterns(mockTraversalSource, 500)
      );

      expect(result).toHaveLength(1);
      expect(mockExecuteGremlinQuery).toHaveBeenCalledWith(
        expect.any(Function),
        'Failed to get edge patterns',
        expect.stringContaining('limit(500)')
      );
    });

    it('should handle empty pattern results', async () => {
      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed([]));

      const result = await Effect.runPromise(generateEdgePatterns(mockTraversalSource));

      expect(result).toEqual([]);
    });

    it('should handle invalid pattern data gracefully', async () => {
      const mockRawPatterns = [
        { from: 'person', to: 'company', label: 'worksAt' },
        { from: null, to: 'company', label: 'invalid' }, // Invalid data
        { from: 'person', to: undefined, label: 'worksAt' }, // Invalid data
        { from: 'company', to: 'project', label: 'sponsors' },
      ];

      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed(mockRawPatterns));

      const result = await Effect.runPromise(generateEdgePatterns(mockTraversalSource));

      // Should filter out invalid patterns and process only valid ones
      expect(result).toHaveLength(2); // Only valid patterns processed
      expect(result[0]).toEqual({
        left_vertex: 'person',
        right_vertex: 'company',
        relation: 'worksAt',
      });
      expect(result[1]).toEqual({
        left_vertex: 'company',
        right_vertex: 'project',
        relation: 'sponsors',
      });
    });

    it('should handle query failures', async () => {
      const error = Errors.query('Connection failed', 'pattern query', new Error('Network error'));
      mockExecuteGremlinQuery.mockReturnValue(Effect.fail(error));

      const result = await Effect.runPromiseExit(generateEdgePatterns(mockTraversalSource));

      expect(result._tag).toBe('Failure');
    });

    it('should handle complex relationship patterns', async () => {
      const mockRawPatterns = [
        { from: 'person', to: 'company', label: 'worksAt' },
        { from: 'person', to: 'person', label: 'knows' },
        { from: 'person', to: 'person', label: 'friendsWith' },
        { from: 'company', to: 'company', label: 'partnerWith' },
        { from: 'project', to: 'person', label: 'ownedBy' },
      ];

      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed(mockRawPatterns));

      const result = await Effect.runPromise(generateEdgePatterns(mockTraversalSource));

      expect(result).toHaveLength(5);

      // Verify self-referencing patterns
      const selfReferencingPatterns = result.filter(p => p.left_vertex === p.right_vertex);
      expect(selfReferencingPatterns).toHaveLength(3);

      // Verify bidirectional potential
      const personToCompany = result.find(
        p => p.left_vertex === 'person' && p.right_vertex === 'company'
      );
      const companyToPerson = result.find(
        p => p.left_vertex === 'company' && p.right_vertex === 'person'
      );
      expect(personToCompany).toBeDefined();
      expect(companyToPerson).toBeUndefined(); // Not in this dataset
    });
  });

  describe('analyzePatternStatistics', () => {
    it('should analyze basic pattern statistics', () => {
      const patterns: EdgePattern[] = [
        { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' },
        { left_vertex: 'person', right_vertex: 'person', relation: 'knows' },
        { left_vertex: 'company', right_vertex: 'project', relation: 'sponsors' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(3);
      expect(stats.uniqueVertexTypes).toBe(3);
      expect(stats.uniqueEdgeTypes).toBe(3);
      expect(stats.averageConnectionsPerVertexType).toBe(1);
      expect(stats.vertexTypes).toEqual(['company', 'person', 'project']);
      expect(stats.edgeTypes).toEqual(['knows', 'sponsors', 'worksAt']);
    });

    it('should handle empty patterns array', () => {
      const patterns: EdgePattern[] = [];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(0);
      expect(stats.uniqueVertexTypes).toBe(0);
      expect(stats.uniqueEdgeTypes).toBe(0);
      expect(stats.averageConnectionsPerVertexType).toBe(0);
      expect(stats.vertexTypes).toEqual([]);
      expect(stats.edgeTypes).toEqual([]);
    });

    it('should count connection frequencies correctly', () => {
      const patterns: EdgePattern[] = [
        { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' },
        { left_vertex: 'person', right_vertex: 'company', relation: 'contractsWith' },
        { left_vertex: 'person', right_vertex: 'person', relation: 'knows' },
        { left_vertex: 'company', right_vertex: 'project', relation: 'sponsors' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.connectionFrequencies['person->company']).toBe(2);
      expect(stats.connectionFrequencies['person->person']).toBe(1);
      expect(stats.connectionFrequencies['company->project']).toBe(1);
    });

    it('should handle duplicate patterns correctly', () => {
      const patterns: EdgePattern[] = [
        { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' },
        { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' }, // Duplicate
        { left_vertex: 'person', right_vertex: 'company', relation: 'contractsWith' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(3);
      expect(stats.uniqueVertexTypes).toBe(2);
      expect(stats.uniqueEdgeTypes).toBe(2); // worksAt and contractsWith
      expect(stats.connectionFrequencies['person->company']).toBe(3);
    });

    it('should calculate averages correctly for complex patterns', () => {
      const patterns: EdgePattern[] = [
        { left_vertex: 'person', right_vertex: 'company', relation: 'worksAt' },
        { left_vertex: 'person', right_vertex: 'project', relation: 'owns' },
        { left_vertex: 'person', right_vertex: 'person', relation: 'knows' },
        { left_vertex: 'company', right_vertex: 'project', relation: 'sponsors' },
        { left_vertex: 'project', right_vertex: 'person', relation: 'managedBy' },
        { left_vertex: 'organization', right_vertex: 'person', relation: 'employs' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(6);
      expect(stats.uniqueVertexTypes).toBe(4); // person, company, project, organization
      expect(stats.uniqueEdgeTypes).toBe(6); // All relation types are unique
      expect(stats.averageConnectionsPerVertexType).toBe(1.5); // 6 patterns / 4 vertex types
    });

    it('should sort vertex and edge types alphabetically', () => {
      const patterns: EdgePattern[] = [
        { left_vertex: 'zebra', right_vertex: 'apple', relation: 'eats' },
        { left_vertex: 'banana', right_vertex: 'cherry', relation: 'grows' },
        { left_vertex: 'apple', right_vertex: 'banana', relation: 'becomes' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.vertexTypes).toEqual(['apple', 'banana', 'cherry', 'zebra']);
      expect(stats.edgeTypes).toEqual(['becomes', 'eats', 'grows']);
    });
  });
});
