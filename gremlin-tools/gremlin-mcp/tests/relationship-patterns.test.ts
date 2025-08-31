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
 * @fileoverview Tests for the relationship patterns module.
 *
 * Tests relationship pattern analysis including edge connectivity patterns,
 * pattern processing, and relationship statistics generation.
 */

import { Effect } from 'effect';
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import {
  generateRelationshipPatterns,
  analyzePatternStatistics,
} from '../src/gremlin/relationship-patterns.js';
import { Errors } from '../src/errors.js';
import type { RelationshipPattern } from '../src/gremlin/models.js';

// Mock Gremlin query utilities
jest.mock('../src/gremlin/query-utils.js', () => ({
  executeGremlinQuery: jest.fn(),
}));

import { executeGremlinQuery } from '../src/gremlin/query-utils.js';

const mockExecuteGremlinQuery = executeGremlinQuery as jest.MockedFunction<
  typeof executeGremlinQuery
>;

describe('relationship-patterns', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('generateRelationshipPatterns', () => {
    const mockTraversalSource = {
      E: jest.fn(),
    } as any;

    it('should generate relationship patterns successfully', async () => {
      const mockRawPatterns = [
        { from: 'person', to: 'company', label: 'worksAt' },
        { from: 'person', to: 'person', label: 'knows' },
        { from: 'company', to: 'project', label: 'sponsors' },
      ];

      const expectedPatterns: RelationshipPattern[] = [
        { left_node: 'person', right_node: 'company', relation: 'worksAt' },
        { left_node: 'person', right_node: 'person', relation: 'knows' },
        { left_node: 'company', right_node: 'project', relation: 'sponsors' },
      ];

      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed(mockRawPatterns));

      const result = await Effect.runPromise(generateRelationshipPatterns(mockTraversalSource));

      expect(result).toEqual(expectedPatterns);
      expect(mockExecuteGremlinQuery).toHaveBeenCalledWith(
        expect.any(Function),
        'Failed to get relationship patterns',
        expect.stringContaining('project')
      );
    });

    it('should handle custom maxPatterns limit', async () => {
      const mockRawPatterns = [{ from: 'person', to: 'company', label: 'worksAt' }];

      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed(mockRawPatterns));

      const result = await Effect.runPromise(
        generateRelationshipPatterns(mockTraversalSource, 500)
      );

      expect(result).toHaveLength(1);
      expect(mockExecuteGremlinQuery).toHaveBeenCalledWith(
        expect.any(Function),
        'Failed to get relationship patterns',
        expect.stringContaining('limit(500)')
      );
    });

    it('should handle empty pattern results', async () => {
      mockExecuteGremlinQuery.mockReturnValue(Effect.succeed([]));

      const result = await Effect.runPromise(generateRelationshipPatterns(mockTraversalSource));

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

      const result = await Effect.runPromise(generateRelationshipPatterns(mockTraversalSource));

      // Should filter out invalid patterns and process only valid ones
      expect(result).toHaveLength(2); // Only valid patterns processed
      expect(result[0]).toEqual({
        left_node: 'person',
        right_node: 'company',
        relation: 'worksAt',
      });
      expect(result[1]).toEqual({
        left_node: 'company',
        right_node: 'project',
        relation: 'sponsors',
      });
    });

    it('should handle query failures', async () => {
      const error = Errors.query('Connection failed', 'pattern query', new Error('Network error'));
      mockExecuteGremlinQuery.mockReturnValue(Effect.fail(error));

      const result = await Effect.runPromiseExit(generateRelationshipPatterns(mockTraversalSource));

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

      const result = await Effect.runPromise(generateRelationshipPatterns(mockTraversalSource));

      expect(result).toHaveLength(5);

      // Verify self-referencing patterns
      const selfReferencingPatterns = result.filter(p => p.left_node === p.right_node);
      expect(selfReferencingPatterns).toHaveLength(3);

      // Verify bidirectional potential
      const personToCompany = result.find(
        p => p.left_node === 'person' && p.right_node === 'company'
      );
      const companyToPerson = result.find(
        p => p.left_node === 'company' && p.right_node === 'person'
      );
      expect(personToCompany).toBeDefined();
      expect(companyToPerson).toBeUndefined(); // Not in this dataset
    });
  });

  describe('analyzePatternStatistics', () => {
    it('should analyze basic pattern statistics', () => {
      const patterns: RelationshipPattern[] = [
        { left_node: 'person', right_node: 'company', relation: 'worksAt' },
        { left_node: 'person', right_node: 'person', relation: 'knows' },
        { left_node: 'company', right_node: 'project', relation: 'sponsors' },
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
      const patterns: RelationshipPattern[] = [];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(0);
      expect(stats.uniqueVertexTypes).toBe(0);
      expect(stats.uniqueEdgeTypes).toBe(0);
      expect(stats.averageConnectionsPerVertexType).toBe(0);
      expect(stats.vertexTypes).toEqual([]);
      expect(stats.edgeTypes).toEqual([]);
    });

    it('should count connection frequencies correctly', () => {
      const patterns: RelationshipPattern[] = [
        { left_node: 'person', right_node: 'company', relation: 'worksAt' },
        { left_node: 'person', right_node: 'company', relation: 'contractsWith' },
        { left_node: 'person', right_node: 'person', relation: 'knows' },
        { left_node: 'company', right_node: 'project', relation: 'sponsors' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.connectionFrequencies['person->company']).toBe(2);
      expect(stats.connectionFrequencies['person->person']).toBe(1);
      expect(stats.connectionFrequencies['company->project']).toBe(1);
    });

    it('should handle duplicate patterns correctly', () => {
      const patterns: RelationshipPattern[] = [
        { left_node: 'person', right_node: 'company', relation: 'worksAt' },
        { left_node: 'person', right_node: 'company', relation: 'worksAt' }, // Duplicate
        { left_node: 'person', right_node: 'company', relation: 'contractsWith' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(3);
      expect(stats.uniqueVertexTypes).toBe(2);
      expect(stats.uniqueEdgeTypes).toBe(2); // worksAt and contractsWith
      expect(stats.connectionFrequencies['person->company']).toBe(3);
    });

    it('should calculate averages correctly for complex patterns', () => {
      const patterns: RelationshipPattern[] = [
        { left_node: 'person', right_node: 'company', relation: 'worksAt' },
        { left_node: 'person', right_node: 'project', relation: 'owns' },
        { left_node: 'person', right_node: 'person', relation: 'knows' },
        { left_node: 'company', right_node: 'project', relation: 'sponsors' },
        { left_node: 'project', right_node: 'person', relation: 'managedBy' },
        { left_node: 'organization', right_node: 'person', relation: 'employs' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.totalPatterns).toBe(6);
      expect(stats.uniqueVertexTypes).toBe(4); // person, company, project, organization
      expect(stats.uniqueEdgeTypes).toBe(6); // All relation types are unique
      expect(stats.averageConnectionsPerVertexType).toBe(1.5); // 6 patterns / 4 vertex types
    });

    it('should sort vertex and edge types alphabetically', () => {
      const patterns: RelationshipPattern[] = [
        { left_node: 'zebra', right_node: 'apple', relation: 'eats' },
        { left_node: 'banana', right_node: 'cherry', relation: 'grows' },
        { left_node: 'apple', right_node: 'banana', relation: 'becomes' },
      ];

      const stats = analyzePatternStatistics(patterns);

      expect(stats.vertexTypes).toEqual(['apple', 'banana', 'cherry', 'zebra']);
      expect(stats.edgeTypes).toEqual(['becomes', 'eats', 'grows']);
    });
  });
});
