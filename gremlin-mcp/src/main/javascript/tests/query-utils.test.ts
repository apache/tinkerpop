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
 * @fileoverview Tests for the query utilities module.
 *
 * Tests shared Gremlin query functions and batching utilities used
 * throughout the schema generation process.
 */

import { Effect } from 'effect';
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import {
  processBatched,
  executeGremlinQuery,
  getVertexLabels,
  getEdgeLabels,
} from '../src/gremlin/query-utils';

// Mock Gremlin traversal source
const mockTraversalSource = {
  V: jest.fn(),
  E: jest.fn(),
} as any;

describe('query-utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('processBatched', () => {
    it('should process items in batches correctly', async () => {
      const items = [1, 2, 3, 4, 5];
      const processor = (item: number) => Effect.succeed(item * 2);

      const result = await Effect.runPromise(processBatched(items, 2, processor));

      expect(result).toEqual([2, 4, 6, 8, 10]);
    });

    it('should handle processor failures', async () => {
      const items = [1, 2, 3];
      const processor = (item: number) =>
        item === 2 ? Effect.fail('error') : Effect.succeed(item * 2);

      const result = await Effect.runPromiseExit(processBatched(items, 2, processor));

      expect(result._tag).toBe('Failure');
    });
  });

  describe('executeGremlinQuery', () => {
    it('should execute successful queries', async () => {
      const mockQuery = jest.fn<() => Promise<string[]>>().mockResolvedValue(['label1', 'label2']);

      const result = await Effect.runPromise(
        executeGremlinQuery(mockQuery, 'Test query failed', 'test query')
      );

      expect(result).toEqual(['label1', 'label2']);
    });

    it('should handle query failures', async () => {
      const mockQuery = jest
        .fn<() => Promise<string[]>>()
        .mockRejectedValue(new Error('Connection failed'));

      await expect(() =>
        Effect.runPromise(executeGremlinQuery(mockQuery, 'Test query failed', 'test query'))
      ).rejects.toThrow();
    });
  });

  describe('getVertexLabels', () => {
    it('should retrieve vertex labels successfully', async () => {
      const mockLabels = ['person', 'company', 'project'];
      const mockToList = jest.fn<() => Promise<string[]>>().mockResolvedValue(mockLabels);
      const mockDedup = jest.fn().mockReturnValue({ toList: mockToList });
      const mockLabel = jest.fn().mockReturnValue({ dedup: mockDedup });
      const mockV = jest.fn().mockReturnValue({ label: mockLabel });

      mockTraversalSource.V = mockV;

      const result = await Effect.runPromise(getVertexLabels(mockTraversalSource));

      expect(result).toEqual(mockLabels);
    });
  });

  describe('getEdgeLabels', () => {
    it('should retrieve edge labels successfully', async () => {
      const mockLabels = ['knows', 'worksAt', 'collaboratesOn'];
      const mockToList = jest.fn<() => Promise<string[]>>().mockResolvedValue(mockLabels);
      const mockDedup = jest.fn().mockReturnValue({ toList: mockToList });
      const mockLabel = jest.fn().mockReturnValue({ dedup: mockDedup });
      const mockE = jest.fn().mockReturnValue({ label: mockLabel });

      mockTraversalSource.E = mockE;

      const result = await Effect.runPromise(getEdgeLabels(mockTraversalSource));

      expect(result).toEqual(mockLabels);
    });
  });
});
