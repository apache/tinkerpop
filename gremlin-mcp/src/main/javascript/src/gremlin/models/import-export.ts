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
 * @fileoverview Data import/export operation models. Note that import operations have been temporarily removed.
 */

import { z } from 'zod';

/**
 * Export operation input schema with validation.
 */
export const ExportSubgraphInputSchema = z
  .object({
    traversal_query: z
      .string()
      .min(1, 'Traversal query cannot be empty')
      .max(10000, 'Traversal query cannot exceed 10,000 characters')
      .refine(
        query => {
          // Basic Gremlin syntax validation
          const invalidPatterns = [';', '--', '/*', '*/', 'DROP', 'DELETE'];
          return !invalidPatterns.some(pattern =>
            query.toUpperCase().includes(pattern.toUpperCase())
          );
        },
        {
          message: 'Query contains potentially unsafe operations',
        }
      ),
    format: z.enum(['graphson', 'json', 'csv'], {
      errorMap: () => ({ message: 'Format must be "graphson", "json", or "csv"' }),
    }),
    include_properties: z
      .array(z.string().min(1, 'Property name cannot be empty'))
      .max(100, 'Cannot include more than 100 properties')
      .optional(),
    exclude_properties: z
      .array(z.string().min(1, 'Property name cannot be empty'))
      .max(100, 'Cannot exclude more than 100 properties')
      .optional(),
    max_depth: z
      .number()
      .positive('Max depth must be positive')
      .max(10, 'Max depth cannot exceed 10 levels')
      .optional(),
  })
  .refine(
    data => {
      // Cannot have both include and exclude properties
      return !(data.include_properties && data.exclude_properties);
    },
    {
      message: 'Cannot specify both include_properties and exclude_properties',
      path: ['include_properties'],
    }
  );

export type ExportSubgraphInput = z.infer<typeof ExportSubgraphInputSchema>;
