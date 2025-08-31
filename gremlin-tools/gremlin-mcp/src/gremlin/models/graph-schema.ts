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
 * @fileoverview Graph schema models for Gremlin database structure definition.
 */

import { z } from 'zod';

/**
 * Property definition for graph elements (vertices and edges).
 */
export const PropertySchema = z.object({
  /** The name of the property */
  name: z.string(),
  /** The data type(s) of the property */
  type: z.array(z.string()),
  /** A list of sample values for the property (optional for schema size optimization) */
  sample_values: z.array(z.unknown()).optional(),
  /** Cardinality information (single, list, set) */
  cardinality: z.string().optional(),
  /** A list of all possible values, if the property is determined to be an enum */
  enum: z.array(z.unknown()).optional(),
});

export type Property = z.infer<typeof PropertySchema>;

/**
 * Node type in the graph schema.
 */
export const NodeSchema = z.object({
  /** The label(s) that categorize this node type */
  labels: z.string(),
  /** List of properties that can be assigned to this node type */
  properties: z.array(PropertySchema).default([]),
  /** Count of vertices with this label */
  count: z.number().optional(),
});

export type Node = z.infer<typeof NodeSchema>;

/**
 * Relationship type in the graph schema.
 */
export const RelationshipSchema = z.object({
  /** The type/category of the relationship */
  type: z.string(),
  /** List of properties that can be assigned to this relationship type */
  properties: z.array(PropertySchema).default([]),
  /** Count of edges with this label */
  count: z.number().optional(),
});

export type Relationship = z.infer<typeof RelationshipSchema>;

/**
 * Valid relationship pattern between nodes.
 */
export const RelationshipPatternSchema = z.object({
  /** The label of the source/starting node */
  left_node: z.string(),
  /** The label of the target/ending node */
  right_node: z.string(),
  /** The type of relationship connecting the nodes */
  relation: z.string(),
});

export type RelationshipPattern = z.infer<typeof RelationshipPatternSchema>;

/**
 * Schema metadata and optimization settings.
 */
export const SchemaMetadataSchema = z.object({
  /** Total size of the schema in bytes */
  schema_size_bytes: z.number().optional(),
  /** Number of node types */
  node_count: z.number(),
  /** Number of relationship types */
  relationship_count: z.number(),
  /** Number of relationship patterns */
  pattern_count: z.number(),
  /** Time taken to generate the schema in milliseconds */
  generation_time_ms: z.number().optional(),
  /** Optimization settings used */
  optimization_settings: z.object({
    sample_values_included: z.boolean(),
    max_enum_values: z.number(),
    counts_included: z.boolean(),
    enum_cardinality_threshold: z.number(),
    timeout_ms: z.number().optional(),
    batch_size: z.number().optional(),
  }),
  /** When the schema was generated */
  generated_at: z.string(),
});

export type SchemaMetadata = z.infer<typeof SchemaMetadataSchema>;

/**
 * Complete graph schema definition.
 */
export const GraphSchemaSchema = z.object({
  /** List of all node types defined in the schema */
  nodes: z.array(NodeSchema),
  /** List of all relationship types defined in the schema */
  relationships: z.array(RelationshipSchema),
  /** List of valid relationship patterns between nodes */
  relationship_patterns: z.array(RelationshipPatternSchema),
  /** Schema metadata and optimization information */
  metadata: SchemaMetadataSchema.optional(),
});

export type GraphSchema = z.infer<typeof GraphSchemaSchema>;

/**
 * Gremlin server configuration.
 */
export const GremlinConfigSchema = z.object({
  /** Host address of the Gremlin server */
  host: z.string(),
  /** Port number of the Gremlin server */
  port: z.number().int().positive(),
  /** Traversal source name */
  traversalSource: z.string(),
  /** Whether to use SSL/TLS connection */
  useSSL: z.boolean(),
  /** Optional username for authentication */
  username: z.string().optional(),
  /** Optional password for authentication */
  password: z.string().optional(),
  /** Idle timeout in seconds */
  idleTimeoutSeconds: z.number().positive(),
  /** Whether enum discovery is enabled */
  enumDiscoveryEnabled: z.boolean().optional().default(true),
  /** Cardinality threshold for enum discovery */
  enumCardinalityThreshold: z.number().positive().optional().default(10),
  /** List of property names to exclude from enum discovery */
  enumPropertyBlacklist: z.array(z.string()).optional().default([]),
  /** Whether to include sample values in schema (for size optimization) */
  includeSampleValues: z.boolean().optional().default(false),
  /** Maximum number of enum values to include (for size optimization) */
  maxEnumValues: z.number().positive().optional().default(10),
  /** Whether to include vertex/edge counts in schema */
  includeCounts: z.boolean().optional().default(true),
});

export type GremlinConfig = z.infer<typeof GremlinConfigSchema>;
