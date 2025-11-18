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
 * @fileoverview Application constants and default values.
 *
 * Centralized location for all constant values used throughout the application
 * including server metadata, MCP protocol identifiers, and configuration defaults.
 */

export const GREMLIN_VERSION = '3.8.1-SNAPSHOT'; // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

// Server Information
export const SERVER_INFO = {
  NAME: 'gremlin-mcp',
  VERSION: GREMLIN_VERSION,
} as const;

// MCP Resource URIs
export const RESOURCE_URIS = {
  STATUS: 'gremlin://status',
  SCHEMA: 'gremlin://schema',
} as const;

// MCP Tool Names
export const TOOL_NAMES = {
  GET_GRAPH_STATUS: 'get_graph_status',
  GET_GRAPH_SCHEMA: 'get_graph_schema',
  RUN_GREMLIN_QUERY: 'run_gremlin_query',
  REFRESH_SCHEMA_CACHE: 'refresh_schema_cache',
} as const;

// Default Configuration Values
export const DEFAULTS = {
  TRAVERSAL_SOURCE: 'g',
  USE_SSL: false,
  LOG_LEVEL: 'info' as const,
} as const;

// Connection Status Messages (only used ones)
export const STATUS_MESSAGES = {
  AVAILABLE: 'Available',
} as const;

// MIME Types
export const MIME_TYPES = {
  TEXT_PLAIN: 'text/plain',
  APPLICATION_JSON: 'application/json',
} as const;
