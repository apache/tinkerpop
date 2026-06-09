/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { P, TextP, EnumValue } from '../traversal.js';

/**
 * A single element flowing through a Tiny Gremlin local pipeline.
 * Carries the current value and its path history. No bulk count or sack —
 * those are Java concerns not needed for local in-memory execution.
 */
export interface Traverser<T = any> {
  value: T;
  /** Path entries accumulated so far. Empty when trackPaths is false. */
  path: PathEntry[];
}

export interface PathEntry {
  labels: string[];
  object: any;
}

/**
 * A typed argument to a step descriptor. Pipeline is used for nested
 * anonymous traversal arguments (e.g. the inner traversal of where()).
 */
export type Arg = string | number | bigint | boolean | null | P | TextP | EnumValue | Pipeline;

/**
 * A single step in a Tiny Gremlin pipeline: its name and fully-typed arguments.
 * Nested traversals appear as Pipeline values within args.
 */
export interface StepDescriptor {
  name: string;
  args: Arg[];
}

/**
 * An ordered sequence of StepDescriptors representing a complete Tiny Gremlin
 * traversal or anonymous sub-traversal.
 */
export type Pipeline = StepDescriptor[];
