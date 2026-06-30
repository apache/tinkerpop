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
import type { Graph } from '../../structure/graph.js';

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

/**
 * The capabilities a "parent" step (one that runs nested child pipelines) needs
 * from the executor. This is Tiny Gremlin's lightweight analog of Java's
 * TraversalParent: rather than each parent step reaching back into the executor
 * through bespoke escapes, the executor hands every parent step a context with
 * three child-execution modes.
 */
export interface ExecutionContext {
  graph: Graph;
  trackPaths: boolean;
  /** Apply a child pipeline as mid-traversal steps over an existing stream (flatMap per traverser). */
  runBranch(child: Pipeline, source: Iterable<any>): Iterable<any>;
  /** Run a single value-extraction child against one object, returning its first result or NON_PRODUCTIVE. */
  runProject(child: Pipeline, object: any): any;
  /** Run a child as a complete source-rooted pipeline (e.g. addE from/to), returning the first result. */
  runRooted(child: Pipeline): any;
}

/**
 * The folded form of a repeat() step and its times()/until()/emit() modulators.
 * `untilFirst`/`emitFirst` record whether the modulator was declared before the
 * repeat() (while/emit-before) or after it (do-while/emit-after). A null `emit`
 * with `emitPresent` true is a bare emit() that always emits.
 */
export interface RepeatSpec {
  body: Pipeline;
  until: Pipeline | null;
  untilFirst: boolean;
  emit: Pipeline | null;
  emitPresent: boolean;
  emitFirst: boolean;
  times: number | null;
  /**
   * Whether times() was declared before the repeat(). Like until(), position decides
   * while vs. do-while: times(0).repeat(b) skips the body entirely (identity), while
   * repeat(b).times(0) still runs the body once.
   */
  timesFirst: boolean;
}
