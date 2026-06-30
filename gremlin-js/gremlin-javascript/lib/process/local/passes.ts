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

import type { Arg, Pipeline, StepDescriptor } from './types.js';
import { LocalExecutionError } from './LocalExecutionError.js';

export type OptimizationPass = (pipeline: Pipeline) => Pipeline;

/** Steps supported by the Tiny Gremlin local executor. */
const SUPPORTED_STEPS = new Set<string>([
  'V', 'E',
  'out', 'in', 'both', 'outE', 'inE', 'bothE', 'outV', 'inV', 'otherV',
  'has', 'hasId', 'hasLabel', 'hasNot',
  'id', 'label', 'values', 'valueMap', 'elementMap', 'value', 'key',
  'path',
  'limit', 'range', 'skip', 'tail',
  'order', 'by',
  'repeat', 'times', 'until', 'emit',
  'addV', 'addE', 'property',
  'from', 'to',
]);

/**
 * Validation pass — runs first. Throws LocalExecutionError for any step not
 * in the Tiny Gremlin subset, including steps inside nested sub-pipelines.
 */
export function validationPass(pipeline: Pipeline): Pipeline {
  for (const step of pipeline) {
    if (!SUPPORTED_STEPS.has(step.name)) {
      throw new LocalExecutionError(
        `'${step.name}' is not supported for Tiny Gremlin local execution`,
      );
    }
    for (const arg of step.args) {
      if (Array.isArray(arg) && arg.length > 0 && arg[0] !== null && typeof arg[0] === 'object' && 'name' in arg[0]) {
        validationPass(arg as Pipeline);
      }
    }
  }
  return pipeline;
}

/**
 * HasId folding pass — collapses V().hasId(id) → V(id) and E().hasId(id) → E(id),
 * enabling O(1) direct map lookup instead of full iteration.
 */
export function hasIdFoldingPass(pipeline: Pipeline): Pipeline {
  const result: StepDescriptor[] = [];
  let i = 0;
  while (i < pipeline.length) {
    const step = pipeline[i];
    const next = pipeline[i + 1];
    if (
      (step.name === 'V' || step.name === 'E') &&
      step.args.length === 0 &&
      next?.name === 'hasId'
    ) {
      result.push({ name: step.name, args: next.args });
      i += 2;
    } else {
      result.push(step);
      i++;
    }
  }
  return result;
}

/** Default optimization passes applied before execution. */
export const defaultPasses: OptimizationPass[] = [
  validationPass,
  hasIdFoldingPass,
];
