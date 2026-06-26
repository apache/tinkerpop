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
import { defaultPasses, type OptimizationPass } from './passes.js';
import { LocalExecutionError } from './LocalExecutionError.js';
import { Graph, Vertex, Edge } from '../../structure/graph.js';
import {
  wrap, getValue, NON_PRODUCTIVE, type StreamItem,
  stepOut, stepIn, stepBoth, stepOutE, stepInE, stepBothE, stepOutV, stepInV, stepOtherV,
  stepHas, stepHasId, stepHasLabel, stepHasNot,
  stepId, stepLabel, stepValue, stepKey, stepValues, stepValueMap, stepElementMap,
  stepPath,
  stepLimit, stepRange, stepSkip, stepTail, stepOrder,
  stepAddV, stepAddEWithModulators, stepProperty,
} from './steps.js';

type StepFn = (source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean) => Generator<StreamItem>;

/** Steps allowed inside a by(Traversal) projection on path() — single value extraction only. */
const VALUE_EXTRACTION_STEPS = new Set<string>([
  'id', 'label', 'key', 'value', 'values', 'valueMap', 'elementMap',
]);

/**
 * Executes a Tiny Gremlin Pipeline against a local in-memory Graph, returning
 * an AsyncGenerator that yields one result element at a time.
 */
export class LocalExecutor {
  private readonly passes: OptimizationPass[];

  constructor(passes: OptimizationPass[] = defaultPasses) {
    this.passes = passes;
  }

  execute(pipeline: Pipeline, graph: Graph): AsyncGenerator<any> {
    const optimized = this.passes.reduce((p, pass) => pass(p), pipeline);
    const folded = this.foldModulators(optimized);
    const trackPaths = folded.some(s => s.name === 'path' || s.name === 'otherV');
    const stream = this.buildChain(folded, graph, trackPaths);
    return this.toAsync(stream, trackPaths);
  }

  /**
   * Folds 'from' and 'to' modulator steps that follow an 'addE' step into a
   * composite descriptor so the addE executor has all modulator args available.
   */
  private foldModulators(pipeline: Pipeline): Pipeline {
    const result: StepDescriptor[] = [];
    let i = 0;
    while (i < pipeline.length) {
      const step = pipeline[i];
      if (step.name === 'addE') {
        let fromArgs: Arg[] | null = null;
        let toArgs: Arg[] | null = null;
        let j = i + 1;
        while (j < pipeline.length && (pipeline[j].name === 'from' || pipeline[j].name === 'to')) {
          if (pipeline[j].name === 'from') fromArgs = pipeline[j].args[0] as unknown as Arg[];
          else toArgs = pipeline[j].args[0] as unknown as Arg[];
          j++;
        }
        result.push({ name: 'addE', args: [step.args[0], fromArgs as unknown as Arg, toArgs as unknown as Arg] });
        i = j;
      } else if (step.name === 'order') {
        // Fold a single following by() modulator into the order step args
        const next = pipeline[i + 1];
        if (next?.name === 'by') {
          if (Array.isArray(next.args[0])) {
            throw new LocalExecutionError('by(Traversal) is not supported for order() in Tiny Gremlin');
          }
          result.push({ name: 'order', args: next.args }); // args = [key, direction]
          i += 2;
        } else {
          result.push(step);
          i++;
        }
      } else if (step.name === 'path') {
        // Fold all following by() modulators into the path step as a list of projections.
        // Each projection is a key (String/T/null) or a single-step value-extraction
        // sub-pipeline (by(Traversal)); they are applied round-robin across path elements.
        const projections: Arg[] = [];
        let j = i + 1;
        while (j < pipeline.length && pipeline[j].name === 'by') {
          const proj = pipeline[j].args[0];
          if (Array.isArray(proj)) {
            const sub = proj as unknown as Pipeline;
            if (sub.length !== 1 || !VALUE_EXTRACTION_STEPS.has(sub[0].name)) {
              throw new LocalExecutionError(
                'Tiny Gremlin only supports by(Traversal) on path() with a single value-extraction ' +
                'step (id, label, key, value, values, valueMap, elementMap)',
              );
            }
          }
          projections.push(proj);
          j++;
        }
        result.push({ name: 'path', args: projections });
        i = j;
      } else {
        result.push(step);
        i++;
      }
    }
    return result;
  }

  private buildChain(pipeline: Pipeline, graph: Graph, trackPaths: boolean): Iterable<StreamItem> {
    if (pipeline.length === 0) return [];

    const [source, ...rest] = pipeline;
    let stream: Iterable<StreamItem> = this.applySourceStep(source, graph, trackPaths);

    for (const step of rest) {
      stream = this.applyStep(step, stream, graph, trackPaths);
    }
    return stream;
  }

  private applySourceStep(step: StepDescriptor, graph: Graph, trackPaths: boolean): Iterable<StreamItem> {
    switch (step.name) {
      case 'V': return sourceV(step.args, graph, trackPaths);
      case 'E': return sourceE(step.args, graph, trackPaths);
      case 'addV': return stepAddV([null], step.args, graph, trackPaths);
      case 'addE': return this.applyAddE([null], step, graph, trackPaths);
      default:
        throw new LocalExecutionError(`'${step.name}' cannot be used as a source step`);
    }
  }

  private applyStep(step: StepDescriptor, source: Iterable<StreamItem>, graph: Graph, trackPaths: boolean): Iterable<StreamItem> {
    if (step.name === 'addE') return this.applyAddE(source, step, graph, trackPaths);
    if (step.name === 'path') return this.applyPath(source, step, graph, trackPaths);
    const fn = STEP_REGISTRY.get(step.name);
    if (!fn) throw new LocalExecutionError(`No implementation for step '${step.name}'`);
    return fn(source, step.args, graph, trackPaths);
  }

  /**
   * Executes path() with its folded by() projections. Provides stepPath a callback that
   * runs a single value-extraction sub-step against one path element, taking its first
   * result (e.g. values('name') yields the first value).
   */
  private applyPath(
    source: Iterable<StreamItem>,
    step: StepDescriptor,
    graph: Graph,
    trackPaths: boolean,
  ): Iterable<StreamItem> {
    const runTraversal = (sub: Pipeline, object: any): any => {
      const sub0 = sub[0];
      const fn = STEP_REGISTRY.get(sub0.name);
      if (!fn) throw new LocalExecutionError(`No implementation for step '${sub0.name}'`);
      for (const out of fn([wrap(object, [], false)], sub0.args, graph, false)) {
        return getValue(out, false);
      }
      return NON_PRODUCTIVE; // non-productive by(Traversal) filters the path traverser
    };
    return stepPath(source, step.args, trackPaths, runTraversal);
  }

  private applyAddE(
    source: Iterable<StreamItem>,
    step: StepDescriptor,
    graph: Graph,
    trackPaths: boolean,
  ): Iterable<StreamItem> {
    const label = step.args[0] as string;
    const fromPipeline = step.args[1] as Arg[] | null;
    const toPipeline = step.args[2] as Arg[] | null;
    return stepAddEWithModulators(
      source, label, fromPipeline, toPipeline, graph, trackPaths,
      (subPipeline, g) => this.executeSubPipeline(subPipeline as unknown as Pipeline, g),
    );
  }

  private executeSubPipeline(pipeline: Pipeline, graph: Graph): any {
    const stream = this.buildChain(pipeline, graph, false);
    for (const item of stream) return item;
    return null;
  }

  private async *toAsync(stream: Iterable<StreamItem>, trackPaths: boolean): AsyncGenerator<any> {
    for (const item of stream) {
      const value = trackPaths ? getValue(item, true) : item;
      // Yield a shallow copy of Vertex/Edge so callers that mutate
      // result objects (e.g. removeProperties) don't corrupt the graph.
      yield shallowCopyElement(value);
    }
  }
}

// ── Result copying ────────────────────────────────────────────────────────────

/** Shallow-copies Vertex/Edge/Path so callers can mutate results without corrupting the graph. */
function shallowCopyElement(value: any): any {
  if (value instanceof Vertex || value instanceof Edge) return Object.assign(Object.create(Object.getPrototypeOf(value)), value);
  return value;
}

// ── Source steps ──────────────────────────────────────────────────────────────
// Mid-traversal V/E: for each incoming traverser, emit matching vertices/edges (cross-product).

function* midSourceV(source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean): Generator<StreamItem> {
  for (const _item of source) {
    yield* sourceV(args, graph, trackPaths);
  }
}

function* midSourceE(source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean): Generator<StreamItem> {
  for (const _item of source) {
    yield* sourceE(args, graph, trackPaths);
  }
}

function lookupById<T>(map: Map<any, T>, id: Arg): T | undefined {
  return map.get(id);
}

function flattenIds(args: Arg[]): Arg[] {
  // Single list argument: unroll it, consistent with V([1,2,3]) semantics.
  // Multiple arguments: treat each as-is — a list is a single ID value, not unrolled.
  if (args.length === 1 && Array.isArray(args[0])) return args[0] as unknown as Arg[];
  return args;
}

function* sourceV(args: Arg[], graph: Graph, trackPaths: boolean): Generator<StreamItem> {
  const ids = flattenIds(args);
  if (ids.length === 0) {
    for (const v of graph.vertices.values()) yield wrap(v, [], trackPaths);
  } else {
    for (const id of ids) {
      if (id === null || id === undefined) continue;
      const v = lookupById(graph.vertices, id);
      if (v !== undefined) yield wrap(v, [], trackPaths);
    }
  }
}

function* sourceE(args: Arg[], graph: Graph, trackPaths: boolean): Generator<StreamItem> {
  const ids = flattenIds(args);
  if (ids.length === 0) {
    for (const e of graph.edges.values()) yield wrap(e, [], trackPaths);
  } else {
    for (const id of ids) {
      if (id === null || id === undefined) continue;
      const e = lookupById(graph.edges, id);
      if (e !== undefined) yield wrap(e, [], trackPaths);
    }
  }
}

// ── Step dispatch table ───────────────────────────────────────────────────────

const STEP_REGISTRY = new Map<string, StepFn>([
  // Mid-traversal V/E: for each incoming element produce matching vertices/edges (cross-product)
  ['V',           (source, args, graph, tp) => midSourceV(source, args, graph, tp)],
  ['E',           (source, args, graph, tp) => midSourceE(source, args, graph, tp)],
  ['out',         stepOut],
  ['in',          stepIn],
  ['both',        stepBoth],
  ['outE',        stepOutE],
  ['inE',         stepInE],
  ['bothE',       stepBothE],
  ['outV',        stepOutV],
  ['inV',         stepInV],
  ['otherV',      stepOtherV],
  ['has',         stepHas],
  ['hasId',       stepHasId],
  ['hasLabel',    stepHasLabel],
  ['hasNot',      stepHasNot],
  ['id',          stepId],
  ['label',       stepLabel],
  ['value',       stepValue],
  ['key',         stepKey],
  ['values',      stepValues],
  ['valueMap',    stepValueMap],
  ['elementMap',  stepElementMap],
  // 'path' is dispatched via applyPath (needs the registry for by(Traversal) projections)
  ['limit',       stepLimit],
  ['range',       stepRange],
  ['skip',        stepSkip],
  ['tail',        stepTail],
  ['order',       stepOrder],
  ['addV',        stepAddV],
  ['property',    stepProperty],
]);
