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

import type { Arg, Pipeline, StepDescriptor, ExecutionContext, RepeatSpec } from './types.js';
import { defaultPasses, type OptimizationPass } from './passes.js';
import { LocalExecutionError } from './LocalExecutionError.js';
import { Graph, Vertex, Edge } from '../../structure/graph.js';
import {
  wrap, getValue, NON_PRODUCTIVE, type StreamItem,
  stepOut, stepIn, stepBoth, stepOutE, stepInE, stepBothE, stepOutV, stepInV, stepOtherV,
  stepHas, stepHasId, stepHasLabel, stepHasNot,
  stepId, stepLabel, stepValue, stepKey, stepValues, stepValueMap, stepElementMap,
  stepPath, stepRepeat,
  stepLimit, stepRange, stepSkip, stepTail, stepOrder,
  stepAddV, stepAddEWithModulators, stepProperty,
} from './steps.js';

type StepFn = (source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean) => Generator<StreamItem>;

/**
 * A "parent" step that runs nested child pipelines (addE from/to, path projections,
 * repeat bodies). It receives the ExecutionContext rather than reaching back into the
 * executor through a bespoke escape, which is what lets new branching steps be added
 * as ordinary registry entries instead of special cases in applyStep.
 */
type ParentStepFn = (source: Iterable<StreamItem>, args: Arg[], ctx: ExecutionContext) => Iterable<StreamItem>;

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
    const trackPaths = this.needsPaths(folded);
    const ctx = this.makeContext(graph, trackPaths);
    const stream = this.buildChain(folded, ctx);
    return this.toAsync(stream, trackPaths);
  }

  /**
   * Folds modulator steps into the steps they modify, then recurses into nested child
   * pipelines (repeat bodies, until/emit conditions, addE from/to, path projections) so
   * an inner order().by() or repeat() is folded too.
   */
  private foldModulators(pipeline: Pipeline): Pipeline {
    const folded = this.foldLevel(pipeline);
    for (const step of folded) {
      if (step.name === 'repeat') {
        const spec = step.args[0] as unknown as RepeatSpec;
        spec.body = this.foldModulators(spec.body);
        if (Array.isArray(spec.until)) spec.until = this.foldModulators(spec.until as Pipeline);
        if (Array.isArray(spec.emit)) spec.emit = this.foldModulators(spec.emit as Pipeline);
      } else {
        for (let a = 0; a < step.args.length; a++) {
          const arg = step.args[a];
          if (isPipelineArg(arg)) {
            step.args[a] = this.foldModulators(arg as unknown as Pipeline) as unknown as Arg;
          }
        }
      }
    }
    return folded;
  }

  /** Folds the modulators at a single pipeline level (no recursion into child pipelines). */
  private foldLevel(pipeline: Pipeline): Pipeline {
    const result: StepDescriptor[] = [];
    let i = 0;
    while (i < pipeline.length) {
      const step = pipeline[i];
      if (step.name === 'addE') {
        // Fold the following from()/to() modulators into the addE descriptor.
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
        // Fold a single following by() modulator into the order step args.
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
      } else if (step.name === 'repeat') {
        // repeat() with only trailing times()/until()/emit() modulators.
        let m = i + 1;
        while (m < pipeline.length && isRepeatModulator(pipeline[m].name)) m++;
        result.push({ name: 'repeat', args: [buildRepeatSpec(step, [], pipeline.slice(i + 1, m)) as unknown as Arg] });
        i = m;
      } else if (isRepeatModulator(step.name)) {
        // A leading run of times()/until()/emit() — these declare modulators on the
        // repeat() that must immediately follow (e.g. until(c).repeat(b), emit().repeat(b)).
        let k = i;
        while (k < pipeline.length && isRepeatModulator(pipeline[k].name)) k++;
        if (k >= pipeline.length || pipeline[k].name !== 'repeat') {
          throw new LocalExecutionError(`'${step.name}' is only supported as a repeat() modulator in Tiny Gremlin`);
        }
        const leading = pipeline.slice(i, k);
        let m = k + 1;
        while (m < pipeline.length && isRepeatModulator(pipeline[m].name)) m++;
        const trailing = pipeline.slice(k + 1, m);
        result.push({ name: 'repeat', args: [buildRepeatSpec(pipeline[k], leading, trailing) as unknown as Arg] });
        i = m;
      } else {
        result.push(step);
        i++;
      }
    }
    return result;
  }

  /** True when paths must be tracked: a path()/otherV() anywhere top-level or inside a repeat body. */
  private needsPaths(pipeline: Pipeline): boolean {
    for (const step of pipeline) {
      if (step.name === 'path' || step.name === 'otherV') return true;
      if (step.name === 'repeat' && this.needsPaths((step.args[0] as unknown as RepeatSpec).body)) return true;
    }
    return false;
  }

  /** Builds the ExecutionContext handed to parent steps for running their child pipelines. */
  private makeContext(graph: Graph, trackPaths: boolean): ExecutionContext {
    const ctx: ExecutionContext = {
      graph,
      trackPaths,
      runBranch: (child, src) => {
        let stream: Iterable<StreamItem> = src;
        for (const step of child) stream = this.applyStep(step, stream, ctx);
        return stream;
      },
      runProject: (child, object) => {
        // child is a single value-extraction step (validated during folding).
        const probe: ExecutionContext = { ...ctx, trackPaths: false };
        for (const out of this.applyStep(child[0], [wrap(object, [], false)], probe)) {
          return getValue(out, false);
        }
        return NON_PRODUCTIVE; // non-productive by(Traversal) filters the path traverser
      },
      runRooted: (child) => {
        const probe: ExecutionContext = { ...ctx, trackPaths: false };
        for (const item of this.buildChain(child, probe)) return item;
        return null;
      },
    };
    return ctx;
  }

  private buildChain(pipeline: Pipeline, ctx: ExecutionContext): Iterable<StreamItem> {
    if (pipeline.length === 0) return [];
    const [source, ...rest] = pipeline;
    let stream: Iterable<StreamItem> = this.applySourceStep(source, ctx);
    for (const step of rest) stream = this.applyStep(step, stream, ctx);
    return stream;
  }

  private applySourceStep(step: StepDescriptor, ctx: ExecutionContext): Iterable<StreamItem> {
    switch (step.name) {
      case 'V': return sourceV(step.args, ctx.graph, ctx.trackPaths);
      case 'E': return sourceE(step.args, ctx.graph, ctx.trackPaths);
      case 'addV': return stepAddV([null], step.args, ctx.graph, ctx.trackPaths);
      case 'addE': return PARENT_STEPS.get('addE')!([null], step.args, ctx);
      default:
        throw new LocalExecutionError(`'${step.name}' cannot be used as a source step`);
    }
  }

  private applyStep(step: StepDescriptor, source: Iterable<StreamItem>, ctx: ExecutionContext): Iterable<StreamItem> {
    const parent = PARENT_STEPS.get(step.name);
    if (parent) return parent(source, step.args, ctx);
    const fn = STEP_REGISTRY.get(step.name);
    if (!fn) throw new LocalExecutionError(`No implementation for step '${step.name}'`);
    return fn(source, step.args, ctx.graph, ctx.trackPaths);
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

// ── Modulator folding helpers ───────────────────────────────────────────────────

function isRepeatModulator(name: string): boolean {
  return name === 'times' || name === 'until' || name === 'emit';
}

/** True when an argument is itself a pipeline (a non-empty array of step descriptors). */
function isPipelineArg(arg: unknown): boolean {
  return Array.isArray(arg) && arg.length > 0 && arg[0] !== null
    && typeof arg[0] === 'object' && 'name' in (arg[0] as object);
}

/** Mutation steps. Graph mutation inside a loop is undefined, so these are blocked within repeat(). */
const MUTATION_STEPS = new Set<string>(['addV', 'addE', 'property']);

/**
 * Rejects mutation steps anywhere within a repeat() body or its until()/emit() conditions,
 * recursing into nested child pipelines (including nested repeats), since mutating the graph
 * while looping over it has no well-defined semantics.
 */
function assertNoMutationSteps(pipeline: Pipeline, context: string): void {
  for (const step of pipeline) {
    if (MUTATION_STEPS.has(step.name)) {
      throw new LocalExecutionError(
        `Mutation step '${step.name}()' is not supported inside ${context} in Tiny Gremlin`,
      );
    }
    for (const arg of step.args) {
      if (isPipelineArg(arg)) assertNoMutationSteps(arg as unknown as Pipeline, context);
    }
  }
}

/** Folds a repeat() step plus its leading/trailing modulators into a RepeatSpec. */
function buildRepeatSpec(repeatStep: StepDescriptor, leading: StepDescriptor[], trailing: StepDescriptor[]): RepeatSpec {
  const body = repeatStep.args[0];
  if (!isPipelineArg(body)) {
    throw new LocalExecutionError('repeat() with a loop label is not supported in Tiny Gremlin');
  }
  assertNoMutationSteps(body as unknown as Pipeline, 'a repeat() body');
  const spec: RepeatSpec = {
    body: body as unknown as Pipeline,
    until: null, untilFirst: false,
    emit: null, emitPresent: false, emitFirst: false,
    times: null, timesFirst: false,
  };
  for (const mod of leading) applyRepeatModulator(spec, mod, true);
  for (const mod of trailing) applyRepeatModulator(spec, mod, false);
  return spec;
}

function applyRepeatModulator(spec: RepeatSpec, mod: StepDescriptor, declaredFirst: boolean): void {
  if (mod.name === 'times') {
    spec.times = mod.args[0] as number;
    spec.timesFirst = declaredFirst;
  } else if (mod.name === 'until') {
    spec.until = requireTraversalCondition('until', mod.args[0]);
    spec.untilFirst = declaredFirst;
  } else if (mod.name === 'emit') {
    spec.emitPresent = true;
    spec.emit = mod.args.length ? requireTraversalCondition('emit', mod.args[0]) : null;
    spec.emitFirst = declaredFirst;
  }
}

/**
 * Validates an until()/emit() condition. Only the anonymous-traversal form is supported.
 * The predicate form (until(P)/emit(P)) maps to Java's Predicate<Traverser> lambda overload,
 * which Tiny Gremlin cannot express faithfully, so it is rejected with a clear message.
 */
function requireTraversalCondition(stepName: string, arg: Arg | undefined): Pipeline | null {
  if (arg == null) return null;
  if (!isPipelineArg(arg)) {
    throw new LocalExecutionError(
      `${stepName}(Predicate) is not supported in Tiny Gremlin; use the traversal form, ` +
      `e.g. ${stepName}(__.has(...))`,
    );
  }
  const pipeline = arg as unknown as Pipeline;
  assertNoMutationSteps(pipeline, `an ${stepName}() condition`);
  return pipeline;
}

// ── Parent step registry ────────────────────────────────────────────────────────
// Steps that run nested child pipelines. They receive the ExecutionContext and use
// its runBranch/runProject/runRooted helpers, so adding a new branching step is a
// registry entry rather than a special case in applyStep.

const PARENT_STEPS = new Map<string, ParentStepFn>([
  ['path', (source, args, ctx) => stepPath(source, args, ctx.trackPaths, (sub, obj) => ctx.runProject(sub, obj))],
  ['addE', (source, args, ctx) => stepAddEWithModulators(
    source,
    args[0] as string,
    args[1] as unknown as Arg[] | null,
    args[2] as unknown as Arg[] | null,
    ctx.graph,
    ctx.trackPaths,
    (sub) => ctx.runRooted(sub as unknown as Pipeline),
  )],
  ['repeat', (source, args, ctx) => stepRepeat(source, args[0] as unknown as RepeatSpec, ctx)],
]);

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
  // 'path', 'addE' and 'repeat' are parent steps dispatched via PARENT_STEPS (they run child pipelines)
  ['limit',       stepLimit],
  ['range',       stepRange],
  ['skip',        stepSkip],
  ['tail',        stepTail],
  ['order',       stepOrder],
  ['addV',        stepAddV],
  ['property',    stepProperty],
]);
