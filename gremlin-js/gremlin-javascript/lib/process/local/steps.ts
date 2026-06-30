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

import { P, TextP, t, direction } from '../traversal.js';
import { Graph, Vertex, Edge, VertexProperty, Property, Path } from '../../structure/graph.js';
import type { Arg, Pipeline, ExecutionContext, RepeatSpec } from './types.js';

// ── Traverser helpers ─────────────────────────────────────────────────────────

export type StreamItem = any;

export function wrap(value: any, prevPath: PathEntry[], trackPaths: boolean): StreamItem {
  if (!trackPaths) return value;
  return { value, path: [...prevPath, { labels: [], object: value }] };
}

export function getValue(item: StreamItem, trackPaths: boolean): any {
  return trackPaths ? item.value : item;
}

export function getPath(item: StreamItem, trackPaths: boolean): PathEntry[] {
  return trackPaths ? (item.path ?? []) : [];
}

export interface PathEntry {
  labels: string[];
  object: any;
}

// ── Property lookup helpers ───────────────────────────────────────────────────

/** Returns the property key regardless of whether it's a Property or VertexProperty. */
function propKey(p: Property | VertexProperty): string {
  return p instanceof VertexProperty ? p.label : p.key;
}

/** Returns all properties of an element as a flat list. */
function allProps(el: Vertex | Edge): Array<Property | VertexProperty> {
  return (el.properties ?? []) as Array<Property | VertexProperty>;
}

/** Returns all values for a given key on an element (vertex properties are multi-valued). */
function getValues(el: Vertex | Edge, key: string): any[] {
  return allProps(el)
    .filter(p => propKey(p) === key)
    .map(p => (p as any).value);
}

/** Returns true when the element has at least one property with the given key. */
function hasKey(el: Vertex | Edge, key: string): boolean {
  return allProps(el).some(p => propKey(p) === key);
}

// ── Predicate evaluation ──────────────────────────────────────────────────────

export function evaluatePredicate(pred: P | TextP, value: any): boolean {
  const op = pred.operator;
  const v = pred.value;
  const other = pred.other;

  switch (op) {
    case 'eq':  { const c = compareForPredicate(value, v); return c !== null && c === 0; }
    case 'neq': { const c = compareForPredicate(value, v); return c !== null ? c !== 0 : value !== v; }
    case 'lt':  { const c = compareForPredicate(value, v); return c !== null && c < 0; }
    case 'lte': { const c = compareForPredicate(value, v); return c !== null && c <= 0; }
    case 'gt':  { const c = compareForPredicate(value, v); return c !== null && c > 0; }
    case 'gte': { const c = compareForPredicate(value, v); return c !== null && c >= 0; }
    case 'between': {
      const clo = compareForPredicate(value, v);
      const chi = compareForPredicate(value, other);
      return clo !== null && chi !== null && clo >= 0 && chi < 0;
    }
    case 'inside': {
      const clo = compareForPredicate(value, v);
      const chi = compareForPredicate(value, other);
      return clo !== null && chi !== null && clo > 0 && chi < 0;
    }
    case 'outside': {
      const clo = compareForPredicate(value, v);
      const chi = compareForPredicate(value, other);
      return (clo !== null && clo < 0) || (chi !== null && chi > 0);
    }
    case 'within':  return (Array.isArray(v) ? v : [v]).includes(value);
    case 'without': return !(Array.isArray(v) ? v : [v]).includes(value);
    case 'not':     return !evaluatePredicate(v as P, value);
    case 'and':     return evaluatePredicate(v as P, value) && evaluatePredicate(other as P, value);
    case 'or':      return evaluatePredicate(v as P, value) || evaluatePredicate(other as P, value);
    case 'containing':       return typeof value === 'string' && value.includes(v as string);
    case 'notContaining':    return typeof value === 'string' && !value.includes(v as string);
    case 'startingWith':     return typeof value === 'string' && value.startsWith(v as string);
    case 'notStartingWith':  return typeof value === 'string' && !value.startsWith(v as string);
    case 'endingWith':       return typeof value === 'string' && value.endsWith(v as string);
    case 'notEndingWith':    return typeof value === 'string' && !value.endsWith(v as string);
    case 'regex':    return typeof value === 'string' && new RegExp(v as string, 'u').test(value);
    case 'notRegex': return typeof value === 'string' && !new RegExp(v as string, 'u').test(value);
    default: return false;
  }
}

// ── Navigation steps ──────────────────────────────────────────────────────────

export function* stepOut(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const labels = args as string[];
  for (const item of source) {
    const v: Vertex = getValue(item, trackPaths);
    const edges = graph.outEdges.get(v.id) ?? [];
    for (const e of edges) {
      if (labels.length === 0 || labels.includes(e.label)) {
        yield wrap(e.inV, getPath(item, trackPaths), trackPaths);
      }
    }
  }
}

export function* stepIn(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const labels = args as string[];
  for (const item of source) {
    const v: Vertex = getValue(item, trackPaths);
    const edges = graph.inEdges.get(v.id) ?? [];
    for (const e of edges) {
      if (labels.length === 0 || labels.includes(e.label)) {
        yield wrap(e.outV, getPath(item, trackPaths), trackPaths);
      }
    }
  }
}

export function* stepBoth(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const buf = [...source];
  yield* stepOut(buf, args, graph, trackPaths);
  yield* stepIn(buf, args, graph, trackPaths);
}

export function* stepOutE(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const labels = args as string[];
  for (const item of source) {
    const v: Vertex = getValue(item, trackPaths);
    const edges = graph.outEdges.get(v.id) ?? [];
    for (const e of edges) {
      if (labels.length === 0 || labels.includes(e.label)) {
        yield wrap(e, getPath(item, trackPaths), trackPaths);
      }
    }
  }
}

export function* stepInE(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const labels = args as string[];
  for (const item of source) {
    const v: Vertex = getValue(item, trackPaths);
    const edges = graph.inEdges.get(v.id) ?? [];
    for (const e of edges) {
      if (labels.length === 0 || labels.includes(e.label)) {
        yield wrap(e, getPath(item, trackPaths), trackPaths);
      }
    }
  }
}

export function* stepBothE(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const buf = [...source];  // already buffered correctly
  yield* stepOutE(buf, args, graph, trackPaths);
  yield* stepInE(buf, args, graph, trackPaths);
}

export function* stepOutV(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const e: Edge = getValue(item, trackPaths);
    yield wrap(e.outV, getPath(item, trackPaths), trackPaths);
  }
}

export function* stepInV(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const e: Edge = getValue(item, trackPaths);
    yield wrap(e.inV, getPath(item, trackPaths), trackPaths);
  }
}

export function* stepOtherV(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  // trackPaths is always true when otherV is present (forced in LocalExecutor)
  for (const item of source) {
    const e: Edge = getValue(item, trackPaths);
    const path = getPath(item, trackPaths);
    const prevEntry = path.length >= 2 ? path[path.length - 2] : null;
    const prevId = prevEntry?.object?.id;
    const result = prevId === e.outV.id ? e.inV : e.outV;
    yield wrap(result, path, trackPaths);
  }
}

// ── Filter steps ──────────────────────────────────────────────────────────────

export function* stepHas(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const el = getValue(item, trackPaths);
    if (matchesHas(el, args)) yield item;
  }
}

function matchesHas(el: any, args: Arg[]): boolean {
  if (!(el instanceof Vertex || el instanceof Edge)) return false;

  if (args.length === 1) {
    const key = args[0] as string;
    if (key === 'T.id' || key === 'id') return true;
    if (key === 'T.label' || key === 'label') return true;
    return hasKey(el, key);
  }

  if (args.length === 2) {
    const [key, valOrPred] = args;
    const keyStr = key as string;
    let values: any[];
    if (keyStr === 'T.id' || keyStr === 'id') {
      values = [el.id];
    } else if (keyStr === 'T.label' || keyStr === 'label') {
      values = [el.label];
    } else {
      values = getValues(el, keyStr);
    }
    if (values.length === 0) return false;
    if (valOrPred instanceof P || valOrPred instanceof TextP) {
      return values.some(v => evaluatePredicate(valOrPred, v));
    }
    return values.some(v => v === valOrPred);
  }

  if (args.length === 3) {
    const [label, key, valOrPred] = args;
    if (el.label !== label) return false;
    const values = getValues(el, key as string);
    if (values.length === 0) return false;
    if (valOrPred instanceof P || valOrPred instanceof TextP) {
      return values.some(v => evaluatePredicate(valOrPred as P, v));
    }
    return values.some(v => v === valOrPred);
  }

  return false;
}

function idMatches(id: any, argId: Arg): boolean {
  if (argId === null || argId === undefined) return false;
  return id === argId;
}

function flattenIdArgs(args: Arg[]): Arg[] {
  // Single list argument: unroll it, consistent with hasId([1,2,3]) semantics.
  // Multiple arguments: treat each as-is — a list is a single ID value, not unrolled.
  if (args.length === 1 && Array.isArray(args[0])) return args[0] as unknown as Arg[];
  return args;
}

export function* stepHasId(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const flatArgs = flattenIdArgs(args);
  for (const item of source) {
    const el = getValue(item, trackPaths);
    const id = el?.id;
    if (flatArgs.length > 0 && flatArgs[0] instanceof P) {
      if (evaluatePredicate(flatArgs[0] as P, id)) yield item;
    } else if (flatArgs.some(a => idMatches(id, a))) {
      yield item;
    }
  }
}

export function* stepHasLabel(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const el = getValue(item, trackPaths);
    const label = el?.label;
    if (args.length > 0 && args[0] instanceof P) {
      if (evaluatePredicate(args[0] as P, label)) yield item;
    } else if (args.length === 0 || args.includes(label)) {
      yield item;
    }
  }
}

export function* stepHasNot(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const key = args[0] as string;
  for (const item of source) {
    const el = getValue(item, trackPaths);
    if (el instanceof Vertex || el instanceof Edge) {
      if (!hasKey(el, key)) yield item;
    }
  }
}

export function* stepIs(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const arg = args[0];
  for (const item of source) {
    const v = getValue(item, trackPaths);
    if (arg instanceof P || arg instanceof TextP) {
      if (evaluatePredicate(arg, v)) yield item;
    } else if (v === arg) {
      yield item;
    }
  }
}

export function* stepHasKey(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const p = getValue(item, trackPaths);
    const k = propKey(p);
    if (args.length === 0) {
      yield item;
    } else if (args[0] instanceof P) {
      if (evaluatePredicate(args[0] as P, k)) yield item;
    } else if (args.includes(k)) {
      yield item;
    }
  }
}

export function* stepHasValue(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const p = getValue(item, trackPaths);
    const v = p?.value;
    if (args.length === 0) {
      yield item;
    } else if (args[0] instanceof P) {
      if (evaluatePredicate(args[0] as P, v)) yield item;
    } else if (args.some(a => a == v)) {
      yield item;
    }
  }
}

// ── Value extraction steps ────────────────────────────────────────────────────

export function* stepId(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const el = getValue(item, trackPaths);
    yield wrap(el?.id, getPath(item, trackPaths), trackPaths);
  }
}

export function* stepLabel(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const el = getValue(item, trackPaths);
    yield wrap(el?.label, getPath(item, trackPaths), trackPaths);
  }
}

export function* stepValue(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const p = getValue(item, trackPaths);
    yield wrap(p?.value, getPath(item, trackPaths), trackPaths);
  }
}

export function* stepKey(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  for (const item of source) {
    const p = getValue(item, trackPaths);
    yield wrap(propKey(p), getPath(item, trackPaths), trackPaths);
  }
}

export function* stepValues(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const keys = args as string[];
  for (const item of source) {
    const el = getValue(item, trackPaths);
    const props = allProps(el);
    for (const p of props) {
      if (keys.length === 0 || keys.includes(propKey(p))) {
        yield wrap((p as any).value, getPath(item, trackPaths), trackPaths);
      }
    }
  }
}

export function* stepProperties(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const keys = args as string[];
  for (const item of source) {
    const el = getValue(item, trackPaths);
    const props = allProps(el);
    for (const p of props) {
      if (keys.length === 0 || keys.includes(propKey(p))) {
        yield wrap(p, getPath(item, trackPaths), trackPaths);
      }
    }
  }
}

export function* stepValueMap(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  // A boolean true as the first arg means includeTokens (valueMap(true, ...))
  const includeTokens = args.length > 0 && args[0] === true;
  const keys = (includeTokens ? args.slice(1) : args) as string[];
  for (const item of source) {
    const el = getValue(item, trackPaths);
    const map = new Map<any, any[]>();
    if (includeTokens) {
      map.set(t.id, el?.id);
      map.set(t.label, el?.label);
    }
    for (const p of allProps(el)) {
      const k = propKey(p);
      if (keys.length === 0 || keys.includes(k)) {
        if (!map.has(k)) map.set(k, []);
        map.get(k)!.push((p as any).value);
      }
    }
    yield wrap(map, getPath(item, trackPaths), trackPaths);
  }
}

/** Builds the stub vertex map used as Direction.OUT / Direction.IN values in edge elementMap. */
function vertexStubMap(v: Vertex): Map<any, any> {
  const m = new Map<any, any>();
  m.set(t.id, v.id);
  m.set(t.label, v.label);
  return m;
}

export function* stepElementMap(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const keys = args as string[];
  for (const item of source) {
    const el = getValue(item, trackPaths);
    const map = new Map<any, any>();
    map.set(t.id, el?.id);
    map.set(t.label, el?.label);
    if (el instanceof Edge) {
      // Standard Gremlin edge elementMap includes Direction.OUT and Direction.IN stub maps
      map.set(direction.out, vertexStubMap(el.outV));
      map.set(direction.in, vertexStubMap(el.inV));
    }
    for (const p of allProps(el)) {
      const k = propKey(p);
      if (keys.length === 0 || keys.includes(k)) {
        map.set(k, (p as any).value);
      }
    }
    yield wrap(map, getPath(item, trackPaths), trackPaths);
  }
}

// ── Path step ─────────────────────────────────────────────────────────────────

/**
 * Sentinel signalling a non-productive by() projection (e.g. by('age') on an element
 * lacking the 'age' property). Without ProductiveByStrategy — which Tiny Gremlin does not
 * support — a non-productive by() filters the whole path traverser.
 */
export const NON_PRODUCTIVE = Symbol('nonProductive');

/** by(String)/by(T)/natural projection. Returns NON_PRODUCTIVE for an absent property. */
function projectByKey(el: any, key: string | null | undefined): any {
  if (key == null) return el;                              // natural / by()
  if (key === 'T.id' || key === 'id') return el?.id;
  if (key === 'T.label' || key === 'label') return el?.label;
  if (el instanceof Vertex || el instanceof Edge) {
    const vals = getValues(el, key);
    return vals.length === 0 ? NON_PRODUCTIVE : vals[0];   // first value, multi-valued
  }
  return el;
}

// projections carry one entry per by() modulator, applied round-robin across path
// elements. An entry is either a key (String/T/null for by(String)/by(T)/natural) or a
// single-step sub-Pipeline (by(Traversal)), run via runTraversal. Empty when no by()
// modulators are present, in which case the raw path objects are used.
export function* stepPath(
  source: Iterable<StreamItem>,
  projections: Arg[],
  trackPaths: boolean,
  runTraversal: (sub: Pipeline, object: any) => any,
): Generator<StreamItem> {
  for (const item of source) {
    const pathEntries: PathEntry[] = getPath(item, trackPaths);
    const labels: string[][] = pathEntries.map(e => e.labels);
    const objects: any[] = [];
    let productive = true;
    for (let idx = 0; idx < pathEntries.length; idx++) {
      const object = pathEntries[idx].object;
      if (projections.length === 0) { objects.push(object); continue; }
      const proj = projections[idx % projections.length];
      const value = Array.isArray(proj)
        ? runTraversal(proj as unknown as Pipeline, object)
        : projectByKey(object, proj as string | null | undefined);
      if (value === NON_PRODUCTIVE) { productive = false; break; }
      objects.push(value);
    }
    if (!productive) continue;   // default (non-ProductiveBy) semantics: filter the path
    const pathObj = new Path(labels, objects);
    // Yield as a traverser so toAsync's trackPaths unwrap produces the Path value correctly.
    yield trackPaths ? { value: pathObj, path: [] } : pathObj;
  }
}

// ── Range steps ───────────────────────────────────────────────────────────────

export function* stepLimit(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const n = args[0] as number;
  let count = 0;
  for (const item of source) {
    if (count >= n) return;
    yield item;
    count++;
  }
}

export function* stepRange(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const lo = args[0] as number;
  const hi = args[1] as number;
  if (lo > hi) throw new Error(`Not a legal range: [${lo}, ${hi}]`);
  let i = 0;
  for (const item of source) {
    if (i >= hi) return;
    if (i >= lo) yield item;
    i++;
  }
}

export function* stepSkip(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const n = args[0] as number;
  let i = 0;
  for (const item of source) {
    if (i++ >= n) yield item;
  }
}

export function* stepTail(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const n = args[0] as number;
  const buf: StreamItem[] = [];
  for (const item of source) buf.push(item);
  yield* buf.slice(Math.max(0, buf.length - n));
}

// ── Gremlin Orderability semantics ───────────────────────────────────────────
// Total order across all types per the priority list in gremlin-semantics.asciidoc.

function gremlinTypeOrder(v: any): number {
  if (v === null || v === undefined) return 1;          // nulltype
  if (typeof v === 'boolean') return 2;                 // Boolean
  if (typeof v === 'number' || typeof v === 'bigint') return 3; // Number
  if (v instanceof Date) return 4;                      // Date
  if (typeof v === 'string') return 5;                  // String
  if (v instanceof Vertex) return 7;                    // Vertex
  if (v instanceof Edge) return 8;                      // Edge
  if (v instanceof VertexProperty) return 9;            // VertexProperty
  if (v instanceof Property) return 10;                 // Property
  if (v instanceof Path) return 11;                     // Path
  if (v instanceof Set) return 12;                      // Set
  if (Array.isArray(v)) return 13;                      // List
  if (v instanceof Map) return 14;                      // Map
  return 15;                                            // Unknown
}

function compareNumeric(a: number, b: number): number {
  // Per orderability: NaN > +Infinity > everything else
  const aNaN = Number.isNaN(a), bNaN = Number.isNaN(b);
  if (aNaN && bNaN) return 0;
  if (aNaN) return 1;
  if (bNaN) return -1;
  return a < b ? -1 : a > b ? 1 : 0;
}

function compareValues(va: any, vb: any): number {
  const ta = gremlinTypeOrder(va);
  const tb = gremlinTypeOrder(vb);
  if (ta !== tb) return ta - tb;

  // Within same type family:
  if (va === null || va === undefined) return 0;
  if (typeof va === 'boolean') return (va ? 1 : 0) - (vb ? 1 : 0);
  if (typeof va === 'number') return compareNumeric(va as number, vb as number);
  if (typeof va === 'bigint') return va < vb ? -1 : va > vb ? 1 : 0;
  if (va instanceof Date) return va.getTime() - (vb as Date).getTime();
  if (typeof va === 'string') return va < vb ? -1 : va > vb ? 1 : 0;
  // Elements: compare by id
  if (va instanceof Vertex || va instanceof Edge || va instanceof VertexProperty) {
    return compareValues((va as any).id, (vb as any).id);
  }
  if (va instanceof Property) {
    const kc = compareValues((va as Property).key, (vb as Property).key);
    return kc !== 0 ? kc : compareValues((va as Property).value, (vb as Property).value);
  }
  if (Array.isArray(va)) {
    const len = Math.min(va.length, (vb as any[]).length);
    for (let i = 0; i < len; i++) {
      const c = compareValues(va[i], (vb as any[])[i]);
      if (c !== 0) return c;
    }
    return va.length - (vb as any[]).length;
  }
  // Unknown: class name then toString()
  const sa = Object.prototype.toString.call(va);
  const sb = Object.prototype.toString.call(vb);
  if (sa !== sb) return sa < sb ? -1 : 1;
  const ts = String(va), tv = String(vb);
  return ts < tv ? -1 : ts > tv ? 1 : 0;
}

// ── Gremlin Comparability semantics ──────────────────────────────────────────
// Used by P.lt/lte/gt/gte: returns null (ERROR) for cross-type or NaN comparisons.

function compareForPredicate(va: any, vb: any): number | null {
  const ta = gremlinTypeOrder(va);
  const tb = gremlinTypeOrder(vb);
  if (ta !== tb) return null; // cross-type: ERROR
  if (va === null || va === undefined) return 0; // null == null
  if (typeof va === 'number') {
    if (Number.isNaN(va) || Number.isNaN(vb)) return null; // NaN: ERROR
    return compareNumeric(va as number, vb as number);
  }
  return compareValues(va, vb);
}

function getByValue(el: any, key: string | null | undefined): any {
  if (key == null) return el;
  if (key === 'T.id' || key === 'id') return el?.id;
  if (key === 'T.label' || key === 'label') return el?.label;
  if (el instanceof Vertex || el instanceof Edge) {
    const vals = getValues(el, key);
    return vals[0]; // first value for multi-valued properties
  }
  return el;
}

export function* stepOrder(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const key = args[0] as string | null | undefined;
  const dir = ((args[1] as string) ?? 'asc').split('.').pop()!.toLowerCase();
  const multiplier = dir === 'desc' ? -1 : 1;
  const buf: StreamItem[] = [...source];
  buf.sort((a, b) => {
    const va = getByValue(getValue(a, trackPaths), key as string | null | undefined);
    const vb = getByValue(getValue(b, trackPaths), key as string | null | undefined);
    return multiplier * compareValues(va, vb);
  });
  yield* buf;
}

// ── Mutation steps ────────────────────────────────────────────────────────────

export function* stepAddV(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const label = (args[0] as string | undefined) ?? 'vertex';
  for (const _item of source) {
    const id = graph.vertices.size;
    const v = new Vertex(id, label, []);
    graph.vertices.set(id, v);
    yield wrap(v, [], trackPaths);
  }
}

export function* stepAddEWithModulators(
  source: Iterable<StreamItem>,
  label: string,
  fromPipeline: Arg[] | null,
  toPipeline: Arg[] | null,
  graph: Graph,
  trackPaths: boolean,
  executeSubPipeline: (pipeline: Arg[], graph: Graph) => any,
): Generator<StreamItem> {
  const fromVertex = fromPipeline
    ? executeSubPipeline(fromPipeline, graph)
    : null;
  const toVertex = toPipeline
    ? executeSubPipeline(toPipeline, graph)
    : null;

  if (!(fromVertex instanceof Vertex) || !(toVertex instanceof Vertex)) {
    throw new Error("The addE step from/to must resolve to a Vertex or the ID of a Vertex present in the graph");
  }

  const id = graph.edges.size;
  const edge = new Edge(id, fromVertex, label, toVertex, []);
  graph.edges.set(id, edge);
  graph._indexEdge(edge);
  yield wrap(edge, [], trackPaths);
}

export function* stepProperty(
  source: Iterable<StreamItem>, args: Arg[], graph: Graph, trackPaths: boolean,
): Generator<StreamItem> {
  const key = args[0] as string;
  const value = args[1];
  for (const item of source) {
    const el = getValue(item, trackPaths);
    if (el instanceof Vertex) {
      // T.label sets the element label directly; T.id sets the id
      if (key === 'T.label' || key === 'label') {
        (el as any).label = value;
      } else if (key === 'T.id' || key === 'id') {
        const oldId = el.id;
        (el as any).id = value;
        graph.vertices.delete(oldId);
        graph.vertices.set(value, el);
      } else {
        const existing = el.properties as VertexProperty[];
        const idx = existing.findIndex(p => p.label === key);
        const vpId = idx >= 0 ? existing[idx].id : existing.length;
        const vp = new VertexProperty(vpId, key, value, []);
        if (idx >= 0) existing[idx] = vp;
        else existing.push(vp);
      }
    } else if (el instanceof Edge) {
      const existing = el.properties as Property[];
      const idx = existing.findIndex(p => p.key === key);
      const newProp = new Property(key, value);
      if (idx >= 0) (existing as any)[idx] = newProp;
      else existing.push(newProp);
    }
    yield item;
  }
}

// ── Loops step ────────────────────────────────────────────────────────────────

/**
 * Replaces each traverser's value with the loop count of the nearest enclosing repeat()
 * (ctx.loops), typically tested by a following is(), e.g. until(__.loops().is(2)). Zero
 * outside any repeat(). The loops("label") form is rejected — Tiny Gremlin has no step labels.
 */
export function* stepLoops(
  source: Iterable<StreamItem>, args: Arg[], ctx: ExecutionContext,
): Generator<StreamItem> {
  if (args.length > 0) {
    throw new Error('loops("label") is not supported in Tiny Gremlin; step labels are excluded');
  }
  for (const item of source) {
    yield wrap(ctx.loops, getPath(item, ctx.trackPaths), ctx.trackPaths);
  }
}

// ── Repeat step ───────────────────────────────────────────────────────────────

/**
 * Executes repeat() with its folded times()/until()/emit() modulators against a
 * stream. The loop is driven breadth-first over the whole frontier: at each loop
 * depth, every surviving traverser advances in lockstep through one shared run of
 * the body. until() is the exit condition (checked before the body when declared
 * first — while — or after it otherwise — do-while), times() is a loop-count exit,
 * and emit() is a side output that does not stop a traverser from continuing to loop.
 *
 * Driving the body over the entire frontier at once (rather than per input
 * traverser) is what gives barrier steps inside the body — order().by(), and the
 * same for a nested repeat() — their global semantics: order() sees every traverser
 * at that loop position, not just the descendants of a single start. Because the
 * frontier only ever holds traversers that have survived exactly `loops` iterations,
 * the loop count is uniform across the frontier and the per-traverser times()/until()
 * exits remain exact.
 *
 * Child pipelines (the body and any until()/emit() traversal conditions) are run
 * through the ExecutionContext, so a repeat() body may itself contain order().by(),
 * path() projections, or a nested repeat().
 */
export function* stepRepeat(
  source: Iterable<StreamItem>,
  spec: RepeatSpec,
  ctx: ExecutionContext,
): Generator<StreamItem> {
  // until()/emit() conditions are always filter traversals here; the predicate form
  // is rejected during folding (see requireTraversalCondition in LocalExecutor). Each is
  // run through a context carrying the loop count it should observe via loops(), so
  // until(__.loops().is(n)) sees the same counter that times(n) compares against.
  const conditionHolds = (cond: Pipeline, item: StreamItem, condCtx: ExecutionContext): boolean => {
    for (const _out of condCtx.runBranch(cond, [item])) return true; // filter traversal: any output => true
    return false;
  };

  const exitBeforeBody = (item: StreamItem, loops: number, condCtx: ExecutionContext): boolean => {
    if (spec.times != null && spec.timesFirst && loops >= spec.times) return true;
    if (spec.untilFirst && spec.until != null && conditionHolds(spec.until, item, condCtx)) return true;
    return false;
  };
  const exitAfterBody = (item: StreamItem, loops: number, condCtx: ExecutionContext): boolean => {
    if (spec.times != null && !spec.timesFirst && loops >= spec.times) return true;
    if (!spec.untilFirst && spec.until != null && conditionHolds(spec.until, item, condCtx)) return true;
    return false;
  };
  const shouldEmit = (item: StreamItem, condCtx: ExecutionContext): boolean => {
    if (!spec.emitPresent) return false;
    if (spec.emit == null) return true; // bare emit() — emit every traverser
    return conditionHolds(spec.emit, item, condCtx);
  };

  let frontier: StreamItem[] = [...source];
  let loops = 0;
  while (frontier.length > 0) {
    // The body, the while-phase until(), and an emit-before condition all observe the
    // current loop count; the do-while until() and emit-after condition observe the
    // post-body count.
    const curCtx = ctx.withLoops(loops);
    // While-phase: traversers that hit a before-body exit leave the loop now;
    // the rest (optionally emitted ahead of the body) form the body's input.
    const entering: StreamItem[] = [];
    for (const item of frontier) {
      if (exitBeforeBody(item, loops, curCtx)) { yield item; continue; }
      if (spec.emitFirst && shouldEmit(item, curCtx)) yield item;
      entering.push(item);
    }
    if (entering.length === 0) break;

    // Run the body over the entire frontier in one pass so barriers see it all.
    const nextLoops = loops + 1;
    const nextCtx = ctx.withLoops(nextLoops);
    const nextFrontier: StreamItem[] = [];
    for (const out of curCtx.runBranch(spec.body, entering)) {
      if (exitAfterBody(out, nextLoops, nextCtx)) { yield out; continue; }
      if (!spec.emitFirst && shouldEmit(out, nextCtx)) yield out;
      nextFrontier.push(out);
    }
    frontier = nextFrontier;
    loops = nextLoops;
  }
}
