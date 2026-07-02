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
 * @author Jorge Bay Gondra
 */

/**
 * An "empty" graph object to server only as a reference.
 *
 * Holds in-memory collections of vertices and edges so that GraphBinary
 * Graph (0x10) deserialization can return a usable data container.
 */
export class Graph {
  readonly vertices: Map<any, Vertex>;
  readonly edges: Map<any, Edge>;

  constructor() {
    this.vertices = new Map();
    this.edges = new Map();
  }

  toString() {
    return `graph[vertices:${this.vertices.size} edges:${this.edges.size}]`;
  }
}

class Element<TLabel extends string = string, TId = any> {
  // properties are stored as list of property objects
  constructor(
    readonly id: TId,
    readonly label: TLabel,
    readonly properties: Property[] = []
  ) {
    this.properties = properties ?? [];
  }

  /**
   * Compares this instance to another and determines if they can be considered as equal.
   */
  equals(other: any): boolean {
    return other instanceof Element && this.id === other.id;
  }
}

export class Vertex<
  TLabel extends string = string,
  TProperties extends Record<string, any> = Record<string, any>,
  TId = number,
  TVertexProperties = {
    [P in keyof TProperties]: P extends string ? VertexProperties<P, TProperties[P]> : never;
  },
> extends Element<TLabel, TId> {
  constructor(
    id: TId,
    label: TLabel,
    properties: Property[] | null = [],
  ) {
    super(id, label, properties ?? []);
  }

  toString() {
    return `v[${this.id}]`;
  }
}

export class Edge<
  TOutVertex extends Vertex = Vertex,
  TLabel extends string = string,
  TInVertex extends Vertex = Vertex,
  TProperties extends Record<string, any> = Record<string, any>,
  TId = number,
> extends Element<TLabel, TId> {
  constructor(
    id: TId,
    readonly outV: TOutVertex,
    readonly label: TLabel,
    readonly inV: TInVertex,
    properties: Property[] | null = [],
  ) {
    super(id, label, properties ?? []);
  }

  toString() {
    const outVId = this.outV ? this.outV.id : '?';
    const inVId = this.inV ? this.inV.id : '?';

    return `e[${this.id}][${outVId}-${this.label}->${inVId}]`;
  }
}

export class VertexProperty<
  TLabel extends string = string,
  TValue = any,
  TProperties extends Record<string, any> | null | undefined = Record<string, any>,
  TId = any,
> extends Element<TLabel, TId> {
  readonly key: string;

  constructor(
    id: TId,
    label: TLabel,
    readonly value: TValue,
    properties: Property[] | null = [],
  ) {
    super(id, label, properties ?? []);
    this.value = value;
    this.key = this.label;
  }

  toString() {
    return `vp[${this.label}->${summarize(this.value)}]`;
  }
}

export type VertexProperties<
  TLabel extends string = string,
  TValue = any,
  TProperties extends Record<string, any> = Record<string, any>,
  TId = any,
> = [VertexProperty<TLabel, TValue, TProperties, TId>, ...Array<VertexProperty<TLabel, TValue, TProperties, TId>>];

export class Property<T = any> {
  constructor(
    readonly key: string,
    readonly value: T,
  ) {}

  toString() {
    return `p[${this.key}->${summarize(this.value)}]`;
  }

  equals(other: any) {
    return other instanceof Property && this.key === other.key && this.value === other.value;
  }
}

/**
 * A tree data structure with a tree-shaped public API.
 *
 * Children are kept in a private, ordered array of {key, value} entries (not a
 * native {@link Map}, whose reference-identity keys would break value-equality).
 * Access is only through the read API; lookups use {@link treeKeysEqual}, which
 * compares keys by value and treats null/undefined as a distinct key.
 */
export class Tree {
  #entries: Array<{ key: any; value: Tree }>;

  constructor(...rootValues: any[]) {
    this.#entries = [];
    for (const value of rootValues) {
      this.#entries.push({ key: value, value: new Tree() });
    }
  }

  /**
   * Locates the entry whose key is structurally equal to the given key.
   * @returns the matching entry, or undefined if none exists.
   */
  private _findEntry(key: any): { key: any; value: Tree } | undefined {
    for (const entry of this.#entries) {
      if (treeKeysEqual(entry.key, key)) {
        return entry;
      }
    }
    return undefined;
  }

  /**
   * Returns the keys at the root of this tree.
   */
  rootNodes(): any[] {
    return this.#entries.map((e) => e.key);
  }

  /**
   * Returns the child subtree for the given key.
   * @throws {Error} if no child exists for the given key.
   */
  childAt(key: any): Tree {
    const entry = this._findEntry(key);
    if (entry === undefined) {
      throw new Error(`Tree has no child for key: ${key}`);
    }
    return entry.value;
  }

  /**
   * Returns true if the given key is an immediate child of this tree.
   */
  hasChild(key: any): boolean {
    return this._findEntry(key) !== undefined;
  }

  /**
   * Returns true if the given value appears as a key anywhere in this tree (recursive).
   */
  contains(value: any): boolean {
    for (const entry of this.#entries) {
      if (treeKeysEqual(entry.key, value)) {
        return true;
      }
    }
    for (const entry of this.#entries) {
      if (entry.value.contains(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Recursively searches the tree for the first subtree rooted at key.
   * @returns the matching subtree, or undefined if absent.
   */
  findSubtree(key: any): Tree | undefined {
    const entry = this._findEntry(key);
    if (entry !== undefined) {
      return entry.value;
    }
    for (const e of this.#entries) {
      const found = e.value.findSubtree(key);
      if (found !== undefined) {
        return found;
      }
    }
    return undefined;
  }

  /**
   * Returns the existing child for key, or inserts and returns a new empty Tree if absent.
   */
  getOrCreateChild(key: any): Tree {
    const entry = this._findEntry(key);
    if (entry !== undefined) {
      return entry.value;
    }
    const child = new Tree();
    this.#entries.push({ key, value: child });
    return child;
  }

  /**
   * Returns true if this tree has no children. An empty tree is considered a leaf.
   */
  isLeaf(): boolean {
    return this.#entries.length === 0;
  }

  /**
   * Returns the total number of nodes (keys) in the tree, counted recursively.
   */
  nodeCount(): number {
    let count = this.#entries.length;
    for (const entry of this.#entries) {
      count += entry.value.nodeCount();
    }
    return count;
  }

  /**
   * Returns the keys at the given depth. Depth 0 returns the root keys.
   * Negative depths or depths beyond the tree's height return an empty array.
   */
  getNodesAtDepth(depth: number): any[] {
    const list: any[] = [];
    for (const t of this.getTreesAtDepth(depth)) {
      for (const entry of t.#entries) {
        list.push(entry.key);
      }
    }
    return list;
  }

  /**
   * Returns the trees at the given depth. Depth 0 returns a singleton list containing this tree.
   * Negative depths or depths beyond the tree's height return an empty array.
   */
  getTreesAtDepth(depth: number): Tree[] {
    if (depth < 0) {
      return [];
    }
    let currentDepth: Tree[] = [this];
    for (let i = 0; i < depth; i++) {
      const next: Tree[] = [];
      for (const t of currentDepth) {
        for (const entry of t.#entries) {
          next.push(entry.value);
        }
      }
      if (next.length === 0) {
        return [];
      }
      currentDepth = next;
    }
    return currentDepth;
  }

  /**
   * Returns all keys whose subtrees are leaves.
   */
  getLeafNodes(): any[] {
    const leaves: any[] = [];
    this._collectLeafKeys(leaves);
    return leaves;
  }

  private _collectLeafKeys(out: any[]): void {
    for (const entry of this.#entries) {
      if (entry.value.isLeaf()) {
        out.push(entry.key);
      } else {
        entry.value._collectLeafKeys(out);
      }
    }
  }

  /**
   * Returns single-key trees representing each leaf key in this tree, preserving the original
   * key-to-empty-subtree mapping.
   */
  getLeafTrees(): Tree[] {
    const leaves: Tree[] = [];
    this._collectLeafTrees(leaves);
    return leaves;
  }

  private _collectLeafTrees(out: Tree[]): void {
    for (const entry of this.#entries) {
      if (entry.value.isLeaf()) {
        const leaf = new Tree();
        leaf.#entries.push({ key: entry.key, value: entry.value });
        out.push(leaf);
      } else {
        entry.value._collectLeafTrees(out);
      }
    }
  }

  /**
   * Recursively merges other into this tree. For overlapping keys, child subtrees are merged in turn.
   * For keys present only in other, the corresponding subtree reference is adopted directly.
   */
  addTree(other: Tree): void {
    for (const entry of other.#entries) {
      const existing = this._findEntry(entry.key);
      if (existing !== undefined) {
        existing.value.addTree(entry.value);
      } else {
        this.#entries.push({ key: entry.key, value: entry.value });
      }
    }
  }

  /**
   * Splits this tree into one tree per root key. If the tree has a single root, returns a singleton list
   * containing this tree.
   */
  splitParents(): Tree[] {
    if (this.#entries.length === 1) {
      return [this];
    }
    const parents: Tree[] = [];
    for (const entry of this.#entries) {
      const parentTree = new Tree();
      parentTree.#entries.push({ key: entry.key, value: entry.value });
      parents.push(parentTree);
    }
    return parents;
  }

  /**
   * Produces a formatted string representation of the tree structure using a |-- ASCII style,
   * Uses a |-- ASCII style with a 3-space indent per level and no trailing newline.
   */
  prettyPrint(): string {
    const lines: string[] = [];
    this._prettyPrint(lines, '');
    return lines.join('\n');
  }

  private _prettyPrint(lines: string[], prefix: string): void {
    for (const entry of this.#entries) {
      lines.push(`${prefix}|--${entry.key}`);
      entry.value._prettyPrint(lines, `${prefix}   `);
    }
  }

  /**
   * Structural recursive equality. Order across siblings is irrelevant; two trees are equal if they have the
   * same set of keys and each key's subtree is recursively equal.
   */
  equals(other: any): boolean {
    if (this === other) {
      return true;
    }
    if (!(other instanceof Tree)) {
      return false;
    }
    if (this.#entries.length !== other.#entries.length) {
      return false;
    }
    for (const entry of this.#entries) {
      const match = other._findEntry(entry.key);
      if (match === undefined) {
        return false;
      }
      if (!entry.value.equals(match.value)) {
        return false;
      }
    }
    return true;
  }

  toString(): string {
    const parts = this.#entries.map((e) => `${e.key}=${e.value.toString()}`);
    return `{${parts.join(', ')}}`;
  }
}

/**
 * Represents a walk through a graph as defined by a traversal.
 */
export class Path {
  constructor(
    readonly labels: string[][],
    readonly objects: any[],
  ) {}

  toString() {
    return `path[${(this.objects || []).join(', ')}]`;
  }

  equals(other: any) {
    if (!(other instanceof Path)) {
      return false;
    }
    if (other === this) {
      return true;
    }
    return areEqual(this.objects, other.objects) && areEqual(this.labels, other.labels);
  }
}

/**
 * Null-safe structural key comparison for Tree keys, built on {@link areEqual}.
 * areEqual dereferences obj.equals, so null/undefined keys are handled here first.
 * Element keys also require matching concrete type (the shared {@link Element.equals}
 * compares by id only), so a Vertex and an Edge with the same id stay distinct keys.
 */
function treeKeysEqual(a: any, b: any): boolean {
  if (a === b) {
    return true;
  }
  if (a === null || a === undefined || b === null || b === undefined) {
    return false;
  }
  if (a instanceof Element && b instanceof Element && a.constructor !== b.constructor) {
    return false;
  }
  return areEqual(a, b);
}

function areEqual(obj1: any, obj2: any) {
  if (obj1 === obj2) {
    return true;
  }
  if (typeof obj1.equals === 'function') {
    return obj1.equals(obj2);
  }
  if (Array.isArray(obj1) && Array.isArray(obj2)) {
    if (obj1.length !== obj2.length) {
      return false;
    }
    for (let i = 0; i < obj1.length; i++) {
      if (!areEqual(obj1[i], obj2[i])) {
        return false;
      }
    }
    return true;
  }
  return false;
}

/**
 * Represents a composite Provider Defined Type (PDT).
 */
export class ProviderDefinedType {
  readonly name: string;
  readonly fields: Readonly<Record<string, any>>;

  constructor(name: string, fields?: Record<string, any>) {
    if (!name) throw new Error('ProviderDefinedType name cannot be null or empty');
    this.name = name;
    this.fields = Object.freeze(fields || {});
  }

  toString() {
    return `pdt[${this.name}]${JSON.stringify(this.fields)}`;
  }
}

function summarize(value: any) {
  if (value === null || value === undefined) {
    return value;
  }

  const strValue = value.toString();
  return strValue.length > 20 ? strValue.substr(0, 20) : strValue;
}
