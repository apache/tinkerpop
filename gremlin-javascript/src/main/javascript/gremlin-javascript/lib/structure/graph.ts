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

import { GraphTraversalSource } from '../process/graph-traversal.js';
import { TraversalStrategies } from '../process/traversal-strategy.js';

/**
 * An "empty" graph object to server only as a reference.
 */
export class Graph {
  /**
   * Returns the graph traversal source.
   * @param {Function} [traversalSourceClass] The constructor to use for the {@code GraphTraversalSource} instance.
   * @returns {GraphTraversalSource}
   * @deprecated As of release 3.3.5, replaced by the traversal() anonymous function.
   */
  traversal(TraversalSourceClass: typeof GraphTraversalSource = GraphTraversalSource) {
    return new TraversalSourceClass(this, new TraversalStrategies());
  }

  toString() {
    return 'graph[]';
  }
}

class Element {
  constructor(
    readonly id: string,
    readonly label: string,
  ) {}

  /**
   * Compares this instance to another and determines if they can be considered as equal.
   * @param {Element} other
   * @returns {boolean}
   */
  equals(other: any) {
    return other instanceof Element && this.id === other.id;
  }
}

export class Vertex<T extends Record<string, any> = Record<string, any>> extends Element {
  constructor(
    id: string,
    label: string,
    readonly properties?: T,
  ) {
    super(id, label);
  }

  toString() {
    return `v[${this.id}]`;
  }
}

export class Edge<T extends Record<string, any> = Record<string, any>> extends Element {
  constructor(
    id: string,
    readonly outV: Element,
    label: string,
    readonly inV: Element,
    readonly properties: T = {} as T,
  ) {
    super(id, label);
    if (properties) {
      const keys = Object.keys(properties);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        this.properties[k as keyof T] = properties[k].value;
      }
    }
  }

  toString() {
    const outVId = this.outV ? this.outV.id : '?';
    const inVId = this.inV ? this.inV.id : '?';

    return `e[${this.id}][${outVId}-${this.label}->${inVId}]`;
  }
}

export class VertexProperty<T1 = any, T2 = any> extends Element {
  readonly key: string;

  constructor(
    id: string,
    label: string,
    readonly value: T1,
    readonly properties: T2,
  ) {
    super(id, label);
    this.value = value;
    this.key = this.label;
    this.properties = properties;
  }

  toString() {
    return `vp[${this.label}->${summarize(this.value)}]`;
  }
}

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

export class Path {
  /**
   * Represents a walk through a graph as defined by a traversal.
   * @param {Array} labels
   * @param {Array} objects
   * @constructor
   */
  constructor(
    readonly labels: string[],
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

function summarize(value: any) {
  if (value === null || value === undefined) {
    return value;
  }

  const strValue = value.toString();
  return strValue.length > 20 ? strValue.substr(0, 20) : strValue;
}
