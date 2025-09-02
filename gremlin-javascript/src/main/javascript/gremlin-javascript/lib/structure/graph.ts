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
   * @param TraversalSourceClass The constructor to use for the {@code GraphTraversalSource} instance.
   * @deprecated As of release 3.3.5, replaced by the traversal() anonymous function.
   */
  traversal(TraversalSourceClass: typeof GraphTraversalSource = GraphTraversalSource): GraphTraversalSource {
    return new TraversalSourceClass(this, new TraversalStrategies());
  }

  toString() {
    return 'graph[]';
  }
}

class Element<TLabel extends string = string, TId = any> {
  constructor(
    readonly id: TId,
    readonly label: TLabel,
  ) {}

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
    readonly properties?: TVertexProperties,
  ) {
    super(id, label);
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
    readonly properties: TProperties = {} as TProperties,
  ) {
    super(id, label);
    if (properties) {
    if (Array.isArray(properties)) {
      // Handle array of Property objects
      properties.forEach((prop) => {
        // Use type assertion to inform TypeScript that prop.key is a valid key for TProperties
        (this.properties as any)[prop.key] = prop.value;
      });
    } else {
      // Handle object format as before
      Object.keys(properties).forEach((k) => {
        (this.properties as any)[k] = properties[k].value;
      });
    }
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
    readonly properties?: TProperties,
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
 * Represents a walk through a graph as defined by a traversal.
 */
export class Path {
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
