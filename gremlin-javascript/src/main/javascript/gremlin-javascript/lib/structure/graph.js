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
'use strict';

const gt = require('../process/graph-traversal');
const TraversalStrategies = require('../process/traversal-strategy').TraversalStrategies;

class Graph {
  /**
   * Returns the graph traversal source.
   * @returns {GraphTraversalSource}
   */
  traversal() {
    return new gt.GraphTraversalSource(this, new TraversalStrategies());
  }

  toString() {
    return 'graph[empty]';
  }
}

class Element {
  constructor(id, label) {
    this.id = id;
    this.label = label;
  }

  /**
   * Compares this instance to another and determines if they can be considered as equal.
   * @param {Element} other
   * @returns {boolean}
   */
  equals(other) {
    return (other instanceof Element) && this.id === other.id;
  }
}

class Vertex extends Element {
  constructor(id, label, properties) {
    super(id, label);
    this.properties = properties;
  }

  toString() {
    return 'v[' + this.id + ']';
  }
}

class Edge extends Element {
  constructor(id, outV, label, inV, properties) {
    super(id, label);
    this.outV = outV;
    this.inV = inV;
    this.properties = {};
    if (properties) {
      const keys = Object.keys(properties);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        this.properties[k] = properties[k].value;
      }
    }
  }

  toString() {
    return 'e[' + this.id + '][' + this.outV.id + '-' + this.label + '->' + this.inV.id + ']';
  }
}

class VertexProperty extends Element {
  constructor(id, label, value, properties) {
    super(id, label);
    this.value = value;
    this.key = this.label;
    this.properties = properties;
  }

  toString() {
    return 'vp[' + this.label + '->' + this.value.substr(0, 20) + ']';
  }
}

class Property {
  constructor(key, value) {
    this.key = key;
    this.value = value;
  }

  toString() {
    return 'p[' + this.key + '->' + this.value.substr(0, 20) + ']';
  }

  equals(other) {
    return (other instanceof Property) && this.key === other.key && this.value === other.value;
  }
}

class Path {
  /**
   * Represents a walk through a graph as defined by a traversal.
   * @param {Array} labels
   * @param {Array} objects
   * @constructor
   */
  constructor(labels, objects) {
    this.labels = labels;
    this.objects = objects;
  }

  equals(other) {
    if (!(other instanceof Path)) {
      return false;
    }
    if (other === this) {
      return true;
    }
    return areEqual(this.objects, other.objects) && areEqual(this.labels, other.labels);
  }
}

function areEqual(obj1, obj2) {
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
      if (!areEqual(obj1[i], obj2[i])){
        return false;
      }
    }
    return true;
  }
  return false;
}

module.exports = {
  Edge: Edge,
  Graph: Graph,
  Path: Path,
  Property: Property,
  Vertex: Vertex,
  VertexProperty: VertexProperty
};