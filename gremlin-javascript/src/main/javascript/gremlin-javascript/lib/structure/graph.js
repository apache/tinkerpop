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

var gt = require('../process/graph-traversal');
var t = require('../process/traversal');
var TraversalStrategies = require('../process/traversal-strategy').TraversalStrategies;
var utils = require('../utils');
var inherits = utils.inherits;

function Graph() {

}

/**
 * Returns the graph traversal source.
 * @returns {GraphTraversalSource}
 */
Graph.prototype.traversal = function () {
  return new gt.GraphTraversalSource(this, new TraversalStrategies());
};

Graph.prototype.toString = function () {
  return 'graph[empty]';
};

function Element(id, label) {
  this.id = id;
  this.label = label;
}

/**
 * Compares this instance to another and determines if they can be considered as equal.
 * @param {Element} other
 * @returns {boolean}
 */
Element.prototype.equals = function (other) {
  return (other instanceof Element) && this.id === other.id;
};

function Vertex(id, label, properties) {
  Element.call(this, id, label);
  this.properties = properties;
}

Vertex.prototype.toString = function () {
  return 'v[' + this.id + ']';
};

inherits(Vertex, Element);

function Edge(id, outV, label, inV, properties) {
  Element.call(this, id, label);
  this.outV = outV;
  this.inV = inV;
  this.properties = {};
  (function adaptProperties(self) {
    if (properties) {
      var keys = Object.keys(properties);
      for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        self.properties[k] = properties[k].value;
      }
    }
  })(this);
}

inherits(Edge, Element);

Edge.prototype.toString = function () {
  return 'e[' + this.id + '][' + this.outV.id + '-' + this.label + '->' + this.inV.id + ']';
};

function VertexProperty(id, label, value, properties) {
  Element.call(this, id, label);
  this.value = value;
  this.key = this.label;
  this.properties = properties;
}

inherits(VertexProperty, Element);

VertexProperty.prototype.toString = function () {
  return 'vp[' + this.label + '->' + this.value.substr(0, 20) + ']';
};

function Property(key, value) {
  this.key = key;
  this.value = value;
}

Property.prototype.toString = function () {
  return 'p[' + this.key + '->' + this.value.substr(0, 20) + ']';
};

Property.prototype.equals = function (other) {
  return (other instanceof Property) && this.key === other.key && this.value === other.value;
};

/**
 * Represents a walk through a graph as defined by a traversal.
 * @param {Array} labels
 * @param {Array} objects
 * @constructor
 */
function Path(labels, objects) {
  this.labels = labels;
  this.objects = objects;
}

module.exports = {
  Edge: Edge,
  Graph: Graph,
  Path: Path,
  Property: Property,
  Vertex: Vertex,
  VertexProperty: VertexProperty
};