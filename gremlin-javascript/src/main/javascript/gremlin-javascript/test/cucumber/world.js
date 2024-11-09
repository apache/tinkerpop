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

const {setWorldConstructor, Before, BeforeAll, AfterAll} = require('cucumber');
const helper = require('../helper');
const traversal = require('../../lib/process/anonymous-traversal').traversal;
const graphTraversalModule = require('../../lib/process/graph-traversal');
const __ = graphTraversalModule.statics;

const cache = {};

function TinkerPopWorld(){
  this.scenario = null;
  this.g = null;
  this.traversal = null;
  this.result = null;
  this.cache = null;
  this.graphName = null;
  this.parameters = {};
  this.isGraphComputer = false;
}

TinkerPopWorld.prototype.getData = function () {
  if (!this.graphName) {
    throw new Error("Graph name is not set");
  }
  return this.cache[this.graphName];
};

TinkerPopWorld.prototype.cleanEmptyGraph = function () {
  const connection = this.cache['empty'].connection;
  const g = traversal().withRemote(connection);
  return g.V().drop().toList();
};

TinkerPopWorld.prototype.loadEmptyGraphData = function () {
  const cacheData = this.cache['empty'];
  const c = cacheData.connection;
  return Promise.all([ getVertices(c), getEdges(c), getVertexProperties(c) ]).then(values => {
    cacheData.vertices = values[0];
    cacheData.edges = values[1];
    cacheData.vertexProperties = values[2]
  });
};

setWorldConstructor(TinkerPopWorld);

BeforeAll(function () {
  // load all traversals
  const promises = ['modern', 'classic', 'crew', 'grateful', 'sink', 'empty'].map(graphName => {
    let connection = null;
    if (graphName === 'empty') {
      connection = helper.getConnection('ggraph');
      return connection.open().then(() => cache['empty'] = { connection: connection });
    }
    connection = helper.getConnection('g' + graphName);
    return connection.open()
      .then(() => Promise.all([getVertices(connection), getEdges(connection), getVertexProperties(connection)]))
      .then(values => {
        cache[graphName] = {
          connection: connection,
          vertices: values[0],
          edges: values[1],
          vertexProperties: values[2]
        };
      });
  });
  return Promise.all(promises);
});

AfterAll(function () {
  return Promise.all(Object.keys(cache).map(graphName => cache[graphName].connection.close()));
});

Before(function (info) {
  this.scenario = info.pickle.name;
  this.cache = cache;
});

Before({tags: "@GraphComputerOnly"}, function() {
  this.isGraphComputer = true;
})

Before({tags: "@AllowNullPropertyValues"}, function() {
  return 'skipped'
})

function getVertices(connection) {
  const g = traversal().withRemote(connection);
  return g.V().group().by('name').by(__.tail()).next().then(it => {
    // properties excluded from verification
    if (it.value instanceof Map) {
      for (const value of it.value.values()) {
        value.properties = undefined
      }
    }
    return it.value
  });
}

function getEdges(connection) {
  const g = traversal().withRemote(connection);
  return g.E().group()
    .by(__.project("o", "l", "i").by(__.outV().values("name")).by(__.label()).by(__.inV().values("name")))
    .by(__.tail())
    .next()
    .then(it => {
      const edges = {};
      it.value.forEach((v, k) => {
        // properties excluded from verification
        v.properties = undefined
        edges[getEdgeKey(k)] = v;
      });
      return edges;
    });
}

function getVertexProperties(connection) {
  const g = traversal().withRemote(connection);
  return g.V().properties()
      .group()
      .by(__.project("n", "k", "v").by(__.element().values("name")).by(__.key()).by(__.value()))
      .by(__.tail())
      .next()
      .then(it => {
        const vps = {};
        it.value.forEach((v, k) => {
          vps[getVertexPropertyKey(k)] = v;
        });
        return vps;
      });
}

function getEdgeKey(key) {
  // key is a map
  return key.get('o') + "-" + key.get('l') + "->" + key.get('i');
}

function getVertexPropertyKey(key) {
  // key is a map
  const k = key.get('k');

  // hardcoding the types as we don't have a good dynamic way to get them. python does this with a lambda, but
  // trying it this way in javascript to see if this pattern is better in an attempt to avoid lambdas. right now
  // we'd really just have problems with the classic graph which defines "weight" as "float" and here we hard code
  // for the modern graph which uses "double". we really don't test classic with gherkin so for now it is probably
  // fine
  let val = key.get('v');
  if (k === 'weight') {
    val = 'd[' + val + '].d' ;
  } else if (k === 'age' || k === 'since' || k === 'skill') {
    val = 'd[' + val + '].i';
  }
  return key.get('n') + "-" + k + "->" + val;
}