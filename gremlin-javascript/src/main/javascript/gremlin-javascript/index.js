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

const t = require('./lib/process/traversal');
const gt = require('./lib/process/graph-traversal');
const strategiesModule = require('./lib/process/traversal-strategy');
const graph = require('./lib/structure/graph');
const gs = require('./lib/structure/io/graph-serializer');
const rc = require('./lib/driver/remote-connection');
const Bytecode = require('./lib/process/bytecode');
const utils = require('./lib/utils');
const DriverRemoteConnection = require('./lib/driver/driver-remote-connection');

module.exports = {
  driver: {
    RemoteConnection: rc.RemoteConnection,
    RemoteStrategy: rc.RemoteStrategy,
    RemoteTraversal: rc.RemoteTraversal,
    DriverRemoteConnection: DriverRemoteConnection
  },
  process: {
    Bytecode: Bytecode,
    EnumValue: t.EnumValue,
    P: t.P,
    Traversal: t.Traversal,
    TraversalSideEffects: t.TraversalSideEffects,
    TraversalStrategies: strategiesModule.TraversalStrategies,
    TraversalStrategy: strategiesModule.TraversalStrategy,
    Traverser: t.Traverser,
    barrier: t.barrier,
    cardinality: t.cardinality,
    column: t.column,
    direction: t.direction,
    operator: t.operator,
    order: t.order,
    pop: t.pop,
    scope: t.scope,
    t: t.t,
    GraphTraversal: gt.GraphTraversal,
    GraphTraversalSource: gt.GraphTraversalSource,
    statics: gt.statics
  },
  structure: {
    io: {
      GraphSONReader: gs.GraphSONReader,
      GraphSONWriter: gs.GraphSONWriter
    },
    Edge: graph.Edge,
    Graph: graph.Graph,
    Path: graph.Path,
    Property: graph.Property,
    Vertex: graph.Vertex,
    VertexProperty: graph.VertexProperty,
    toLong: utils.toLong
  }
};