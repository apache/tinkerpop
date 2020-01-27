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
const Translator = require('./lib/process/translator');
const utils = require('./lib/utils');
const DriverRemoteConnection = require('./lib/driver/driver-remote-connection');
const ResponseError = require('./lib/driver/response-error');
const Client = require('./lib/driver/client');
const ResultSet = require('./lib/driver/result-set');
const Authenticator = require('./lib/driver/auth/authenticator');
const PlainTextSaslAuthenticator = require('./lib/driver/auth/plain-text-sasl-authenticator');
const AnonymousTraversalSource = require('./lib/process/anonymous-traversal');

module.exports = {
  driver: {
    RemoteConnection: rc.RemoteConnection,
    RemoteStrategy: rc.RemoteStrategy,
    RemoteTraversal: rc.RemoteTraversal,
    ResponseError,
    DriverRemoteConnection,
    Client,
    ResultSet,
    auth: {
      Authenticator,
      PlainTextSaslAuthenticator
    }
  },
  process: {
    Bytecode,
    EnumValue: t.EnumValue,
    P: t.P,
    TextP: t.TextP,
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
    pick: t.pick,
    pop: t.pop,
    scope: t.scope,
    t: t.t,
    GraphTraversal: gt.GraphTraversal,
    GraphTraversalSource: gt.GraphTraversalSource,
    statics: gt.statics,
    Translator,
    traversal: AnonymousTraversalSource.traversal,
    AnonymousTraversalSource,
    withOptions: t.withOptions
  },
  structure: {
    io: gs,
    Edge: graph.Edge,
    Graph: graph.Graph,
    Path: graph.Path,
    Property: graph.Property,
    Vertex: graph.Vertex,
    VertexProperty: graph.VertexProperty,
    toLong: utils.toLong
  }
};
