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

import * as t from './process/traversal.js';
import * as gt from './process/graph-traversal.js';
import * as strategiesModule from './process/traversal-strategy.js';
import * as graph from './structure/graph.js';
import * as gs from './structure/io/graph-serializer.js';
import * as rc from './driver/remote-connection.js';
import Bytecode from './process/bytecode.js';
import Translator from './process/translator.js';
import * as utils from './utils.js';
import DriverRemoteConnection from './driver/driver-remote-connection.js';
import ResponseError from './driver/response-error.js';
import Client from './driver/client.js';
import ResultSet from './driver/result-set.js';
import Authenticator from './driver/auth/authenticator.js';
import PlainTextSaslAuthenticator from './driver/auth/plain-text-sasl-authenticator.js';
import AnonymousTraversalSource from './process/anonymous-traversal.js';

export const driver = {
  RemoteConnection: rc.RemoteConnection,
  RemoteStrategy: rc.RemoteStrategy,
  RemoteTraversal: rc.RemoteTraversal,
  ResponseError,
  DriverRemoteConnection,
  Client,
  ResultSet,
  auth: {
    Authenticator,
    PlainTextSaslAuthenticator,
  },
};

export const process = {
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
  dt: t.dt,
  merge: t.merge,
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
  withOptions: t.withOptions,
};

export const structure = {
  io: gs,
  Edge: graph.Edge,
  Graph: graph.Graph,
  Path: graph.Path,
  Property: graph.Property,
  Vertex: graph.Vertex,
  VertexProperty: graph.VertexProperty,
  toLong: utils.toLong,
};
