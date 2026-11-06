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

import {
  Barrier,
  Cardinality,
  Column,
  Direction,
  DT,
  EnumValue,
  GType,
  IO,
  Merge,
  Operator,
  Order,
  P,
  Pick,
  Pop,
  Scope,
  T,
  TextP,
  Traversal,
  TraversalSideEffects,
  Traverser,
  WithOptions,
} from './process/traversal.js';
import { __, CardinalityValue, GraphTraversal, GraphTraversalSource } from './process/graph-traversal.js';
import * as strategies from './process/traversal-strategy.js';
import {
  CompositePDT,
  Edge,
  Graph,
  Path,
  PrimitivePDT,
  Property,
  Tree,
  Vertex,
  VertexProperty,
} from './structure/graph.js';
import { PDTRegistry } from './structure/PDTRegistry.js';
import { RemoteConnection, RemoteStrategy, RemoteTraversal } from './driver/remote-connection.js';
import GremlinLang from './process/gremlin-lang.js';
import { GValue } from './process/gvalue.js';
import {
  Byte,
  Double,
  Float,
  Int,
  Long,
  Short,
  toByte,
  toDouble,
  toFloat,
  toInt,
  toLong,
  toShort,
  unwrap,
} from './utils.js';
import DriverRemoteConnection from './driver/driver-remote-connection.js';
import ResponseError from './driver/response-error.js';
import Client from './driver/client.js';
import ResultSet from './driver/result-set.js';
import { HttpRequest } from './driver/http-request.js';
import { basic, sigv4 } from './driver/auth.js';
import AnonymousTraversalSource from './process/anonymous-traversal.js';

const traversal = AnonymousTraversalSource.traversal;
const auth = { basic, sigv4 };

// Every public symbol is available as a named export of the package, e.g.
// import { traversal, __, P, T, Order, DriverRemoteConnection } from 'gremlin';
// and is also grouped under the driver, process and structure namespaces below.
export * from './process/traversal-strategy.js';
export {
  // driver
  RemoteConnection,
  RemoteStrategy,
  RemoteTraversal,
  ResponseError,
  DriverRemoteConnection,
  Client,
  ResultSet,
  HttpRequest,
  auth,
  // process
  __,
  AnonymousTraversalSource,
  Barrier,
  Cardinality,
  CardinalityValue,
  Column,
  Direction,
  DT,
  EnumValue,
  GraphTraversal,
  GraphTraversalSource,
  GremlinLang,
  GType,
  GValue,
  IO,
  Merge,
  Operator,
  Order,
  P,
  Pick,
  Pop,
  Scope,
  T,
  TextP,
  Traversal,
  TraversalSideEffects,
  Traverser,
  traversal,
  WithOptions,
  // structure
  CompositePDT,
  Edge,
  Graph,
  Path,
  PDTRegistry,
  PrimitivePDT,
  Property,
  Tree,
  Vertex,
  VertexProperty,
  Byte,
  Double,
  Float,
  Int,
  Long,
  Short,
  toByte,
  toDouble,
  toFloat,
  toInt,
  toLong,
  toShort,
  unwrap,
};

export const driver = {
  RemoteConnection,
  RemoteStrategy,
  RemoteTraversal,
  ResponseError,
  DriverRemoteConnection,
  Client,
  ResultSet,
  HttpRequest,
  auth,
};

const processBase = {
  __,
  AnonymousTraversalSource,
  Barrier,
  Cardinality,
  CardinalityValue,
  Column,
  Direction,
  DT,
  EnumValue,
  GraphTraversal,
  GraphTraversalSource,
  GremlinLang,
  GType,
  GValue,
  IO,
  Merge,
  Operator,
  Order,
  P,
  Pick,
  Pop,
  Scope,
  T,
  TextP,
  Traversal,
  TraversalSideEffects,
  Traverser,
  traversal,
  WithOptions,
};

// the explicit type keeps typedoc from expanding the spread strategy module, which it cannot render
/** @inline */
type ProcessNamespace = typeof processBase & typeof strategies;

export const process: ProcessNamespace = { ...processBase, ...strategies };

export const structure = {
  CompositePDT,
  Edge,
  Graph,
  Path,
  PDTRegistry,
  PrimitivePDT,
  Property,
  Tree,
  Vertex,
  VertexProperty,
  Byte,
  Double,
  Float,
  Int,
  Long,
  Short,
  toByte,
  toDouble,
  toFloat,
  toInt,
  toLong,
  toShort,
  unwrap,
};

// the default export carries the same flat names as the named exports, alongside the namespaces
export default { ...driver, ...process, ...structure, driver, process, structure };
