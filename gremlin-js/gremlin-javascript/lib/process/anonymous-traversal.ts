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

import { RemoteConnection, RemoteStrategy } from '../driver/remote-connection.js';
import { LocalGraphConnection } from '../driver/local-graph-connection.js';
import { Graph } from '../structure/graph.js';
import { GraphTraversalSource, GraphTraversal } from './graph-traversal.js';
import { TraversalStrategies } from './traversal-strategy.js';
import GremlinLang from './gremlin-lang.js';

/**
 * Provides a unified way to construct a <code>TraversalSource</code> from the perspective of the traversal. In this
 * syntax the user is creating the source and binding it to a reference which is either an existing <code>Graph</code>
 * instance or a <code>RemoteConnection</code>.
 */
export default class AnonymousTraversalSource {
  /**
   * Creates a new instance of `AnonymousTraversalSource`.
   * @param {Function} [traversalSourceClass] Optional `GraphTraversalSource` constructor.
   * @param {Function} [traversalClass] Optional `GraphTraversal` constructor.
   */
  constructor(
    readonly traversalSourceClass?: typeof GraphTraversalSource,
    readonly traversalClass?: typeof GraphTraversal,
  ) {}

  /**
   * Constructs an `AnonymousTraversalSource` which will then be configured to spawn a
   * {@link GraphTraversalSource}.
   * @param {Function} [traversalSourceClass] Optional `GraphTraversalSource` constructor.
   * @param {Function} [traversalClass] Optional `GraphTraversalSource` constructor.
   * @returns {AnonymousTraversalSource}.
   */
  static traversal(traversalSourceClass?: typeof GraphTraversalSource, traversalClass?: typeof GraphTraversal) {
    return new AnonymousTraversalSource(traversalSourceClass || GraphTraversalSource, traversalClass || GraphTraversal);
  }

  /**
   * Creates a {@link GraphTraversalSource} bound to a connection or a local {@link Graph}.
   * When passed a {@link Graph} instance, execution runs locally via Tiny Gremlin.
   * When passed a {@link RemoteConnection}, traversals are submitted to a remote server.
   * @param {RemoteConnection | Graph} connectionOrGraph
   * @return {GraphTraversalSource}
   */
  with_(connectionOrGraph: RemoteConnection | Graph) {
    const connection = connectionOrGraph instanceof Graph
      ? new LocalGraphConnection(connectionOrGraph)
      : connectionOrGraph;
    const traversalStrategies = new TraversalStrategies();
    traversalStrategies.addStrategy(new RemoteStrategy(connection));
    const gl = new GremlinLang();
    if (connection.options?.pdtRegistry) {
      gl.pdtRegistry = connection.options.pdtRegistry;
    }
    return new this.traversalSourceClass!(
      new Graph(),
      traversalStrategies,
      gl,
      this.traversalSourceClass,
      this.traversalClass,
    );
  }

  /**
   * Creates a {@link GraphTraversalSource} binding a {@link RemoteConnection} to a remote {@link Graph} instances as its
   * reference so that traversals spawned from it will execute over that reference.
   * @param {RemoteConnection} remoteConnection
   * @return {GraphTraversalSource}
   * @deprecated As of release 3.8.0, prefer {@link with_}.
   */
  withRemote(remoteConnection: RemoteConnection) {
    return this.with_(remoteConnection);
  }
}
