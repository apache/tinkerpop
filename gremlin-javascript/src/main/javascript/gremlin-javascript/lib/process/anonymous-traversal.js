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

'use strict';

const graphTraversalModule = require('./graph-traversal');
const remote = require('../driver/remote-connection');
const TraversalStrategies = require('./traversal-strategy').TraversalStrategies;
const GraphTraversalSource = graphTraversalModule.GraphTraversalSource;
const GraphTraversal = graphTraversalModule.GraphTraversal;
const Bytecode = require('./bytecode');
const Graph = require('../structure/graph').Graph;

/**
 * Provides a unified way to construct a <code>TraversalSource</code> from the perspective of the traversal. In this
 * syntax the user is creating the source and binding it to a reference which is either an existing <code>Graph</code>
 * instance or a <code>RemoteConnection</code>.
 */
class AnonymousTraversalSource {
  /**
   * Creates a new instance of {@code AnonymousTraversalSource}.
   * @param {Function} [traversalSourceClass] Optional {@code GraphTraversalSource} constructor.
   * @param {Function} [traversalClass] Optional {@code GraphTraversal} constructor.
   */
  constructor(traversalSourceClass, traversalClass) {
    this.traversalSourceClass = traversalSourceClass;
    this.traversalClass = traversalClass;
  }

  /**
   * Constructs an {@code AnonymousTraversalSource} which will then be configured to spawn a
   * {@link GraphTraversalSource}.
   * @param {Function} [traversalSourceClass] Optional {@code GraphTraversalSource} constructor.
   * @param {Function} [traversalClass] Optional {@code GraphTraversalSource} constructor.
   * @returns {AnonymousTraversalSource}.
   */
  static traversal(traversalSourceClass, traversalClass) {
    return new AnonymousTraversalSource(traversalSourceClass || GraphTraversalSource, traversalClass || GraphTraversal);
  }

  /**
   * Creates a {@link GraphTraversalSource} binding a {@link RemoteConnection} to a remote {@link Graph} instances as its
   * reference so that traversals spawned from it will execute over that reference.
   * @param {RemoteConnection} connection
   * @return {GraphTraversalSource}
   */
  with_(connection) {
    const traversalStrategies = new TraversalStrategies();
    traversalStrategies.addStrategy(new remote.RemoteStrategy(connection));
    return new this.traversalSourceClass(
      new Graph(),
      traversalStrategies,
      new Bytecode(),
      this.traversalSourceClass,
      this.traversalClass,
    );
  }

  /**
   * Creates a {@link GraphTraversalSource} binding a {@link RemoteConnection} to a remote {@link Graph} instances as its
   * reference so that traversals spawned from it will execute over that reference.
   * @param {RemoteConnection} remoteConnection
   * @return {GraphTraversalSource}
   * @deprecated As of release 4.0.0, prefer {@link with_}.
   */
  withRemote(remoteConnection) {
    return this.with_(remoteConnection);
  }
}

module.exports = AnonymousTraversalSource;
