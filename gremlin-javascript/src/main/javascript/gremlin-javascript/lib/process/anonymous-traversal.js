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
const TraversalStrategies = require('./traversal-strategy').TraversalStrategies;
const GraphTraversalSource = graphTraversalModule.GraphTraversalSource;
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
   */
  constructor(traversalSourceClass) {
    this.traversalSourceClass = traversalSourceClass;
  }

  /**
   * Constructs an {@code AnonymousTraversalSource} which will then be configured to spawn a
   * {@link GraphTraversalSource}.
   * @param {Function} [traversalSourceClass] Optional {@code GraphTraversalSource} constructor.
   * @returns {AnonymousTraversalSource}.
   */
  static traversal(traversalSourceClass) {
    return new AnonymousTraversalSource(traversalSourceClass || GraphTraversalSource);
  }

  /**
   * Creates a {@link GraphTraversalSource} binding a {@link RemoteConnection} to a remote {@link Graph} instances as its
   * reference so that traversals spawned from it will execute over that reference.
   * @param {GraphTraversalSource} remoteConnection
   * @return {GraphTraversalSource}
   */
  withRemote(remoteConnection) {
    return this.withGraph(new Graph()).withRemote(remoteConnection);
  }

  /**
   * Creates the specified {@link GraphTraversalSource} binding an embedded {@link Graph} as its reference such that
   * traversals spawned from it will execute over that reference. As there are no "embedded Graph" instances in
   * gremlin-javascript as there on the JVM, the {@link GraphTraversalSource} can only ever be constructed as "empty"
   * with a {@link Graph} instance (which is only a reference to a graph and is not capable of holding data). As a
   * result, the {@link GraphTraversalSource} will do nothing unless a "remote" is then assigned to it later.
   * @param {Graph} graph
   * @return {GraphTraversalSource}
   * @deprecated As of release 3.4.9, prefer {@link withRemote} until some form of "embedded graph" becomes available
   * at which point there will be support for {@code withEmbedded} which is part of the canonical Java API.
   */
  withGraph(graph) {
    return new this.traversalSourceClass(graph, new TraversalStrategies());
  }
}

module.exports = AnonymousTraversalSource;