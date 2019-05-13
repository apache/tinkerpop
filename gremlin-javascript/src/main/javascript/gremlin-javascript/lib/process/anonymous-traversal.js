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
   * Creates a new instance of {@link AnonymousTraversalSource}.
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
   * Creates the specified {@link GraphTraversalSource{ binding a {@link RemoteConnection} as its reference such that
   * traversals spawned from it will execute over that reference.
   * @param {GraphTraversalSource} remoteConnection
   * @return {GraphTraversalSource}
   */
  withRemote(remoteConnection) {
    return this.withGraph(new Graph()).withRemote(remoteConnection);
  }

  /**
   * Creates the specified {@link GraphTraversalSource} binding a {@link Graph} as its reference such that traversals
   * spawned from it will execute over that reference.
   * @param {Graph} graph
   * @return {GraphTraversalSource}
   */
  withGraph(graph) {
    return new this.traversalSourceClass(graph, new TraversalStrategies());
  }
}

module.exports = AnonymousTraversalSource;