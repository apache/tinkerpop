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

const t = require('../process/traversal');
const TraversalStrategy = require('../process/traversal-strategy').TraversalStrategy;

class RemoteConnection {
  constructor(url) {
    this.url = url;
  }

  /**
   * @abstract
   * @param {Bytecode} bytecode
   * @returns {Promise}
   */
  submit(bytecode) {
    throw new Error('submit() was not implemented');
  };
}

class RemoteTraversal extends t.Traversal {
  constructor(traversers, sideEffects) {
    super(null, null, null);
    this.traversers = traversers;
    this.sideEffects = sideEffects;
  }
}

class RemoteStrategy extends TraversalStrategy {
  /**
   * Creates a new instance of RemoteStrategy.
   * @param {RemoteConnection} connection
   */
  constructor(connection) {
    super();
    this.connection = connection;
  }

  /** @override */
  apply(traversal) {
    if (traversal.traversers) {
      return Promise.resolve();
    }
    return this.connection.submit(traversal.getBytecode()).then(function (remoteTraversal) {
      traversal.sideEffects = remoteTraversal.sideEffects;
      traversal.traversers = remoteTraversal.traversers;
    });
  }
}

module.exports = {
  RemoteConnection: RemoteConnection,
  RemoteStrategy: RemoteStrategy,
  RemoteTraversal: RemoteTraversal
};
