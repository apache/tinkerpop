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

/**
 * Represents an abstraction of a "connection" to a "server" that is capable of processing a traversal and
 * returning results.
 */
class RemoteConnection {

  /**
   * @param {String} url The resource uri.
   * @param {Object} [options] The connection options.
   */
  constructor(url, options = {}) {
    this.url = url;
    this.options = options;
  }

  /**
   * Opens the connection, if its not already opened.
   * @returns {Promise}
   */
  open() {
    throw new Error('open() must be implemented');
  }

  /**
   * Returns true if connection is open
   * @returns {Boolean}
   */
  get isOpen() {
    throw new Error('isOpen() must be implemented');
  }

  /**
   * Determines if the connection is already bound to a session. If so, this indicates that the
   * <code>#createSession()</code> cannot be called so as to produce child sessions.
   * @returns {boolean}
   */
  get isSessionBound() {
    return false;
  }

  /**
   * Submits the <code>Bytecode</code> provided and returns a <code>RemoteTraversal</code>.
   * @abstract
   * @param {Bytecode} bytecode
   * @returns {Promise} Returns a <code>Promise</code> that resolves to a <code>RemoteTraversal</code>.
   */
  submit(bytecode) {
    throw new Error('submit() must be implemented');
  };

  /**
   * Create a new <code>RemoteConnection</code> that is bound to a session using the configuration from this one.
   * If the connection is already session bound then this function should throw an exception.
   * @returns {RemoteConnection}
   */
  createSession() {
    throw new Error('createSession() must be implemented');
  }

  /**
   * Submits a <code>Bytecode.GraphOp.commit</code> to the server and closes the connection.
   * @returns {Promise}
   */
  commit() {
    throw new Error('commit() must be implemented');
  }
  /**
   * Submits a <code>Bytecode.GraphOp.rollback</code> to the server and closes the connection.
   * @returns {Promise}
   */
  rollback() {
    throw new Error('rollback() must be implemented');
  }

  /**
   * Closes the connection where open transactions will close according to the features of the graph provider.
   * @returns {Promise}
   */
  close() {
    throw new Error('close() must be implemented');
  }
}

/**
 * Represents a traversal as a result of a {@link RemoteConnection} submission.
 */
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
    // gave this a fqcn that has a local "js:" prefix since this strategy isn't sent as bytecode to the server.
    // this is a sort of local-only strategy that actually executes client side. not sure if this prefix is the
    // right way to name this or not, but it should have a name to identify it.
    super("js:RemoteStrategy");
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

module.exports = { RemoteConnection, RemoteStrategy, RemoteTraversal };
