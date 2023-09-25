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

const remote = require('../driver/remote-connection');
const Bytecode = require('./bytecode');
const { TraversalStrategies } = require('./traversal-strategy');

/**
 * A controller for a remote transaction that is constructed from <code>g.tx()</code>. Calling <code>begin()</code>
 * on this object will produce a new <code>GraphTraversalSource</code> that is bound to a remote transaction over which
 * multiple traversals may be executed in that context. Calling <code>commit()</code> or <code>rollback()</code> will
 * then close the transaction and thus, the session. This feature only works with transaction enabled graphs.
 */
class Transaction {
  constructor(g) {
    this._g = g;
    this._sessionBasedConnection = undefined;
  }

  /**
   * Spawns a <code>GraphTraversalSource</code> that is bound to a remote session which enables a transaction.
   * @returns {*}
   */
  begin() {
    if (this._sessionBasedConnection) {
      throw new Error('Transaction already started on this object');
    }

    this._sessionBasedConnection = this._g.remoteConnection.createSession();
    const traversalStrategy = new TraversalStrategies();
    traversalStrategy.addStrategy(new remote.RemoteStrategy(this._sessionBasedConnection));
    return new this._g.graphTraversalSourceClass(
      this._g.graph,
      traversalStrategy,
      new Bytecode(this._g.bytecode),
      this._g.graphTraversalSourceClass,
      this._g.graphTraversalClass,
    );
  }

  /**
   * @returns {Promise}
   */
  commit() {
    return this._sessionBasedConnection.commit().then(() => this.close());
  }

  /**
   * @returns {Promise}
   */
  rollback() {
    return this._sessionBasedConnection.rollback().then(() => this.close());
  }

  /**
   * Returns true if transaction is open.
   * @returns {Boolean}
   */
  get isOpen() {
    return this._sessionBasedConnection.isOpen;
  }

  /**
   * @returns {Promise}
   */
  close() {
    if (this._sessionBasedConnection) {
      this._sessionBasedConnection.close();
    }
  }
}

module.exports = {
  Transaction,
};
