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
import Bytecode from './bytecode.js';
import { GraphTraversalSource } from './graph-traversal.js';
import { TraversalStrategies } from './traversal-strategy.js';

/**
 * A controller for a remote transaction that is constructed from <code>g.tx()</code>. Calling <code>begin()</code>
 * on this object will produce a new <code>GraphTraversalSource</code> that is bound to a remote transaction over which
 * multiple traversals may be executed in that context. Calling <code>commit()</code> or <code>rollback()</code> will
 * then close the transaction and thus, the session. This feature only works with transaction enabled graphs.
 */
export class Transaction {
  private _sessionBasedConnection?: RemoteConnection = undefined;

  constructor(private readonly g: GraphTraversalSource) {}

  /**
   * Spawns a <code>GraphTraversalSource</code> that is bound to a remote session which enables a transaction.
   * @returns {GraphTraversalSource}
   */
  begin(): GraphTraversalSource {
    if (this._sessionBasedConnection) {
      throw new Error('Transaction already started on this object');
    }

    this._sessionBasedConnection = this.g.remoteConnection!.createSession();
    const traversalStrategy = new TraversalStrategies();
    traversalStrategy.addStrategy(new RemoteStrategy(this._sessionBasedConnection));
    return new this.g.graphTraversalSourceClass(
      this.g.graph,
      traversalStrategy,
      new Bytecode(this.g.bytecode),
      this.g.graphTraversalSourceClass,
      this.g.graphTraversalClass,
    );
  }

  /**
   * @returns {Promise}
   */
  commit(): Promise<void> {
    if (!this._sessionBasedConnection) {
      throw new Error('Cannot commit a transaction that is not started');
    }

    return this._sessionBasedConnection.commit().finally(() => this.close());
  }

  /**
   * @returns {Promise}
   */
  rollback(): Promise<void> {
    if (!this._sessionBasedConnection) {
      throw new Error('Cannot rollback a transaction that is not started');
    }

    return this._sessionBasedConnection.rollback().finally(() => this.close());
  }

  /**
   * Returns true if transaction is open.
   * @returns {Boolean}
   */
  get isOpen(): boolean {
    return this._sessionBasedConnection?.isOpen ?? false;
  }

  /**
   * @returns {Promise}
   */
  async close(): Promise<void> {
    if (this._sessionBasedConnection) {
      this._sessionBasedConnection.close();
    }
  }
}
