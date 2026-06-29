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

import { RemoteStrategy } from '../driver/remote-connection.js';
import DriverRemoteConnection from '../driver/driver-remote-connection.js';
import Client from '../driver/client.js';
import GremlinLang from './gremlin-lang.js';
import { GraphTraversalSource } from './graph-traversal.js';
import { TraversalStrategies } from './traversal-strategy.js';

/**
 * Controls an explicit remote transaction. A thin wrapper around a Client that
 * adds transaction lifecycle (begin/commit/rollback/close) and attaches a
 * transactionId to every request.
 *
 * Created via Client.transact() or g.tx(). The traversal source (g alias) is
 * inherited from the Client and cannot be changed.
 *
 * Transactions are short-lived and single-use. After commit or rollback, the
 * transaction ID is invalid and the object cannot be reused.
 *
 * Each traversal on the transaction-bound source must be awaited before
 * submitting the next, to ensure the server receives requests in order.
 *
 * This class is NOT thread-safe. Do not share a Transaction across concurrent
 * async contexts without external synchronization.
 */
export class Transaction {
  private _transactionId?: string;
  private _isOpen = false;
  private _failed = false;
  private readonly _client: Client;

  /**
   * Creates a Transaction. Accepts a Client which provides the connection
   * and traversal source for all transaction requests.
   */
  constructor(client: Client) {
    if (!client) {
      throw new Error('Client is required for transactions');
    }
    this._client = client;
  }

  /**
   * Spawns a GraphTraversalSource that is bound to a remote transaction.
   *
   * begin() is idempotent: calling it while a transaction is already open does not send a
   * second begin to the server and does not throw - it reuses the existing transaction ID and
   * returns a source bound to the same transaction. A transaction is single-use, so calling
   * begin() after it has been closed (commit/rollback/failed begin) throws.
   * @returns {Promise<GraphTraversalSource>}
   */
  async begin(): Promise<GraphTraversalSource> {
    if (this._failed) {
      throw new Error('Transaction is closed and cannot be reused; begin a new transaction');
    }

    // idempotent: if a transaction is already open, reuse the existing transactionId without
    // sending a second begin to the server, and return a source bound to the same transaction
    if (!this._isOpen) {
      let result;
      try {
        result = await this._client.submit('g.tx().begin()', null);
      } catch (e) {
        this._failed = true;
        throw e;
      }

      const resultArray = result.toArray();
      if (!resultArray || resultArray.length === 0) {
        this._failed = true;
        throw new Error('Server did not return transaction ID');
      }

      const resultMap = resultArray[0];
      if (!resultMap || !(resultMap instanceof Map) || !resultMap.get('transactionId')) {
        this._failed = true;
        throw new Error('Server did not return transaction ID in expected format');
      }

      this._transactionId = resultMap.get('transactionId');
      this._isOpen = true;
      this._client.trackTransaction(this);
    }

    // Create a DriverRemoteConnection bound to this transaction. The DRC
    // will automatically attach the transactionId to all requests.
    const txConnection = new DriverRemoteConnection(
      this._client.url,
      this._client.options,
      this._transactionId,
    );

    const strategies = new TraversalStrategies();
    strategies.addStrategy(new RemoteStrategy(txConnection));
    const gtx = new GraphTraversalSource(null as any, strategies, new GremlinLang());
    gtx._boundTransaction = this;
    return gtx;
  }

  /**
   * Commits the transaction.
   * @returns {Promise<void>}
   */
  async commit(): Promise<void> {
    await this.#closeTransaction('g.tx().commit()');
  }

  /**
   * Rolls back the transaction.
   * @returns {Promise<void>}
   */
  async rollback(): Promise<void> {
    await this.#closeTransaction('g.tx().rollback()');
  }

  /**
   * Returns true if transaction is open.
   */
  get isOpen(): boolean {
    return this._isOpen;
  }

  /**
   * Returns the server-generated transaction ID, or undefined if not yet begun.
   */
  get transactionId(): string | undefined {
    return this._transactionId;
  }

  /**
   * Closes the transaction. Default behavior is rollback: partial work is
   * discarded rather than accidentally persisted.
   * @returns {Promise<void>}
   */
  async close(): Promise<void> {
    if (this._isOpen) {
      await this.rollback();
    }
  }

  /**
   * Submits a gremlin-lang string within this transaction. The transactionId
   * is automatically attached. Has the same signature as Client.submit().
   */
  async submit(gremlin: string, bindings?: any, requestOptions?: any): Promise<any> {
    if (!this._isOpen) {
      throw new Error('Transaction is not open');
    }
    const opts = {
      ...requestOptions,
      transactionId: this._transactionId,
    };
    return this._client.submit(gremlin, bindings || null, opts);
  }

  async #closeTransaction(script: string): Promise<void> {
    if (!this._isOpen) {
      throw new Error('Transaction is not open');
    }

    await this._client.submit(script, null, {
      transactionId: this._transactionId
    });

    this._isOpen = false;
    this._failed = true; // Terminal state: transaction cannot be reused
    this._client.untrackTransaction(this);
  }
}
