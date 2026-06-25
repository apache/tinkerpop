/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  'License'); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import Connection, { ConnectionOptions } from './connection.js';
import {RequestMessage} from "./request-message.js";
import { Transaction } from '../process/transaction.js';

export type RequestOptions = {
  bindings?: any;
  language?: string;
  accept?: string;
  evaluationTimeout?: number;
  batchSize?: number;
  userAgent?: string;
  materializeProperties?: string;
  bulkResults?: boolean;
  params?: Record<string, any>;
  transactionId?: string;
};

export type ClientOptions = ConnectionOptions & RequestOptions & { processor?: string };

/**
 * A {@link Client} contains methods to send messages to a Gremlin Server.
 */
export default class Client {
  private readonly _connection: Connection;

  /**
   * Creates a new instance of {@link Client}.
   * @param {String} url The resource uri.
   * @param {ClientOptions} [options] The connection options.
   */
  // Tracks open transactions for cascade rollback on close. JS Set iteration
  // is safe during concurrent modification (deleting the current element during
  // for-of is spec-guaranteed to not affect remaining iterations).
  private readonly _trackedTransactions = new Set<any>();

  constructor(
    readonly url: string,
    readonly options: ClientOptions = {},
  ) {
    this._connection = new Connection(url, options);
  }

  /**
   * Opens the underlying connection to the Gremlin Server, if it's not already opened.
   * @returns {Promise}
   */
  open(): Promise<void> {
    return this._connection.open();
  }

  /**
   * Returns true if the underlying connection is open
   * @returns {Boolean}
   */
  get isOpen(): boolean {
    return this._connection.isOpen;
  }

  /**
   * Send a request to the Gremlin Server and buffer the entire response.
   * @param {string} message The script to send
   * @param {Object|null} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {Promise<ResultSet>}
   */
  submit(message: string, bindings: any | null, requestOptions?: RequestOptions): Promise<any> {
    return this._connection.submit(this.#buildRequest(message, bindings, requestOptions));
  }

  /**
   * Send a request to the Gremlin Server and stream results incrementally.
   * Returns an AsyncGenerator that yields individual result items as they are
   * deserialized from the response. For bulked responses, yields Traverser objects.
   * @param {string} message The script to send
   * @param {Object|null} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {AsyncGenerator<any>}
   */
  async *stream(message: string, bindings: any | null, requestOptions?: RequestOptions): AsyncGenerator<any> {
    return yield* this._connection.stream(this.#buildRequest(message, bindings, requestOptions));
  }

  #buildRequest(message: string, bindings: any | null, requestOptions?: RequestOptions): RequestMessage {
    const requestBuilder = RequestMessage.build(message)
        .addG(this.options.traversalSource || 'g');

    if (requestOptions?.language) {
      requestBuilder.addLanguage(requestOptions.language);
    }
    if (requestOptions?.bindings) {
          if (typeof requestOptions.bindings === 'string') {
              requestBuilder.addBindingsString(requestOptions.bindings);
          } else {
              requestBuilder.addBindings(requestOptions.bindings);
          }
    }
    if (bindings) {
          if (typeof bindings === 'string') {
              requestBuilder.addBindingsString(bindings);
          } else {
              requestBuilder.addBindings(bindings);
          }
    }
    if (requestOptions?.materializeProperties) {
      requestBuilder.addMaterializeProperties(requestOptions.materializeProperties);
    }
    if (requestOptions?.evaluationTimeout) {
      requestBuilder.addTimeoutMillis(requestOptions.evaluationTimeout);
    }
    // Per-request value wins (including an explicit `false`); otherwise apply the
    // connection-level default only when it is true.
    if (requestOptions?.bulkResults !== undefined) {
      requestBuilder.addBulkResults(requestOptions.bulkResults);
    } else if (this._connection.bulkResults) {
      requestBuilder.addBulkResults(true);
    }
    // Fill the per-request batchSize from the connection-level default when unset.
    const batchSize = requestOptions?.batchSize ?? this._connection.batchSize;
    if (batchSize !== undefined) {
      requestBuilder.addBatchSize(batchSize);
    }
    if (requestOptions?.transactionId) {
      requestBuilder.addField('transactionId', requestOptions.transactionId);
    }

    return requestBuilder.create();
  }

  /**
   * Closes the underlying connection
   * @returns {Promise}
   */
  async close(): Promise<void> {
    // Best-effort rollback of any open transactions before closing connections
    for (const tx of this._trackedTransactions) {
      try { await tx.close(); } catch {}
    }
    this._trackedTransactions.clear();
    return this._connection.close();
  }

  trackTransaction(tx: any): void {
    this._trackedTransactions.add(tx);
  }

  untrackTransaction(tx: any): void {
    this._trackedTransactions.delete(tx);
  }

  /**
   * Creates a new Transaction for executing operations within an explicit transaction.
   * Transactions are short-lived and single-use. After commit or rollback,
   * create a new Transaction for the next unit of work.
   * The traversal source (g alias) is inherited from this Client.
   * @returns {Transaction}
   */
  transact(): Transaction {
    return new Transaction(this);
  }

  /**
   * Adds an event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The callback to be called when the event occurs.
   */
  addListener(event: string, handler: (...args: any[]) => unknown) {
    this._connection.on(event, handler);
  }

  /**
   * Removes a previously added event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The event handler to be removed.
   */
  removeListener(event: string, handler: (...args: any[]) => unknown) {
    this._connection.removeListener(event, handler);
  }
}
