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

import * as rcModule from './remote-connection.js';
const RemoteConnection = rcModule.RemoteConnection;
const RemoteTraversal = rcModule.RemoteTraversal;
import Client, { RequestOptions } from './client.js';
import GremlinLang from '../process/gremlin-lang.js';
import { ConnectionOptions } from './connection.js';

/**
 * Represents the default `RemoteConnection` implementation.
 */
export default class DriverRemoteConnection extends RemoteConnection {
  // Exposed as package-internal for Transaction to submit begin/commit/rollback.
  // Not part of the public API contract.
  readonly _client: Client;
  private _transactionId?: string;

  /**
   * Creates a new instance of {@link DriverRemoteConnection}.
   * @param {String} url The resource uri.
   * @param {ConnectionOptions} [options] The connection options.
   * @param {String} [transactionId] Optional transaction ID to attach to all requests.
   */
  constructor(url: string, options: ConnectionOptions = {}, transactionId?: string) {
    super(url, options);
    this._client = new Client(url, options);
    this._transactionId = transactionId;
  }

  /**
   * Returns the transaction ID if this connection is bound to a transaction, undefined otherwise.
   */
  get transactionId(): string | undefined {
    return this._transactionId;
  }

  /** @override */
  open() {
    return this._client.open();
  }

  /** @override */
  get isOpen() {
    return this._client.isOpen;
  }

  /** @override */
  submit(gremlinLang: GremlinLang) {
    const { gremlin, requestOptions } = this.#buildRequestArgs(gremlinLang);

    // Use streaming internally — returns an AsyncGenerator backed RemoteTraversal
    const generator = this._client.stream(gremlin, null, requestOptions);
    return Promise.resolve(new RemoteTraversal(generator));
  }

  #buildRequestArgs(gremlinLang: GremlinLang) {
    gremlinLang.addG(this.options.traversalSource || 'g');

    let requestOptions: RequestOptions | undefined = undefined;
    const strategies = gremlinLang.getOptionsStrategies();
    if (strategies.length > 0) {
      requestOptions = {};
      const allowedKeys = [
        'timeoutMs',
        'batchSize',
        'requestId',
        'userAgent',
        'materializeProperties',
        'bulkResults',
      ];
      for (const strategy of strategies) {
        for (const key in strategy.configuration) {
          if (allowedKeys.indexOf(key) > -1) {
            requestOptions[key as keyof RequestOptions] = strategy.configuration[key];
          }
        }
      }
    }

    if (!requestOptions) {
      requestOptions = {};
    }
    if (!('bulkResults' in requestOptions)) {
      requestOptions.bulkResults = true;
    }

    const parametersString = gremlinLang.getParametersAsString();
    if (parametersString !== '[:]') {
      requestOptions.parameters = parametersString;
    }

    // If this connection is bound to a transaction, attach the transactionId
    if (this._transactionId) {
      requestOptions.transactionId = this._transactionId;
    }

    return { gremlin: gremlinLang.getGremlin(), requestOptions };
  }

  override close() {
    return this._client.close();
  }

  /** @override */
  addListener(event: string, handler: (...args: any[]) => unknown) {
    return this._client.addListener(event, handler);
  }

  /** @override */
  removeListener(event: string, handler: (...args: any[]) => unknown) {
    return this._client.removeListener(event, handler);
  }
}
