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
import * as utils from '../utils.js';
import Client, { RequestOptions } from './client.js';
import GremlinLang from '../process/gremlin-lang.js';
import { ConnectionOptions } from './connection.js';

/**
 * Represents the default {@link RemoteConnection} implementation.
 */
export default class DriverRemoteConnection extends RemoteConnection {
  private readonly _client: Client;

  /**
   * Creates a new instance of {@link DriverRemoteConnection}.
   * @param {String} url The resource uri.
   * @param {ConnectionOptions} [options] The connection options.
   * @param {Array} [options.ca] Trusted certificates.
   * @param {String|Array|Buffer} [options.cert] The certificate key.
   * @param {String} [options.mimeType] The mime type to use.
   * @param {String|Buffer} [options.pfx] The private key, certificate, and CA certs.
   * @param {GraphSONReader} [options.reader] The reader to use.
   * @param {Boolean} [options.rejectUnauthorized] Determines whether to verify or not the server certificate.
   * @param {String} [options.traversalSource] The traversal source. Defaults to: 'g'.
   * @param {GraphSONWriter} [options.writer] The writer to use.
   * @param {Authenticator} [options.authenticator] The authentication handler to use.
   * @param {Object} [options.headers] An associative array containing the additional header key/values for the initial request.
   * @param {Boolean} [options.enableUserAgentOnConnect] Determines if a user agent will be sent during connection handshake. Defaults to: true
   * @param {http.Agent} [options.agent] The http.Agent implementation to use.
   * @constructor
   */
  constructor(url: string, options: ConnectionOptions = {}) {
    super(url, options);
    this._client = new Client(url, options);
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
    gremlinLang.addG(this.options.traversalSource || 'g');

    let requestOptions: RequestOptions | undefined = undefined;
    const strategies = gremlinLang.getOptionsStrategies();
    if (strategies.length > 0) {
      requestOptions = {};
      const allowedKeys = [
        'evaluationTimeout',
        'scriptEvaluationTimeout',
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

    const params = gremlinLang.getParameters();
    if (params.size > 0) {
      requestOptions.params = Object.fromEntries(params);
    }

    return this._client.submit(gremlinLang.getGremlin(), null, requestOptions)
      .then((result) => new RemoteTraversal(result.toArray()));
  }

  override createSession() {
    if (this.isSessionBound) {
      throw new Error('Connection is already bound to a session - child sessions are not allowed');
    }

    // make sure a fresh session is used when starting a new transaction
    const copiedOptions = Object.assign({}, this.options);
    copiedOptions.session = utils.getUuid();
    return new DriverRemoteConnection(this.url, copiedOptions);
  }

  override get isSessionBound() {
    return Boolean(this.options.session);
  }

  override commit() {
    return this._client.submit('g.tx().commit()', null);
  }

  override rollback() {
    return this._client.submit('g.tx().rollback()', null);
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
