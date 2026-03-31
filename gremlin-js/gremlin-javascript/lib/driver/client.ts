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
import { Readable } from 'stream';
import {RequestMessage} from "./request-message.js";

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
  constructor(
    url: string,
    private readonly options: ClientOptions = {},
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
   * Configuration specific to the current request.
   * @typedef {Object} RequestOptions
   * @property {any} bindings - The parameter bindings to apply to the script.
   * @property {String} language - The language of the script to execute. Defaults to 'gremlin-lang'.
   * @property {String} accept - The MIME type expected in the response.
   * @property {Boolean} bulkResults - Indicates whether results should be returned in bulk format.
   * @property {Object} params - Additional parameters to include with the request.
   * @property {Number} batchSize - The size in which the result of a request is to be 'batched' back to the client.
   * @property {String} userAgent - The user agent string to send with the request.
   * @property {Number} evaluationTimeout - The timeout for the evaluation of the request.
   * @property {String} materializeProperties - Indicates whether element properties should be returned or not.
   */

  /**
   * Send a request to the Gremlin Server.
   * @param {string} message The script to send
   * @param {Object|null} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {Promise}
   */ //TODO:: tighten return type to Promise<ResultSet>
  submit(message: string, bindings: any | null, requestOptions?: RequestOptions): Promise<any> {
      const requestBuilder = RequestMessage.build(message)
          .addG(this.options.traversalSource || 'g')

      if (requestOptions?.language) {
          requestBuilder.addLanguage(requestOptions.language);
      }
      if (requestOptions?.bindings) {
          requestBuilder.addBindings(requestOptions.bindings);
      }
      if (bindings) {
          requestBuilder.addBindings(bindings);
      }
      if (requestOptions?.materializeProperties) {
        requestBuilder.addMaterializeProperties(requestOptions.materializeProperties);
      }
      if (requestOptions?.evaluationTimeout) {
          requestBuilder.addTimeoutMillis(requestOptions.evaluationTimeout);
      }
      if (requestOptions?.bulkResults) {
          requestBuilder.addBulkResults(requestOptions.bulkResults);
      }

      return this._connection.submit(requestBuilder.create());
  }

  /**
   * Send a request to the Gremlin Server and receive a stream for the results.
   * @param {string} message The script to send
   * @param {Object} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {ReadableStream}
   */
  //TODO:: Update stream() to mirror submit()
  stream(message: string, bindings: any, requestOptions?: RequestOptions): Readable {
      throw new Error("Stream not yet implemented");
  }

  /**
   * Closes the underlying connection
   * send session close request before connection close if session mode
   * @returns {Promise}
   */
  close(): Promise<void> {
    return this._connection.close();
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
   * Removes a previowsly added event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The event handler to be removed.
   */
  removeListener(event: string, handler: (...args: any[]) => unknown) {
    this._connection.removeListener(event, handler);
  }
}
