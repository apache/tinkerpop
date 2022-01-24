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

const utils = require('../utils');
const Connection = require('./connection');
const Bytecode = require('../process/bytecode');

/**
 * A {@link Client} contains methods to send messages to a Gremlin Server.
 */
class Client {
  /**
   * Creates a new instance of {@link Client}.
   * @param {String} url The resource uri.
   * @param {Object} [options] The connection options.
   * @param {Array} [options.ca] Trusted certificates.
   * @param {String|Array|Buffer} [options.cert] The certificate key.
   * @param {String} [options.mimeType] The mime type to use.
   * @param {String|Buffer} [options.pfx] The private key, certificate, and CA certs.
   * @param {GraphSONReader} [options.reader] The reader to use.
   * @param {Boolean} [options.rejectUnauthorized] Determines whether to verify or not the server certificate.
   * @param {String} [options.traversalSource] The traversal source. Defaults to: 'g'.
   * @param {GraphSONWriter} [options.writer] The writer to use.
   * @param {Authenticator} [options.authenticator] The authentication handler to use.
   * @param {String} [options.processor] The name of the opProcessor to use, leave it undefined or set 'session' when session mode.
   * @param {String} [options.session] The sessionId of Client in session mode. Defaults to null means session-less Client.
   * @constructor
   */
  constructor(url, options = {}) {
    this._options = options;
    if (this._options.processor === "session") {
      // compatibility with old 'session' processor setting
      this._options.session = options.session || utils.getUuid();
    }
    if (this._options.session) {
      // re-assign processor to 'session' when in session mode
      this._options.processor = options.processor || "session";
    }
    this._connection = new Connection(url, options);
  }

  /**
   * Opens the underlying connection to the Gremlin Server, if it's not already opened.
   * @returns {Promise}
   */
  open() {
    return this._connection.open();
  }

  /**
   * Returns true if the underlying connection is open
   * @returns {Boolean}
   */
  get isOpen() {
    return this._connection.isOpen;
  }

  /**
   * Configuration specific to the current request.
   * @typedef {Object} RequestOptions
   * @property {String} requestId - User specified request identifier which must be a UUID.
   * @property {Number} batchSize - Indicates whether the Power component is present.
   * @property {String} userAgent - The size in which the result of a request is to be "batched" back to the client
   * @property {Number} evaluationTimeout - The timeout for the evaluation of the request.
   */

  /**
   * Submit configuration
   * @typedef {Object} SubmitConfiguration
   * @property {String} processor - The processer to use for the gremlin query.
   * @property {Number} op - The operation for the gremlin server.
   * @property {boolean} requestIdOverride - Whether or not a user provided request id is in use.
   * @property {Object} args - Bindings and request options.
   */

  /**
   * @private
   * @param {Bytecode|string} message The bytecode or script to send
   * @param {Object} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {SubmitConfiguration}
   */
  buildSubmitRequest(message, bindings, requestOptions) {
    const args = Object.assign(
      {
        gremlin: message,
        aliases: { g: this._options.traversalSource || "g" },
      },
      requestOptions
    );

    if (this._options.session && this._options.processor === "session") {
      args["session"] = this._options.session;
    }

    const requestIdOverride = requestOptions && requestOptions.requestId;
    if (requestIdOverride) delete requestOptions["requestId"];

    const output = {
      args,
      requestIdOverride,
    };

    if (message instanceof Bytecode) {
      if (this._options.session && this._options.processor === "session") {
        output.processer = "session";
        output.op = "bytecode";
      } else {
        output.processer = "traversal";
        output.op = "bytecode";
      }
    } else if (typeof message === "string") {
      args["bindings"] = bindings;
      args["language"] = "gremlin-groovy";
      args["accept"] = this._connection.mimeType;
      output.processer = this._options.processor || "";
      output.op = "eval";
    } else {
      throw new TypeError("message must be of type Bytecode or string");
    }
    return output;
  }

  /**
   * Send a request to the Gremlin Server, can send a script or bytecode steps.
   * @param {Bytecode|string} message The bytecode or script to send
   * @param {Object} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {Promise}
   */
  submit(message, bindings, requestOptions) {
    const { processor, op, args, requestIdOverride } = this.buildSubmitRequest(
      message,
      bindings,
      requestOptions
    );

    return this._connection.submit(processor, op, args, requestIdOverride);
  }

  /**
   * Send a request to the Gremlin Server and receive a stream for the results, can send a script or bytecode steps.
   * @param {Bytecode|string} message The bytecode or script to send
   * @param {Object} [bindings] The script bindings, if any.
   * @param {RequestOptions} [requestOptions] Configuration specific to the current request.
   * @returns {ReadableStream}
   */
  stream(message, bindings, requestOptions) {
    const { processor, op, args, requestIdOverride } = this.buildSubmitRequest(
      message,
      bindings,
      requestOptions
    );

    return this._connection.stream(processor, op, args, requestIdOverride);
  }

  /**
   * Closes the underlying connection
   * send session close request before connection close if session mode
   * @returns {Promise}
   */
  close() {
    if (this._options.session && this._options.processor === "session") {
      const args = { session: this._options.session };
      return this._connection
        .submit(this._options.processor, "close", args, null)
        .then(() => this._connection.close());
    }
    return this._connection.close();
  }

  /**
   * Adds an event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The callback to be called when the event occurs.
   */
  addListener(event, handler) {
    this._connection.on(event, handler);
  }

  /**
   * Removes a previowsly added event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The event handler to be removed.
   */
  removeListener(event, handler) {
    this._connection.removeListener(event, handler);
  }
}

module.exports = Client;
