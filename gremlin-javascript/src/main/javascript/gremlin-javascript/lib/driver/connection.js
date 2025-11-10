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

const { Buffer } = require('buffer');
const EventEmitter = require('eventemitter3');
const Stream = require('readable-stream');
const utils = require('../utils');
const { DeferredPromise } = utils;
const serializer = require('../structure/io/graph-serializer');
const { graphBinaryReader, graphBinaryWriter } = require('../structure/io/binary/GraphBinary');
const ResultSet = require('./result-set');
const ResponseError = require('./response-error');

const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206,
  authenticationChallenge: 407,
};

const defaultMimeType = 'application/vnd.graphbinary-v1.0';
const graphSON2MimeType = 'application/vnd.gremlin-v2.0+json';
const graphBinaryMimeType = 'application/vnd.graphbinary-v1.0';

const uuidPattern = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';

/**
 * Represents a single connection to a Gremlin Server.
 */
class Connection extends EventEmitter {
  /**
   * Creates a new instance of {@link Connection}.
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
   * @param {Object} [options.headers] An associative array containing the additional header key/values for the initial request.
   * @param {Boolean} [options.enableUserAgentOnConnect] Determines if a user agent will be sent during connection handshake. Defaults to: true
   * @param {http.Agent} [options.agent] The http.Agent implementation to use.
   * @param {Boolean} [options.enableCompression] Enable per-message deflate compression. Defaults to: false.
   * @constructor
   */
  constructor(url, options) {
    super();

    this.url = url;
    this.options = options = options || {};

    /**
     * Gets the MIME type.
     * @type {String}
     */
    this.mimeType = options.mimeType || defaultMimeType;

    // A map containing the request id and the handler. The id should be in lower case to prevent string comparison issues.
    this._responseHandlers = {};
    this._reader = options.reader || this.#getDefaultReader(this.mimeType);
    this._writer = options.writer || this.#getDefaultWriter(this.mimeType);
    this._openPromise = null;
    this._openCallback = null;
    this._closePromise = null;
    this._closeCallback = null;

    this._header = String.fromCharCode(this.mimeType.length) + this.mimeType; // TODO: what if mimeType.length > 255
    this._header_buf = Buffer.from(this._header);
    this.isOpen = false;
    this.traversalSource = options.traversalSource || 'g';
    this._authenticator = options.authenticator;
    this._enableUserAgentOnConnect = options.enableUserAgentOnConnect !== false;
    this._enableCompression = this.options.enableCompression || false;
  }

  /**
   * Opens the connection, if its not already opened.
   * @returns {Promise}
   */
  async open() {
    if (this.isOpen) {
      return;
    }
    if (this._openPromise) {
      return this._openPromise;
    }

    this._openPromise = DeferredPromise();

    this.emit('log', 'ws open');
    let headers = this.options.headers;
    if (this._enableUserAgentOnConnect) {
      if (!headers) {
        headers = [];
      }

      const userAgent = await utils.getUserAgent();
      if (userAgent !== undefined) {
        headers[utils.getUserAgentHeader()] = await utils.getUserAgent();
      }
    }
    // All these options are available to the `ws` package's constructor, but not the global WebSocket class
    const wsSpecificOptions = new Set([
      'headers',
      'ca',
      'cert',
      'pfx',
      'rejectUnauthorized',
      'agent',
      'perMessageDeflate',
    ]);
    // Check if any `ws` specific options are provided and are non-null / non-undefined
    const hasWsSpecificOptions =
      Object.entries(this.options).some(
        ([key, value]) => wsSpecificOptions.has(key) && ![null, undefined].includes(value),
      ) ||
      this._enableCompression || // we need to check the presence of this variable and pass this into ws
      this._enableUserAgentOnConnect; // global websocket will send "node" as user agent by default which doesn't comply with Gremlin
    // Only use the global websocket if we don't have any unsupported options
    const useGlobalWebSocket = !hasWsSpecificOptions && globalThis.WebSocket;
    const WebSocket = useGlobalWebSocket || (await import('ws')).default;

    this._ws = new WebSocket(
      this.url,
      !useGlobalWebSocket
        ? {
            headers: headers,
            ca: this.options.ca,
            cert: this.options.cert,
            pfx: this.options.pfx,
            rejectUnauthorized: this.options.rejectUnauthorized,
            agent: this.options.agent,
            perMessageDeflate: this._enableCompression,
          }
        : undefined,
    );

    if ('binaryType' in this._ws) {
      this._ws.binaryType = 'arraybuffer';
    }

    this._ws.addEventListener('open', this.#handleOpen);
    this._ws.addEventListener('error', this.#handleError);
    // Only attach unexpected-response listener if WebSocket supports .on() method
    // Browser WebSocket does not have this event and .on() method
    if (this._ws && 'on' in this._ws) {
      // The following listener needs to use `.on` and `.off` because the `ws` package's addEventListener only accepts certain event types
      // Ref: https://github.com/websockets/ws/blob/8.16.0/lib/event-target.js#L202-L241
      this._ws.on('unexpected-response', this.#handleUnexpectedResponse);
    }
    this._ws.addEventListener('message', this.#handleMessage);
    this._ws.addEventListener('close', this.#handleClose);
    return await this._openPromise;
  }

  /** @override */
  submit(processor, op, args, requestId) {
    // TINKERPOP-2847: Use lower case to prevent string comparison issues.
    const rid = (requestId || utils.getUuid()).toLowerCase();
    if (!rid.match(uuidPattern)) {
      throw new Error('Provided requestId "' + rid + '" is not a valid UUID.');
    }

    return this.open().then(
      () =>
        new Promise((resolve, reject) => {
          if (op !== 'authentication') {
            this._responseHandlers[rid] = {
              callback: (err, result) => (err ? reject(err) : resolve(result)),
              result: null,
            };
          }

          const request = {
            requestId: rid,
            op: op || 'bytecode',
            // if using op eval need to ensure processor stays unset if caller didn't set it.
            processor: !processor && op !== 'eval' ? 'traversal' : processor,
            args: args || {},
          };

          const request_buf = this._writer.writeRequest(request);
          const message = utils.toArrayBuffer(Buffer.concat([this._header_buf, request_buf]));
          this._ws.send(message);
        }),
    );
  }

  /** @override */
  stream(processor, op, args, requestId) {
    // TINKERPOP-2847: Use lower case to prevent string comparison issues.
    const rid = (requestId || utils.getUuid()).toLowerCase();
    if (!rid.match(uuidPattern)) {
      throw new Error('Provided requestId "' + rid + '" is not a valid UUID.');
    }

    const readableStream = new Stream.Readable({
      objectMode: true,
      read() {},
    });

    this._responseHandlers[rid] = {
      callback: (err) => (err ? readableStream.destroy(err) : readableStream.push(null)),
      result: readableStream,
    };

    this.open()
      .then(() => {
        const request = {
          requestId: rid,
          op: op || 'bytecode',
          // if using op eval need to ensure processor stays unset if caller didn't set it.
          processor: !processor && op !== 'eval' ? 'traversal' : processor,
          args: args || {},
        };

        const request_buf = this._writer.writeRequest(request);
        const message = utils.toArrayBuffer(Buffer.concat([this._header_buf, request_buf]));
        this._ws.send(message);
      })
      .catch((err) => readableStream.destroy(err));

    return readableStream;
  }

  #getDefaultReader(mimeType) {
    if (mimeType === graphBinaryMimeType) {
      return graphBinaryReader;
    }

    return mimeType === graphSON2MimeType ? new serializer.GraphSON2Reader() : new serializer.GraphSONReader();
  }

  #getDefaultWriter(mimeType) {
    if (mimeType === graphBinaryMimeType) {
      return graphBinaryWriter;
    }

    return mimeType === graphSON2MimeType ? new serializer.GraphSON2Writer() : new serializer.GraphSONWriter();
  }

  #handleOpen = () => {
    this._openPromise.resolve();
    this.isOpen = true;
  };

  /**
   * @param {ClientRequest} _req
   * @param {IncomingMessage} res
   */
  #handleUnexpectedResponse = async (_req, res) => {
    const body = await new Promise((resolve, reject) => {
      const chunks = [];
      res.on('data', (data) => {
        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
      });
      res.on('end', () => {
        resolve(chunks.length ? Buffer.concat(chunks) : null);
      });
      res.on('error', reject);
    });
    const statusCodeErrorMessage = `Unexpected server response code ${res.statusCode}`;
    const errorMessage = body ? `${statusCodeErrorMessage} with body:\n${body.toString()}` : statusCodeErrorMessage;
    const error = new Error(errorMessage);
    this.#handleError({
      error,
      message: errorMessage,
      type: 'unexpected-response',
      target: this._ws,
    });
  };

  /**
   * @param {Event} event
   */
  #handleError = ({ error }) => {
    this._openPromise.reject(error);
    this.emit('log', `ws error ${error}`);
    this.#cleanupWebsocket(error);
    this.emit('socketError', error);
  };

  /**
   * @param {CloseEvent} event
   */
  #handleClose = ({ code, message }) => {
    this.emit('log', `ws close code=${code} message=${message}`);
    this.#cleanupWebsocket();
    if (this._closeCallback) {
      this._closeCallback();
    }
    this.emit('close', code, message);
  };

  /**
   * @param {MessageEvent<any>} event
   */
  #handleMessage = ({ data: _data }) => {
    const data = _data instanceof ArrayBuffer ? Buffer.from(_data) : _data;

    const response = this._reader.readResponse(data);
    if (response.requestId === null || response.requestId === undefined) {
      // There was a serialization issue on the server that prevented the parsing of the request id
      // We invoke any of the pending handlers with an error
      Object.keys(this._responseHandlers).forEach((requestId) => {
        const handler = this._responseHandlers[requestId];
        this.#clearHandler(requestId);
        if (response.status !== undefined && response.status.message) {
          return handler.callback(
            // TINKERPOP-2285: keep the old server error message in case folks are parsing that - fix in a future breaking version
            new ResponseError(
              `Server error (no request information): ${response.status.message} (${response.status.code})`,
              response.status,
            ),
          );
        }
        // TINKERPOP-2285: keep the old server error message in case folks are parsing that - fix in a future breaking version
        return handler.callback(
          new ResponseError(`Server error (no request information): ${JSON.stringify(response)}`, response.status),
        );
      });
      return;
    }

    // TINKERPOP-2847: Use lower case to prevent string comparison issues.
    response.requestId = response.requestId.toLowerCase();
    const handler = this._responseHandlers[response.requestId];

    if (!handler) {
      // The handler for a given request id was not found
      // It was probably invoked earlier due to a serialization issue.
      return;
    }

    if (response.status.code === responseStatusCode.authenticationChallenge && this._authenticator) {
      this._authenticator
        .evaluateChallenge(response.result.data)
        .then((res) => this.submit(undefined, 'authentication', res, response.requestId))
        .catch(handler.callback);

      return;
    } else if (response.status.code >= 400) {
      this.#clearHandler(response.requestId);
      // callback in error
      return handler.callback(
        // TINKERPOP-2285: keep the old server error message in case folks are parsing that - fix in a future breaking version
        new ResponseError(`Server error: ${response.status.message} (${response.status.code})`, response.status),
      );
    }

    const isStreamingResponse = handler.result instanceof Stream.Readable;

    switch (response.status.code) {
      case responseStatusCode.noContent:
        this.#clearHandler(response.requestId);
        if (isStreamingResponse) {
          handler.result.push(new ResultSet(utils.emptyArray, response.status.attributes));
          return handler.callback(null);
        }
        return handler.callback(null, new ResultSet(utils.emptyArray, response.status.attributes));
      case responseStatusCode.partialContent:
        if (isStreamingResponse) {
          handler.result.push(new ResultSet(response.result.data, response.status.attributes));
          break;
        }
        handler.result = handler.result || [];
        handler.result.push.apply(handler.result, response.result.data);
        break;
      default:
        if (isStreamingResponse) {
          handler.result.push(new ResultSet(response.result.data, response.status.attributes));
          return handler.callback(null);
        }
        if (handler.result) {
          handler.result.push.apply(handler.result, response.result.data);
        } else {
          handler.result = response.result.data;
        }
        this.#clearHandler(response.requestId);
        return handler.callback(null, new ResultSet(handler.result, response.status.attributes));
    }
  };

  /**
   * clean websocket context
   */
  #cleanupWebsocket(err) {
    // Invoke waiting callbacks to complete Promises when closing the websocket
    Object.keys(this._responseHandlers).forEach((requestId) => {
      const handler = this._responseHandlers[requestId];
      const isStreamingResponse = handler.result instanceof Stream.Readable;
      if (isStreamingResponse) {
        handler.callback(null);
      } else {
        const cause = err ? err : new Error('Connection has been closed.');
        handler.callback(cause);
      }
    });
    this._ws.removeEventListener('open', this.#handleOpen);
    this._ws.removeEventListener('error', this.#handleError);
    // Only remove unexpected-response listener for Node.js WebSocket (ws package)
    // Browser WebSocket does not have this event and .off() method
    if (this._ws && 'off' in this._ws) {
      // The following listener needs to use `.on` and `.off` because the `ws` package's addEventListener only accepts certain event types
      // Ref: https://github.com/websockets/ws/blob/8.16.0/lib/event-target.js#L202-L241
      this._ws.off('unexpected-response', this.#handleUnexpectedResponse);
    }
    this._ws.removeEventListener('message', this.#handleMessage);
    this._ws.removeEventListener('close', this.#handleClose);
    this._openPromise = null;
    this._closePromise = null;
    this.isOpen = false;
  }

  /**
   * Clears the internal state containing the callback and result buffer of a given request.
   * @param requestId
   * @private
   */
  #clearHandler(requestId) {
    delete this._responseHandlers[requestId];
  }

  /**
   * Closes the Connection.
   * @return {Promise}
   */
  close() {
    if (this.isOpen === false) {
      return Promise.resolve();
    }
    if (!this._closePromise) {
      this._closePromise = new Promise((resolve) => {
        this._closeCallback = resolve;
        this._ws.close();
      });
    }
    return this._closePromise;
  }
}

module.exports = Connection;
