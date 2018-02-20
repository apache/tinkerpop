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

const crypto = require('crypto');
const WebSocket = require('ws');
const util = require('util');
const RemoteConnection = require('./remote-connection').RemoteConnection;
const utils = require('../utils');
const serializer = require('../structure/io/graph-serializer');
const responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206
};
const defaultMimeType = 'application/vnd.gremlin-v2.0+json';

class DriverRemoteConnection extends RemoteConnection {
  /**
   * Creates a new instance of DriverRemoteConnection.
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
   * @constructor
   */
  constructor(url, options) {
    super(url);
    options = options || {};
    this._ws = new WebSocket(url, {
      ca: options.ca,
      cert: options.cert,
      pfx: options.pfx,
      rejectUnauthorized: options.rejectUnauthorized
    });
    this._ws.on('open', () => {
      this.isOpen = true;
      if (this._openCallback) {
        this._openCallback();
      }
    });
    this._ws.on('message', data => this._handleMessage(data));
    // A map containing the request id and the handler
    this._responseHandlers = {};
    this._reader = options.reader || new serializer.GraphSONReader();
    this._writer = options.writer || new serializer.GraphSONWriter();
    this._openPromise = null;
    this._openCallback = null;
    this._closePromise = null;
    const mimeType = options.mimeType || defaultMimeType;
    this._header = String.fromCharCode(mimeType.length) + mimeType;
    this.isOpen = false;
    this.traversalSource = options.traversalSource || 'g';
  }

  /**
   * Opens the connection, if its not already opened.
   * @returns {Promise}
   */
  open() {
    if (this._closePromise) {
      return this._openPromise = Promise.reject(new Error('Connection has been closed'));
    }
    if (this.isOpen) {
      return Promise.resolve();
    }
    if (this._openPromise) {
      return this._openPromise;
    }
    return this._openPromise = new Promise((resolve, reject) => {
      // Set the callback that will be invoked once the WS is opened
      this._openCallback = err => err ? reject(err) : resolve();
    });
  }

  /** @override */
  submit(bytecode) {
    return this.open().then(() => new Promise((resolve, reject) => {
      const requestId = getUuid();
      this._responseHandlers[requestId] = {
        callback: (err, result) => err ? reject(err) : resolve(result),
        result: null
      };
      const message = bufferFromString(this._header + JSON.stringify(this._getRequest(requestId, bytecode)));
      this._ws.send(message);
    }));
  }

  _getRequest(id, bytecode) {
    return ({
      'requestId': { '@type': 'g:UUID', '@value': id },
      'op': 'bytecode',
      'processor': 'traversal',
      'args': {
        'gremlin': this._writer.adaptObject(bytecode),
        'aliases': { 'g': this.traversalSource }
      }
    });
  }

  _handleMessage(data) {
    const response = this._reader.read(JSON.parse(data.toString()));
    if (response.requestId === null || response.requestId === undefined) {
        // There was a serialization issue on the server that prevented the parsing of the request id
        // We invoke any of the pending handlers with an error
        Object.keys(this._responseHandlers).forEach(requestId => {
          const handler = this._responseHandlers[requestId];
          this._clearHandler(requestId);
          if (response.status !== undefined && response.status.message) {
            return handler.callback(
              new Error(util.format(
                'Server error (no request information): %s (%d)', response.status.message, response.status.code)));
          } else {
            return handler.callback(new Error(util.format('Server error (no request information): %j', response)));
          }
        });
        return;
    }

    const handler = this._responseHandlers[response.requestId];

    if (!handler) {
      // The handler for a given request id was not found
      // It was probably invoked earlier due to a serialization issue.
      return;
    }

    if (response.status.code >= 400) {
      // callback in error
      return handler.callback(
        new Error(util.format('Server error: %s (%d)', response.status.message, response.status.code)));
    }
    switch (response.status.code) {
      case responseStatusCode.noContent:
        this._clearHandler(response.requestId);
        return handler.callback(null, { traversers: []});
      case responseStatusCode.partialContent:
        handler.result = handler.result || [];
        handler.result.push.apply(handler.result, response.result.data);
        break;
      default:
        if (handler.result) {
          handler.result.push.apply(handler.result, response.result.data);
        }
        else {
          handler.result = response.result.data;
        }
        this._clearHandler(response.requestId);
        return handler.callback(null, { traversers: handler.result });
    }
  }

  /**
   * Clears the internal state containing the callback and result buffer of a given request.
   * @param requestId
   * @private
   */
  _clearHandler(requestId) {
    delete this._responseHandlers[requestId];
  }

  /**
   * Closes the Connection.
   * @return {Promise}
   */
  close() {
    if (this._closePromise) {
      return this._closePromise;
    }
    this._closePromise = new Promise(resolve => {
      this._ws.on('close', function () {
        this.isOpen = false;
        resolve();
      });
      this._ws.close();
    });
  }
}

function getUuid() {
  const buffer = crypto.randomBytes(16);
  //clear the version
  buffer[6] &= 0x0f;
  //set the version 4
  buffer[6] |= 0x40;
  //clear the variant
  buffer[8] &= 0x3f;
  //set the IETF variant
  buffer[8] |= 0x80;
  const hex = buffer.toString('hex');
  return (
    hex.substr(0, 8) + '-' +
    hex.substr(8, 4) + '-' +
    hex.substr(12, 4) + '-' +
    hex.substr(16, 4) + '-' +
    hex.substr(20, 12));
}

const bufferFromString = (Int8Array.from !== Buffer.from && Buffer.from) || function newBuffer(text) {
  return new Buffer(text, 'utf8');
};

module.exports = DriverRemoteConnection;