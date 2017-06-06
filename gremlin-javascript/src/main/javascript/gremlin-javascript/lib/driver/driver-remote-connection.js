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

var crypto = require('crypto');
var WebSocket = require('ws');
var util = require('util');
var RemoteConnection = require('./remote-connection').RemoteConnection;
var utils = require('../utils');
var serializer = require('../structure/io/graph-serializer');
var inherits = utils.inherits;
var mimeType = 'application/vnd.gremlin-v2.0+json';
var header = String.fromCharCode(mimeType.length) + mimeType;
var responseStatusCode = {
  success: 200,
  noContent: 204,
  partialContent: 206
};

/**
 * Creates a new instance of DriverRemoteConnection.
 * @param {String} url The resource uri.
 * @param {Object} [options] The connection options.
 * @param {Array} [options.ca] Trusted certificates.
 * @param {String|Array|Buffer} [options.cert] The certificate key.
 * @param {String|Buffer} [options.pfx] The private key, certificate, and CA certs.
 * @param {GraphSONReader} [options.reader] The GraphSON2 reader to use.
 * @param {Boolean} [options.rejectUnauthorized] Determines whether to verify or not the server certificate.
 * @param {GraphSONWriter} [options.writer] The GraphSON2 writer to use.
 * @constructor
 */
function DriverRemoteConnection(url, options) {
  options = options || {};
  this._ws = new WebSocket(url, {
    ca: options.ca,
    cert: options.cert,
    pfx: options.pfx,
    rejectUnauthorized: options.rejectUnauthorized
  });
  var self = this;
  this._ws.on('open', function opened () {
    self.isOpen = true;
    if (self._openCallback) {
      self._openCallback();
    }
  });
  this._ws.on('message', function incoming (data) {
    self._handleMessage(data);
  });
  // A map containing the request id and the handler
  this._responseHandlers = {};
  this._reader = options.reader || new serializer.GraphSONReader();
  this._writer = options.writer || new serializer.GraphSONWriter();
  this._openPromise = null;
  this._openCallback = null;
  this._closePromise = null;
  this.isOpen = false;
}

inherits(DriverRemoteConnection, RemoteConnection);

/**
 * Opens the connection, if its not already opened.
 * @returns {Promise}
 */
DriverRemoteConnection.prototype.open = function (promiseFactory) {
  if (this._closePromise) {
    return this._openPromise = utils.toPromise(promiseFactory, function promiseHandler(callback) {
      callback(new Error('Connection has been closed'));
    });
  }
  if (this._openPromise) {
    return this._openPromise;
  }
  var self = this;
  return this._openPromise = utils.toPromise(promiseFactory, function promiseHandler(callback) {
    if (self.isOpen) {
      return callback();
    }
    // It will be invoked when opened
    self._openCallback = callback;
  });
};

/** @override */
DriverRemoteConnection.prototype.submit = function (bytecode, promiseFactory) {
  var self = this;
  return this.open().then(function () {
    return utils.toPromise(promiseFactory, function promiseHandler(callback) {
      var requestId = getUuid();
      self._responseHandlers[requestId] = {
        callback: callback,
        result: null
      };
      var message = bufferFromString(header + JSON.stringify(self._getRequest(requestId, bytecode)));
      self._ws.send(message);
    });
  });
};

DriverRemoteConnection.prototype._getRequest = function (id, bytecode) {
  return ({
    'requestId': { '@type': 'g:UUID', '@value': id },
    'op': 'bytecode',
    'processor': 'traversal',
    'args': {
      'gremlin': this._writer.adaptObject(bytecode),
      'aliases': { 'g': 'g'}
    }
  });
};

DriverRemoteConnection.prototype._handleMessage = function (data) {
  var response = this._reader.read(JSON.parse(data.toString()));
  var handler = this._responseHandlers[response.requestId];
  if (response.status.code >= 400) {
    // callback in error
    return handler.callback(
      new Error(util.format('Server error: %s (%d)', response.status.message, response.status.code)));
  }
  switch (response.status.code) {
    case responseStatusCode.noContent:
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
      return handler.callback(null, { traversers: handler.result });
  }
};

/**
 * Closes the Connection.
 * @return {Promise}
 */
DriverRemoteConnection.prototype.close = function (promiseFactory) {
  if (this._closePromise) {
    return this._closePromise;
  }
  var self = this;
  return this._closePromise = utils.toPromise(promiseFactory, function promiseHandler(callback) {
    self._ws.on('close', function () {
      self.isOpen = false;
      callback();
    });
    self._ws.close();
  });
};

function getUuid() {
  var buffer = crypto.randomBytes(16);
  //clear the version
  buffer[6] &= 0x0f;
  //set the version 4
  buffer[6] |= 0x40;
  //clear the variant
  buffer[8] &= 0x3f;
  //set the IETF variant
  buffer[8] |= 0x80;
  var hex = buffer.toString('hex');
  return (
    hex.substr(0, 8) + '-' +
    hex.substr(8, 4) + '-' +
    hex.substr(12, 4) + '-' +
    hex.substr(16, 4) + '-' +
    hex.substr(20, 12));
}

var bufferFromString = Buffer.from || function newBuffer(text) {
  return new Buffer(text, 'utf8');
};

module.exports = DriverRemoteConnection;