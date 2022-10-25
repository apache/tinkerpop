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

const utils = require('../lib/utils');
const DriverRemoteConnection = require('../lib/driver/driver-remote-connection');
const Client = require('../lib/driver/client');
const PlainTextSaslAuthenticator = require('../lib/driver/auth/plain-text-sasl-authenticator');

let serverUrl;
let serverAuthUrl;
if (process.env.DOCKER_ENVIRONMENT === 'true') {
  serverUrl = 'ws://gremlin-server-test-js:45940/gremlin';
  serverAuthUrl = 'wss://gremlin-server-test-js:45941/gremlin';
} else {
  serverUrl = 'ws://localhost:45940/gremlin';
  serverAuthUrl = 'wss://localhost:45941/gremlin';
}

/** @returns {DriverRemoteConnection} */
exports.getConnection = function getConnection(traversalSource) {
  return new DriverRemoteConnection(serverUrl, { traversalSource, mimeType: process.env.CLIENT_MIMETYPE });
};

exports.getSecureConnectionWithPlainTextSaslAuthenticator = (traversalSource, username, password) => {
  const authenticator = new PlainTextSaslAuthenticator(username, password);
  return new DriverRemoteConnection(serverAuthUrl, {
    traversalSource,
    authenticator,
    rejectUnauthorized: false,
    mimeType: process.env.CLIENT_MIMETYPE,
  });
};

exports.getDriverRemoteConnection = (url, options) => {
  return new DriverRemoteConnection(url, { ...options, mimeType: process.env.CLIENT_MIMETYPE });
};

exports.getClient = function getClient(traversalSource) {
  return new Client(serverUrl, { traversalSource, mimeType: process.env.CLIENT_MIMETYPE });
};

exports.getSessionClient = function getSessionClient(traversalSource) {
  const sessionId = utils.getUuid();
  return new Client(serverUrl, {
    'traversalSource': traversalSource,
    'session': sessionId.toString(),
    mimeType: process.env.CLIENT_MIMETYPE,
  });
};
