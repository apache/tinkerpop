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

const yaml = require('js-yaml');
const fs   = require('fs');

let serverUrl;
let serverAuthUrl;
let socketServerUrl;
let sockerServerConfigPath;
if (process.env.DOCKER_ENVIRONMENT === 'true') {
  serverUrl = 'ws://gremlin-server-test-js:45940/gremlin';
  serverAuthUrl = 'wss://gremlin-server-test-js:45941/gremlin';
  socketServerUrl = 'ws://gremlin-socket-server-js:';
  sockerServerConfigPath = '/js_app/gremlin-socket-server/conf/test-ws-gremlin.yaml';
} else {
  serverUrl = 'ws://localhost:45940/gremlin';
  serverAuthUrl = 'wss://localhost:45941/gremlin';
  socketServerUrl = 'ws://localhost:';
  sockerServerConfigPath = '../../../../../gremlin-tools/gremlin-socket-server/conf/test-ws-gremlin.yaml';
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

function getMimeTypeFromSocketServerSettings(socketServerSettings) {
  let mimeType;
  switch(socketServerSettings.SERIALIZER) {
    case "GraphSONV2":
      mimeType = 'application/vnd.gremlin-v2.0+json';
      break;
    case "GraphSONV3":
      mimeType = 'application/vnd.gremlin-v3.0+json';
      break;
    case "GraphBinaryV1":
    default:
      mimeType = 'application/vnd.graphbinary-v1.0';
      break;
  }
  return mimeType;
}

exports.getGremlinSocketServerClient = function getGremlinSocketServerClient(traversalSource) {
  const settings = exports.getGremlinSocketServerSettings();
  const url = socketServerUrl + settings.PORT + '/gremlin';
  let mimeType = getMimeTypeFromSocketServerSettings(settings)
  return new Client(url, { traversalSource, mimeType });
};

exports.getGremlinSocketServerClientNoUserAgent = function getGremlinSocketServerClient(traversalSource) {
  const settings = exports.getGremlinSocketServerSettings();
  const url = socketServerUrl + settings.PORT + '/gremlin';
  let mimeType = getMimeTypeFromSocketServerSettings(settings)
  return new Client(url, { traversalSource, mimeType, enableUserAgentOnConnect:false });
};

exports.getGremlinSocketServerSettings = function getGremlinSocketServerSettings() {
  const settings = yaml.load(fs.readFileSync(sockerServerConfigPath, 'utf8'));
  return settings;
};
