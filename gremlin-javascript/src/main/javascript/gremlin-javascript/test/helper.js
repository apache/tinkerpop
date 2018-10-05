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
const os = require('os');

const DriverRemoteConnection = require('../lib/driver/driver-remote-connection');
const Client = require('../lib/driver/client');
const PlainTextSaslAuthenticator = require('../lib/driver/auth/plain-text-sasl-authenticator');

exports.getConnection = function getConnection(traversalSource) {
  return new DriverRemoteConnection('ws://localhost:45940/gremlin', { traversalSource: traversalSource });
};

exports.getSecureConnectionWithPlainTextSaslAuthenticator = function getConnection(traversalSource) {
  const authenticator = new PlainTextSaslAuthenticator('stephen', 'password');
  return new DriverRemoteConnection('ws://localhost:45941/gremlin', { 
    traversalSource: traversalSource, 
    authenticator: authenticator, 
    rejectUnauthorized: false 
  });
};

exports.getClient = function getClient(traversalSource) {
  return new Client('ws://localhost:45940/gremlin', { traversalSource: traversalSource });
};