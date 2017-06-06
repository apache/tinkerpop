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

var t = require('../process/traversal');
var TraversalStrategy = require('../process/traversal-strategy').TraversalStrategy;
var utils = require('../utils');
var inherits = utils.inherits;

function RemoteConnection(url, traversalSource) {
  this.url = url;
}

/**
 * @abstract
 * @param {Bytecode} bytecode
 * @param {Function|undefined} promiseFactory
 * @returns {Promise}
 */
RemoteConnection.prototype.submit = function (bytecode, promiseFactory) {
  throw new Error('submit() was not implemented');
};

/**
 * @extends {Traversal}
 * @constructor
 */
function RemoteTraversal(traversers, sideEffects) {
  t.Traversal.call(this, null, null, null);
  this.traversers = traversers;
  this.sideEffects = sideEffects;
}

inherits(RemoteTraversal, t.Traversal);

/**
 *
 * @param {RemoteConnection} connection
 * @extends {TraversalStrategy}
 * @constructor
 */
function RemoteStrategy(connection) {
  TraversalStrategy.call(this);
  this.connection = connection;
}

inherits(RemoteStrategy, TraversalStrategy);

/** @override */
RemoteStrategy.prototype.apply = function (traversal, promiseFactory) {
  if (traversal.traversers) {
    return utils.resolvedPromise(promiseFactory);
  }
  return this.connection.submit(traversal.getBytecode(), promiseFactory).then(function (remoteTraversal) {
    traversal.sideEffects = remoteTraversal.sideEffects;
    traversal.traversers = remoteTraversal.traversers;
  });
};

module.exports = {
  RemoteConnection: RemoteConnection,
  RemoteStrategy: RemoteStrategy,
  RemoteTraversal: RemoteTraversal
};
