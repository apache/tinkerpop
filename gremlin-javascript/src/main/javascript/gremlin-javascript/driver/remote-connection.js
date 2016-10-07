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
(function defineRemoteConnectionModule() {
  "use strict";

  var t = loadModule.call(this, '../process/traversal.js');
  var inherits = t.inherits;

  function RemoteConnection(url, traversalSource) {
    this.url = url;
    this.traversalSource = traversalSource;
  }

  /**
   * @abstract
   * @param {Bytecode} bytecode
   * @param {Function} callback
   */
  RemoteConnection.prototype.submit = function (bytecode, callback) {
    throw new Error('submit() needs to be implemented');
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
    t.TraversalStrategy.call(this);
    this.connection = connection;
  }

  inherits(RemoteStrategy, t.TraversalStrategy);

  /** @override */
  RemoteStrategy.prototype.apply = function (traversal, callback) {
    if (traversal.traversers) {
      return callback();
    }
    this.connection.submit(traversal.getBytecode(), function (err, remoteTraversal) {
      if (err) {
        return callback(err);
      }
      traversal.sideEffects = remoteTraversal.sideEffects;
      traversal.traversers = remoteTraversal.traversers;
      callback();
    });
  };

  function loadModule(moduleName) {
    if (typeof require !== 'undefined') {
      return require(moduleName);
    }
    if (typeof load !== 'undefined') {
      var path = new java.io.File(__DIR__ + moduleName).getCanonicalPath();
      this.__dependencies = this.__dependencies || {};
      return this.__dependencies[path] = (this.__dependencies[path] || load(path));
    }
    throw new Error('No module loader was found');
  }

  var toExport = {
    RemoteConnection: RemoteConnection,
    RemoteStrategy: RemoteStrategy,
    RemoteTraversal: RemoteTraversal
  };
  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  // Nashorn and rest
  return toExport;
}).call(this);