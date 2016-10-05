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
(function exportModule() {
  "use strict";

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

  var t = loadModule.call(this, './process/traversal.js');
  var gt = loadModule.call(this, './process/graph-traversal.js');
  var graph = loadModule.call(this, './structure/graph.js');
  var gs = loadModule.call(this, './structure/io/graph-serializer.js');
  var rc = loadModule.call(this, './driver/remote-connection.js');
  var toExport = {
    driver: {
      RemoteConnection: rc.RemoteConnection,
      RemoteStrategy: rc.RemoteStrategy,
      RemoteTraversal: rc.RemoteTraversal
    },
    process: {
      Bytecode: t.Bytecode,
      EnumValue: t.EnumValue,
      inherits: t.inherits,
      P: t.P,
      parseArgs: t.parseArgs,
      Traversal: t.Traversal,
      TraversalSideEffects: t.TraversalSideEffects,
      TraversalStrategies: t.TraversalStrategies,
      TraversalStrategy: t.TraversalStrategy,
      Traverser: t.Traverser,
      GraphTraversal: gt.GraphTraversal,
      GraphTraversalSource: gt.GraphTraversalSource,
      statics: gt.statics
    },
    structure: {
      io: {
        GraphSONReader: gs.GraphSONReader,
        GraphSONWriter: gs.GraphSONWriter
      },
      Edge: graph.Edge,
      Graph: graph.Graph,
      Path: graph.Path,
      Property: graph.Property,
      Vertex: graph.Vertex,
      VertexProperty: graph.VertexProperty
    }
  };


  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  return toExport;
}).call(this);