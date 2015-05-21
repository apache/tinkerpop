/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Generates the modern graph into an "empty" TinkerGraph via LifeCycleHook
// it is important that the hook be assigned to a variable (in this case "hook").
// the exact name of this variable is unimportant.
hook = [
  onStartUp: { ctx ->
    ctx.logger.info("Loading graph data from data/sample.kryo.")

    // An example of an initialization script that can be configured to run in Gremlin Server.
    graph.io(GryoIo.build()).readGraph('data/sample.kryo')
  }
] as LifeCycleHook

// define the default TraversalSource to bind queries to. Code outside of the "hook"
// will execute for each instantiated ScriptEngine instance. Use this part of the
// script to initialize functions that are meant to be re-usable.
g = graph.traversal()

// Functions defined here will go into global cache and will not be removed from there
// unless there is a reset of the ScriptEngine.
def addItUp(x, y) { x + y }