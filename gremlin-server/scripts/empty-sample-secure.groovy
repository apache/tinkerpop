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

/*******************************************************************
 * This script is meant to be executed with the
 * conf/gremlin-server-secure.yaml configuration file as an example
 * of how an init script running with CompileStaticCustomizerProvider
 * or TypeCheckedCustomizerProvider must be written to properly
 * execute.
 *******************************************************************/

// An example of an initialization script that can be configured to run in Gremlin Server.
// Functions defined here will go into global cache and will not be removed from there
// unless there is a reset of the ScriptEngine.
def addItUp(int x, int y) { x + y }

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// defines a sample LifeCycleHook that prints some output to the Gremlin Server console.
// note that the name of the key in the "global" map is unimportant. As this script,
// runs as part of a sandbox configuration, type-checking is enabled and thus the
// LifeCycleHook type needs to be defined for the "ctx" variable.
globals << [hook : [
  onStartUp: { LifeCycleHook.Context ctx ->
    ctx.logger.info("Executed once at startup of Gremlin Server.")
  },
  onShutDown: { LifeCycleHook.Context ctx ->
    ctx.logger.info("Executed once at shutdown of Gremlin Server.")
  }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
// ReferenceElementStrategy converts all graph elements (vertices/edges/vertex properties)
// to "references" (i.e. just id and label without properties). this strategy was added
// in 3.4.0 to make all Gremlin Server results consistent across all protocols and
// serialization formats aligning it with TinkerPop recommended practices for writing
// Gremlin.
//
// must use an instance of ReferenceElementStrategy as Groovy shorthands won't work with
// secure script execution.
globals << [g : traversal().withEmbedded(graph).withStrategies(ReferenceElementStrategy.instance())]