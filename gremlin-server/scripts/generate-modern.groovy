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

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// Generates the modern graph into an "empty" TinkerGraph via LifeCycleHook.
// Note that the name of the key in the "global" map is unimportant.
globals << [hook : [
  onStartUp: { ctx ->
    ctx.logger.info("Loading 'modern' graph data.")
      org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.generateModern(graph)
  }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
// ReferenceElementStrategy converts all graph elements (vertices/edges/vertex properties)
// to "references" (i.e. just id and label without properties). this strategy was added
// in 3.4.0 to make all Gremlin Server results consistent across all protocols and
// serialization formats aligning it with TinkerPop recommended practices for writing
// Gremlin.
globals << [g : traversal().withEmbedded(graph).withStrategies(ReferenceElementStrategy)]