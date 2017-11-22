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
    TinkerFactory.generateClassic(classic)
    TinkerFactory.generateModern(modern)
    TinkerFactory.generateTheCrew(crew)
    grateful.io(gryo()).readGraph('data/grateful-dead.kryo')
  }
] as LifeCycleHook]

// add default TraversalSource instances for each graph instance
globals << [gclassic : classic.traversal()]
globals << [gmodern : modern.traversal()]
globals << [gcrew : crew.traversal()]
globals << [ggraph : graph.traversal()]
globals << [g : modern.traversal()]
globals << [ggrateful : grateful.traversal()]