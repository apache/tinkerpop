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

// An example of an initialization script that can be configured to run in Gremlin Server.
// Functions defined here will go into global cache and will not be removed from there
// unless there is a reset of the ScriptEngine.
def addItUp(x, y) { x + y }

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// Generates the modern graph into an "empty" TinkerGraph via LifeCycleHook.
// Note that the name of the key in the "global" map is unimportant.
globals << [hook : [
  onStartUp: { ctx ->
    // a wild bit of trickery here. the process tests use an INTEGER id manager when LoadGraphWith is used. this
    // closure provides a way to to manually override the various id managers for TinkerGraph - the graph on which
    // all of these remote tests are executed - so that the tests will pass nicely. an alternative might have been
    // to have a special test TinkerGraph config for setting up the id manager properly, but based on how we do
    // things now, that test config would have been mixed in with release artifacts and there would have been ugly
    // exclusions to make packaging work properly.
    allowSetOfIdManager = { graph, idManagerFieldName, idManager ->
        java.lang.reflect.Field idManagerField = graph.class.getSuperclass().getDeclaredField(idManagerFieldName)
        idManagerField.setAccessible(true)
        idManagerField.set(graph, idManager)
    }

    [classic, modern, crew, sink, grateful].each{
      allowSetOfIdManager(it, "vertexIdManager", TinkerGraph.DefaultIdManager.INTEGER)
      allowSetOfIdManager(it, "edgeIdManager", TinkerGraph.DefaultIdManager.INTEGER)
      allowSetOfIdManager(it, "vertexPropertyIdManager", TinkerGraph.DefaultIdManager.LONG)
    }
    TinkerFactory.generateClassic(classic)
    TinkerFactory.generateModern(modern)
    TinkerFactory.generateTheCrew(crew)
    TinkerFactory.generateGratefulDead(grateful)
    TinkerFactory.generateKitchenSink(sink)
  }
] as LifeCycleHook]

// add default TraversalSource instances for each graph instance
globals << [gclassic : traversal().withEmbedded(classic)]
globals << [gmodern : traversal().withEmbedded(modern)]
globals << [g : traversal().withEmbedded(graph)]
globals << [gcrew : traversal().withEmbedded(crew)]
globals << [ggraph : traversal().withEmbedded(graph)]
globals << [ggrateful : traversal().withEmbedded(grateful)]
globals << [gsink : traversal().withEmbedded(sink)]
globals << [gtx : traversal().withEmbedded(tx)]

// dynamically detect existence of gimmutable as it is only used in gremlin-go testing suite
def dynamicGimmutable = context.getBindings(javax.script.ScriptContext.GLOBAL_SCOPE)["immutable"]
if (dynamicGimmutable != null)
    globals << [gimmutable : traversal().withEmbedded(dynamicGimmutable)]

globals
