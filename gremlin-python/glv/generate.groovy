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

import org.apache.tinkerpop.gremlin.jsr223.CoreImports
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import java.lang.reflect.Modifier
// this is a bit of a copy of what's in SymbolHelper - no way around it because this code generation task occurs
// before the SymbolHelper is available to the plugin.
def toPythonMap = ["global": "global_",
                   "as": "as_",
                   "in": "in_",
                   "and": "and_",
                   "or": "or_",
                   "is": "is_",
                   "not": "not_",
                   "from": "from_",
                   "list": "list_",
                   "set": "set_",
                   "all": "all_"]
def toJavaMap = toPythonMap.collectEntries{k,v -> [(v):k]}
def toPython = { symbol -> toPythonMap.getOrDefault(symbol, symbol) }
def toJava = { symbol -> toJavaMap.getOrDefault(symbol, symbol) }
def binding = ["enums": CoreImports.getClassImports()
        .findAll { Enum.class.isAssignableFrom(it) }
        .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() },
               "pmethods": P.class.getMethods().
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { P.class.isAssignableFrom(it.returnType) }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "sourceStepMethods": GraphTraversalSource.getMethods(). // SOURCE STEPS
                       findAll { GraphTraversalSource.class.equals(it.returnType) }.
                       findAll {
                           !it.name.equals("clone") &&
                                   !it.name.equals(TraversalSource.Symbols.withRemote) &&
                                   !it.name.equals(TraversalSource.Symbols.withComputer)
                       }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "sourceSpawnMethods": GraphTraversalSource.getMethods(). // SPAWN STEPS
                       findAll { GraphTraversal.class.equals(it.returnType) }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "graphStepMethods": GraphTraversal.getMethods().
                       findAll { GraphTraversal.class.equals(it.returnType) }.
                       findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "anonStepMethods": __.class.getMethods().
                       findAll { GraphTraversal.class.equals(it.returnType) }.
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { !it.name.equals("__") && !it.name.equals("start") }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "toPython": toPython,
               "toJava": toJava]

def engine = new groovy.text.GStringTemplateEngine()
def traversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/TraversalSource.template")).make(binding)
def traversalFile = new File("${projectBaseDir}/src/main/jython/gremlin_python/process/traversal.py")
traversalFile.newWriter().withWriter{ it << traversalTemplate }

def graphTraversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/GraphTraversalSource.template")).make(binding)
def graphTraversalFile = new File("${projectBaseDir}/src/main/jython/gremlin_python/process/graph_traversal.py")
graphTraversalFile.newWriter().withWriter{ it << graphTraversalTemplate }