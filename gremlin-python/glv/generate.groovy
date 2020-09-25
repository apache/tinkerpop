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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponent
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.TextP
import org.apache.tinkerpop.gremlin.process.traversal.IO
import org.apache.tinkerpop.gremlin.process.traversal.Operator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.structure.T
import java.lang.reflect.Modifier

// this is a bit of a copy of what's in SymbolHelper - no way around it because this code generation task occurs
// before the SymbolHelper is available to the plugin.
def toPythonMap = ["and": "and_",
                   "all": "all_",
                   "as": "as_",
                   "filter": "filter_",
                   "from": "from_",
                   "global": "global_",
                   "id": "id_",
                   "in": "in_",
                   "is": "is_",
                   "list": "list_",
                   "max": "max_",
                   "min": "min_",
                   "not": "not_",
                   "range": "range_",
                   "or": "or_",
                   "set": "set_",
                   "sum": "sum_",
                   "with": "with_"]

def gatherTokensFrom = { tokenClasses ->
    def m = [:]
    tokenClasses.each { tc -> m << [(tc.simpleName) : tc.getFields().sort{ a, b -> a.name <=> b.name }.collectEntries{ f -> [(f.name) : f.get(null)]}]}
    return m
}

def toPythonValue = { type, value ->
  type == String.class && value != null ? ('"' + value + '"') : value
}

def toJavaMap = toPythonMap.collectEntries{k,v -> [(v):k]}
def toPython = { symbol -> toPythonMap.getOrDefault(symbol, symbol) }
def toJava = { symbol -> toJavaMap.getOrDefault(symbol, symbol) }

// for enums we ignore T and handle it manually because of conflict with T.id which is trickier to deal with
// in the templating language
def binding = ["enums": CoreImports.getClassImports()
        .findAll { Enum.class.isAssignableFrom(it) && !(it in [T, Operator]) }
        .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() },
               "pmethods": P.class.getMethods().
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { P.class.isAssignableFrom(it.returnType) }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "tpmethods": TextP.class.getMethods().
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { TextP.class.isAssignableFrom(it.returnType) }.
                       collect { toPython(it.name) }.
                       unique().
                       sort { a, b -> a <=> b },
               "sourceStepMethods": GraphTraversalSource.getMethods(). // SOURCE STEPS
                       findAll { GraphTraversalSource.class.equals(it.returnType) }.
                       findAll {
                           !it.name.equals("clone") &&
                                   !it.name.equals(TraversalSource.Symbols.with) &&
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
               "tokens": gatherTokensFrom([IO, ConnectedComponent, ShortestPath, PageRank, PeerPressure]),
               "toPython": toPython,
               "toJava": toJava,
               "withOptions": WithOptions.getDeclaredFields().
                        collect {["name": it.name, "value": toPythonValue(it.type, it.get(null))]}]

def engine = new groovy.text.GStringTemplateEngine()
def traversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/TraversalSource.template")).make(binding)
def traversalFile = new File("${projectBaseDir}/src/main/jython/gremlin_python/process/traversal.py")
traversalFile.newWriter().withWriter{ it << traversalTemplate }

def graphTraversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/GraphTraversalSource.template")).make(binding)
def graphTraversalFile = new File("${projectBaseDir}/src/main/jython/gremlin_python/process/graph_traversal.py")
graphTraversalFile.newWriter().withWriter{ it << graphTraversalTemplate }
