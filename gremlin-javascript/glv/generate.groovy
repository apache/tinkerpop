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


import groovy.text.GStringTemplateEngine
import org.apache.tinkerpop.gremlin.jsr223.CoreImports
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponent
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.TP
import org.apache.tinkerpop.gremlin.process.traversal.IO
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import java.lang.reflect.Modifier

def toJsMap = ["in": "in_",
               "from": "from_",
               "with": "with_"]

def toJs = { symbol -> toJsMap.getOrDefault(symbol, symbol) }

def decapitalize = {
    String string = it;
    if (string == null || string.length() == 0) {
        return string;
    }
    def c = string.toCharArray();
    c[0] = Character.toLowerCase(c[0]);
    return new String(c);
}

def determineVersion = {
    def env = System.getenv()
    def mavenVersion = env.containsKey("TP_RELEASE_VERSION") ? env.get("JS_RELEASE_VERSION") : project.version
    return mavenVersion.replace("-SNAPSHOT", "-alpha1")
}

def gatherTokensFrom = { tokenClasses ->
    def m = [:]
    tokenClasses.each { tc -> m << [(tc.simpleName) : tc.getFields().sort{ a, b -> a.name <=> b.name }.collectEntries{ f -> [(f.name) : f.get(null)]}]}
    return m
}

def binding = ["enums": CoreImports.getClassImports()
        .findAll { Enum.class.isAssignableFrom(it) }
        .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() },
               "pmethods": P.class.getMethods().
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { P.class.isAssignableFrom(it.returnType) }.
                       collect { it.name }.
                       unique().
                       sort { a, b -> a <=> b },
               "tpmethods": TP.class.getMethods().
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { TP.class.isAssignableFrom(it.returnType) }.
                       collect { it.name }.
                       unique().
                       sort { a, b -> a <=> b },
               "sourceStepMethods": GraphTraversalSource.getMethods(). // SOURCE STEPS
                       findAll { GraphTraversalSource.class.equals(it.returnType) }.
                       findAll {
                           !it.name.equals("clone") &&
                                   // Use hardcoded name to be for forward-compatibility
                                   !it.name.equals("withBindings") &&
                                   !it.name.equals(TraversalSource.Symbols.withRemote) &&
                                   !it.name.equals(TraversalSource.Symbols.withComputer)
                       }.
                       collect { it.name }.
                       unique().
                       sort { a, b -> a <=> b },
               "sourceSpawnMethods": GraphTraversalSource.getMethods(). // SPAWN STEPS
                       findAll { GraphTraversal.class.equals(it.returnType) }.
                       collect { it.name }.
                       unique().
                       sort { a, b -> a <=> b },
               "graphStepMethods": GraphTraversal.getMethods().
                       findAll { GraphTraversal.class.equals(it.returnType) }.
                       findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                       collect { it.name }.
                       unique().
                       sort { a, b -> a <=> b },
               "anonStepMethods": __.class.getMethods().
                       findAll { GraphTraversal.class.equals(it.returnType) }.
                       findAll { Modifier.isStatic(it.getModifiers()) }.
                       findAll { !it.name.equals("__") && !it.name.equals("start") }.
                       collect { it.name }.
                       unique().
                       sort { a, b -> a <=> b },
               "tokens": gatherTokensFrom([IO, ConnectedComponent, ShortestPath, PageRank, PeerPressure]),
               "toJs": toJs,
               "version": determineVersion(),
               "decapitalize": decapitalize]

def engine = new GStringTemplateEngine()
def graphTraversalTemplate = engine.createTemplate(new File("${project.basedir}/glv/GraphTraversalSource.template"))
        .make(binding)
def graphTraversalFile =
        new File("${project.basedir}/src/main/javascript/gremlin-javascript/lib/process/graph-traversal.js")
graphTraversalFile.newWriter().withWriter{ it << graphTraversalTemplate }

def traversalTemplate = engine.createTemplate(new File("${project.basedir}/glv/TraversalSource.template")).make(binding)
def traversalFile = new File("${project.basedir}/src/main/javascript/gremlin-javascript/lib/process/traversal.js")
traversalFile.newWriter().withWriter{ it << traversalTemplate }

def packageJsonTemplate = engine.createTemplate(new File("${project.basedir}/glv/PackageJson.template")).make(binding)
def packageJsonFile = new File("${project.basedir}/src/main/javascript/gremlin-javascript/package.json")
packageJsonFile.newWriter().withWriter{ it << packageJsonTemplate }