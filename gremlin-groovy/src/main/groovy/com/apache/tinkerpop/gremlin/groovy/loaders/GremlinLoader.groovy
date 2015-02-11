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
package com.apache.tinkerpop.gremlin.groovy.loaders

import com.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.apache.tinkerpop.gremlin.process.Step
import com.apache.tinkerpop.gremlin.process.Traversal
import com.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal
import com.apache.tinkerpop.gremlin.structure.Edge
import com.apache.tinkerpop.gremlin.structure.Element
import com.apache.tinkerpop.gremlin.structure.Graph
import com.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinLoader {

    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()
    private static final Set<String> steps = new HashSet<String>()
    static {
        [GraphTraversal, Graph, Vertex, Edge, Element].forEach {
            it.getMethods().findAll {
                Traversal.class.isAssignableFrom(it.getReturnType());
            }.each {
                addStep(it.getName())
            }
        }
    }

    public static void load() {
        ObjectLoader.load()
        StepLoader.load()
    }

    public static Step compile(final String script) {
        return (Step) engine.eval(script, engine.createBindings())
    }

    public static void addStep(final String stepName) {
        steps.add(stepName)
    }

    public static boolean isStep(final String stepName) {
        return steps.contains(stepName)
    }

    public static Set<String> getStepNames() {
        return new HashSet(steps)
    }
}
