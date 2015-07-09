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
package org.apache.tinkerpop.gremlin.groovy.jsr223

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.transform.stc.GroovyTypeCheckingExtensionSupport

import java.util.function.BiPredicate

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SandboxExtension extends GroovyTypeCheckingExtensionSupport.TypeCheckingDSL {

    protected boolean graphIsAlwaysGraphInstance = true
    protected boolean gIsAlwaysGraphTraversalSource = true
    protected BiPredicate<VariableExpression, Map<String,ClassNode>> filter = USE_ALL

    public static final BiPredicate<VariableExpression, Map<String,ClassNode>> USE_ALL = { var, types -> true }

    @Override
    Object run() {
        unresolvedVariable { var ->
            if (var.name == "graph" && graphIsAlwaysGraphInstance) {
                storeType(var, classNodeFor(Graph))
                handled = true
                return
            }

            if (var.name == "g" && gIsAlwaysGraphTraversalSource) {
                storeType(var, classNodeFor(GraphTraversalSource))
                handled = true
                return
            }

            final Map<String,ClassNode> varTypes = (Map<String,ClassNode>) GremlinGroovyScriptEngine.COMPILE_OPTIONS.get()
                    .get(GremlinGroovyScriptEngine.COMPILE_OPTIONS_VAR_TYPES)
            if (varTypes.containsKey(var.name) && filter.test(var, varTypes)
                || (var.name == "graph" && !graphIsAlwaysGraphInstance
                     || var.name == "g" && !gIsAlwaysGraphTraversalSource)) {
                storeType(var, varTypes.get(var.name))
                handled = true
                return
            }
        }
    }
}
