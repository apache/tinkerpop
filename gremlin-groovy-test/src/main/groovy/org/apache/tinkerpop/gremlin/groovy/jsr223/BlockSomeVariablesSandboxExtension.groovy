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

import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.SandboxExtension
import org.apache.tinkerpop.gremlin.structure.Graph
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.expr.VariableExpression

import java.util.function.BiPredicate

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class BlockSomeVariablesSandboxExtension extends SandboxExtension {
    BlockSomeVariablesSandboxExtension() {
        gIsAlwaysGraphTraversalSource = false
        graphIsAlwaysGraphInstance = false

        // variable names must have a length of 3 and they can't be Graph instances
        variableFilter = (BiPredicate<VariableExpression, Map<String,ClassNode>>) { v, m ->
            def varType = m[v.name].getTypeClass()
            v.name.length() > 3 && !Graph.isAssignableFrom(varType)
        }
    }
}
