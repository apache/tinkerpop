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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.codehaus.groovy.ast.ClassHelper
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.transform.stc.GroovyTypeCheckingExtensionSupport

/**
 * A base class for building sandboxes for the {@code ScriptEngine}. Uses a "white list" method to validate what
 * variables and methods can be used in scripts.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
abstract class AbstractSandboxExtension extends GroovyTypeCheckingExtensionSupport.TypeCheckingDSL {

    /**
     * Return a list of methods that are acceptable for the {@code ScriptEngine} to execute.  The methods in the
     * list should be defined in a regex format for pattern matching.
     */
    abstract List<String> getMethodWhiteList()

    abstract Map<String,Class<?>> getStaticVariableTypes()

    boolean allowAutoTypeOfUnknown() {
        return true
    }

    @Override
    Object run() {
        def staticVariableTyping = getStaticVariableTypes()
        def methodWhiteList = getMethodWhiteList()
        def boolean autoTypeUnknown = allowAutoTypeOfUnknown()

        unresolvedVariable { var ->
            if (staticVariableTyping.containsKey(var.name)) {
                storeType(var, classNodeFor(staticVariableTyping[var.name]))
                handled = true
                return
            }

            final Map<String,ClassNode> varTypes = (Map<String,ClassNode>) GremlinGroovyScriptEngine.COMPILE_OPTIONS.get()
                    .get(GremlinGroovyScriptEngine.COMPILE_OPTIONS_VAR_TYPES)

            // use the types of the bound variables where they match up
            if (varTypes.containsKey(var.name))  {
                storeType(var, varTypes.get(var.name))
                handled = true
                return
            }

            if (autoTypeUnknown) {
                // everything else just gets bound to Object
                storeType(var, ClassHelper.OBJECT_TYPE)
                handled = true
            }
        }

        // evaluate methods to be sure they are on the whitelist
        onMethodSelection { expr, MethodNode methodNode ->
            def descriptor = SandboxHelper.toMethodDescriptor(methodNode)
            if (!methodWhiteList.any { descriptor ==~ it })
                addStaticTypeError("Not authorized to call this method: $descriptor", expr)
        }

        // handles calls to properties to be sure they are on the whitelist
        afterVisitMethod { methodNode ->
            def visitor = new PropertyExpressionEvaluator(context.source, methodWhiteList, this)
            visitor.visitMethod(methodNode)
        }
    }
}