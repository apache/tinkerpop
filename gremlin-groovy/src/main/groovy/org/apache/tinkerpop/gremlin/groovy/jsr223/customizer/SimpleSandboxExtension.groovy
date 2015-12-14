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
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.transform.stc.GroovyTypeCheckingExtensionSupport

/**
 * Blacklists the {@code System} class to ensure that one can't call {@code System.exit()}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SimpleSandboxExtension extends GroovyTypeCheckingExtensionSupport.TypeCheckingDSL {
    @Override
    Object run() {
        unresolvedVariable { var ->
            // use the types of the bound variables.
            final Map<String,ClassNode> varTypes = (Map<String,ClassNode>) GremlinGroovyScriptEngine.COMPILE_OPTIONS.get()
                    .get(GremlinGroovyScriptEngine.COMPILE_OPTIONS_VAR_TYPES)
            if (varTypes.containsKey(var.name))  {
                storeType(var, varTypes.get(var.name))
                handled = true
                return
            }
        }

        onMethodSelection { expr, MethodNode methodNode ->
            def descriptor = toMethodDescriptor(methodNode)
            if (null == descriptor.declaringClass || descriptor.declaringClass.name != 'java.lang.System')
                addStaticTypeError("Not authorized to call this method: $descriptor", expr)
        }
    }

}
