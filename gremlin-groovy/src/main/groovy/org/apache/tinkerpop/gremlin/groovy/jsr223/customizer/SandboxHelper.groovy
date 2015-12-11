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

import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.transform.stc.ExtensionMethodNode

/**
 * Re-usable utility methods that might be useful in building custom sandboxes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SandboxHelper {
    /**
     * Helper method for those extending the sandbox.
     */
    static String prettyPrint(final ClassNode node) {
        node.isArray() ? "${prettyPrint(node.componentType)}[]" : node.toString(false)
    }

    /**
     * Helper method for those extending the sandbox and useful in turning methods into regex matchable strings.
     */
    static String toMethodDescriptor(final MethodNode node) {
        if (node instanceof ExtensionMethodNode)
            return toMethodDescriptor(node.extensionMethodNode)

        def sb = new StringBuilder()
        sb.append(node.declaringClass.toString(false))
        sb.append("#")
        sb.append(node.name)
        sb.append('(')
        sb.append(node.parameters.collect { Parameter it ->
            prettyPrint(it.originType)
        }.join(','))
        sb.append(')')
        sb
    }
}
