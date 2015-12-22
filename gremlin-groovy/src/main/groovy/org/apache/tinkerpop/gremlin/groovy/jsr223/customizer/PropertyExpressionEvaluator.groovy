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

import org.codehaus.groovy.ast.ClassCodeVisitorSupport
import org.codehaus.groovy.ast.ClassHelper
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.transform.sc.StaticCompilationMetadataKeys
import org.codehaus.groovy.transform.stc.GroovyTypeCheckingExtensionSupport
import org.codehaus.groovy.transform.stc.StaticTypeCheckingSupport

/**
 * Evaluates methods selected by groovy property magic (i.e. {@code Person.name} -> {@code Person.getName()}).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PropertyExpressionEvaluator extends ClassCodeVisitorSupport {
    private final SourceUnit unit
    private final List<String> methodWhiteList
    private final GroovyTypeCheckingExtensionSupport.TypeCheckingDSL dsl

    public PropertyExpressionEvaluator(final SourceUnit unit, final List<String> whiteList,
                                       final GroovyTypeCheckingExtensionSupport.TypeCheckingDSL dsl) {
        this.unit = unit
        this.methodWhiteList = whiteList
        this.dsl = dsl
    }

    @Override
    protected SourceUnit getSourceUnit() {
        unit
    }

    @Override
    void visitPropertyExpression(final PropertyExpression expression) {
        super.visitPropertyExpression(expression)

        ClassNode owner = expression.objectExpression.getNodeMetaData(StaticCompilationMetadataKeys.PROPERTY_OWNER)
        if (owner) {
            if (expression.spreadSafe && StaticTypeCheckingSupport.implementsInterfaceOrIsSubclassOf(owner, classNodeFor(Collection)))
                owner = typeCheckingVisitor.inferComponentType(owner, ClassHelper.int_TYPE)

            def descriptor = "${SandboxHelper.prettyPrint(owner)}#${expression.propertyAsString}"
            if (!methodWhiteList.any { descriptor ==~ it })
                dsl.addStaticTypeError("Not authorized to call this method: $descriptor", expression)
        }
    }
}
