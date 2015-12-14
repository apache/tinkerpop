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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.codehaus.groovy.ast.ClassCodeVisitorSupport
import org.codehaus.groovy.ast.ClassHelper
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.transform.sc.StaticCompilationMetadataKeys
import org.codehaus.groovy.transform.stc.ExtensionMethodNode
import org.codehaus.groovy.transform.stc.GroovyTypeCheckingExtensionSupport
import org.codehaus.groovy.transform.stc.StaticTypeCheckingSupport

import java.util.function.BiPredicate

/**
 * A sandbox for the {@link GremlinGroovyScriptEngine} that provides base functionality for securing evaluated scripts.
 * By default, this implementation ensures that the variable "graph" is always a {@link Graph} instance and the
 * variable "g" is always a {@link GraphTraversalSource}.
 * <p/>
 * Users should extend this class to modify features as it has some helper methods to make developing a
 * sandbox extension a bit easier than starting from a base groovy type checking extension.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.1.1-incubating, replaced by {@link AbstractSandboxExtension}
 * @see <a href="https://issues.apache.org/jira/browse/TINKERPOP-891">TINKERPOP-891</a>
 */
@Deprecated
class SandboxExtension extends GroovyTypeCheckingExtensionSupport.TypeCheckingDSL {

    /**
     * When assigned to the {@code #variableFilter} all variables are allowed.
     */
    public static final BiPredicate<VariableExpression, Map<String,ClassNode>> VARIABLES_ALLOW_ALL = { var, types -> true }

    /**
     * When assigned to the {@code methodFilter} all methods are allowed.
     */
    public static final BiPredicate<String, MethodNode> METHODS_ALLOW_ALL = { exp, method -> true }

    /**
     * Forces any variable named "graph" to be of type {@link Graph}.
     */
    protected boolean graphIsAlwaysGraphInstance = true

    /**
     * Forces any variable named "g" to be of type {@link GraphTraversalSource}.
     */
    protected boolean gIsAlwaysGraphTraversalSource = true

    protected BiPredicate<VariableExpression, Map<String,ClassNode>> variableFilter = VARIABLES_ALLOW_ALL
    protected BiPredicate<String, MethodNode> methodFilter = METHODS_ALLOW_ALL

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

            // use the types of the bound variables.  filter as necessary and provide special treatment for
            // "g" and "graph" as they are potentially handled above already and don't need to be bound
            // implicitly by the binding variables
            if (varTypes.containsKey(var.name) && variableFilter.test(var, varTypes))  {
                if (!(var.name in ["graph",  "g"]) || (var.name == "graph" && !graphIsAlwaysGraphInstance
                         || var.name == "g" && !gIsAlwaysGraphTraversalSource)) {
                    storeType(var, varTypes.get(var.name))
                    handled = true
                    return
                }
            }
        }

        onMethodSelection { expr, MethodNode methodNode ->
            def descriptor = toMethodDescriptor(methodNode)
            if (!methodFilter.test(descriptor,methodNode))
                addStaticTypeError("Not authorized to call this method: $descriptor", expr)
        }

        // handles calls to properties
        afterVisitMethod { methodNode ->
            def visitor = new PropertyExpressionEvaluator(context.source)
            visitor.visitMethod(methodNode)
        }
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

    /**
     * Helper method for those extending the sandbox.
     */
    static String prettyPrint(final ClassNode node) {
        node.isArray() ? "${prettyPrint(node.componentType)}[]" : node.toString(false)
    }

    /**
     * Evaluates methods selected by groovy property magic (i.e. {@code Person.name} -> {@code Person.getName()})
     */
    class PropertyExpressionEvaluator extends ClassCodeVisitorSupport {
        private final SourceUnit unit
        private final List<String> whiteList

        public PropertyExpressionEvaluator(final SourceUnit unit) {
            this.unit = unit
            this.whiteList = whiteList
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

                def descriptor = "${prettyPrint(owner)}#${expression.propertyAsString}"
                if (!methodFilter.test(descriptor, expression))
                    addStaticTypeError("Not authorized to call this method: $descriptor", expression)
            }
        }
    }

}
