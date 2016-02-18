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

import org.codehaus.groovy.ast.DynamicVariable
import org.codehaus.groovy.ast.GroovyClassVisitor
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.control.CompilationUnit
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.Phases
import org.codehaus.groovy.tools.shell.util.ScriptVariableAnalyzer

import java.security.CodeSource

/**
 * An extension of Groovy's {@code VariableVisitor} that exposes the bound and unbound variables publicly. This
 * class can likely be removed with the next update of Groovy (after 2.4.5) as the {code ScriptVariableAnalyzer} ends
 * up exposing {@code getBoundVars() in such a way as to allow for the {@code ClassLoader} to be supplied.
 */
class GremlinVariableAnalyzer {
    public static class GremlinVariableVisitor extends ScriptVariableAnalyzer.VariableVisitor {
        String lastBound

        @Override
        void visitVariableExpression(VariableExpression expression) {
            if (!(expression.variable in ['args', 'context', 'this', 'super'])) {
                if (expression.accessedVariable instanceof DynamicVariable) {
                    unbound << expression.variable
                } else {
                    bound << expression.variable
                    lastBound = bound
                }
            }
            super.visitVariableExpression(expression)
        }

        @Override
        public Set<String> getBound() {
            return super.getBound()
        }

        @Override
        public Set<String> getUnbound() {
            return super.getUnbound()
        }
    }

    public static class GremlinVisitorClassLoader extends GroovyClassLoader {
        private final GroovyClassVisitor visitor

        public GremlinVisitorClassLoader(final GroovyClassVisitor visitor, ClassLoader parent) {
            super(parent == null ?  Thread.currentThread().getContextClassLoader() : parent)
            this.visitor = visitor
        }

        @Override
        protected CompilationUnit createCompilationUnit(final CompilerConfiguration config, final CodeSource source) {
            CompilationUnit cu = super.createCompilationUnit(config, source)
            cu.addPhaseOperation(new ScriptVariableAnalyzer.VisitorSourceOperation(visitor), Phases.CLASS_GENERATION)
            return cu
        }
    }

    public static BoundVars getBoundVars(final String scriptText, ClassLoader parent) {
        assert scriptText != null
        final GroovyClassVisitor visitor = new GremlinVariableVisitor()
        new GremlinVisitorClassLoader(visitor, parent).parseClass(scriptText)
        return new BoundVars(visitor.getLastBound(), visitor.getBound())
    }

    public static class BoundVars {
        private String lastBound;
        private Set<String> bound;

        BoundVars(String lastBound, Set<String> bound) {
            this.lastBound = lastBound
            this.bound = bound
        }

        String getLastBound() {
            return lastBound
        }

        Set<String> getBound() {
            return bound
        }
    }
}
