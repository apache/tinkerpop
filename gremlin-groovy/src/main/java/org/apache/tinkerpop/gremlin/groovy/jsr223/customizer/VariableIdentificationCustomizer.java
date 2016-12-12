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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer;

import org.codehaus.groovy.ast.ClassCodeVisitorSupport;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, not replaced.
 */
@Deprecated
public class VariableIdentificationCustomizer extends CompilationCustomizer {

    private static final ThreadLocal<Set<String>> variables = new ThreadLocal<Set<String>>()  {
        @Override
        protected Set<String> initialValue() {
            return new TreeSet<>();
        }
    };

    private static final List<String> variablesToIgnore = new ArrayList<>(Arrays.asList("this", "args", "context", "super"));

    public VariableIdentificationCustomizer() {
        super(CompilePhase.CLASS_GENERATION);
    }

    public Set<String> getVariables() {
        return variables.get();
    }

    public void clearVariables() {
        variables.get().clear();
    }

    @Override
    public void call(final SourceUnit sourceUnit, final GeneratorContext generatorContext,
                     final ClassNode classNode) throws CompilationFailedException {
        classNode.visitContents(new Visitor(sourceUnit));
    }

    class Visitor extends ClassCodeVisitorSupport {

        private SourceUnit sourceUnit;

        public Visitor(final SourceUnit sourceUnit) {
            this.sourceUnit = sourceUnit;
        }

        @Override
        public void visitVariableExpression(final VariableExpression expression) {
            if (!variablesToIgnore.contains(expression.getName())
                    && expression.getAccessedVariable().isDynamicTyped()) {
                variables.get().add(expression.getName());
            }

            super.visitVariableExpression(expression);
        }

        @Override
        protected SourceUnit getSourceUnit() {
            return sourceUnit;
        }
    }
}
