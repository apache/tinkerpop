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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Resolves parameters in Gremlin to objects.
 */
public interface VariableResolver<T> extends BiFunction<String, GremlinParser.VariableContext, T> {

    /**
     * This function resolves a variable name and the given parsers context to an object.
     */
    @Override
    T apply(final String varName, final GremlinParser.VariableContext variableContext);

    /**
     * This {@link VariableResolver} implementation throws exceptions for all variable names.
     */
    class NoVariableResolver implements VariableResolver<Object> {
        private static NoVariableResolver instance = new NoVariableResolver();

        public static VariableResolver instance() {
            return instance;
        }

        @Override
        public Object apply(final String s, final GremlinParser.VariableContext variableContext) {
            throw new VariableResolverException(String.format("No variable found for %s", s));
        }
    }

    /**
     * Allows for a provided variable set in the form of a {@code Map}, where the keys are variable names and the
     * values are the parameter values to be injected into the traversal in their place. The value is provided to a
     * {@link GValue} object along with the variable name for further reference.
     */
    class DefaultVariableResolver implements VariableResolver<GValue<?>> {

        private final Map<String, Object> variables;

        public DefaultVariableResolver(final Map<String, Object> variables) {
            this.variables = variables;
        }

        @Override
        public GValue<?> apply(final String s, final GremlinParser.VariableContext variableContext) {
            if (!variables.containsKey(s)) {
                throw new VariableResolverException(String.format("No variable found for %s", s));
            }

            return GValue.of(s, variables.get(s));
        }
    }

    /**
     * This {@link VariableResolver} simply provides a {@code null} value for all variable names. It's typical use
     * is for when you really don't intend to execute the traversal and just want to get an instance of one when
     * bindings are being used as with {@link NoOpTerminalVisitor}.
     */
    class NullVariableResolver implements VariableResolver<Object> {
        private static NullVariableResolver instance = new NullVariableResolver();

        public static VariableResolver instance() {
            return instance;
        }

        @Override
        public Object apply(final String s, final GremlinParser.VariableContext variableContext) {
            return null;
        }
    }

    /**
     * Allows for a provided variable set in the form of a {@code Map}, where the keys are variable names and the
     * values are the parameter values to be injected into the traversal in their place.
     */
    class DirectVariableResolver implements VariableResolver<Object> {

        private final Map<String, Object> variables;

        public DirectVariableResolver(final Map<String, Object> variables) {
            this.variables = variables;
        }

        @Override
        public Object apply(final String s, final GremlinParser.VariableContext variableContext) {
            if (!variables.containsKey(s)) {
                throw new VariableResolverException(String.format("No variable found for %s", s));
            }

            return variables.get(s);
        }
    }

}