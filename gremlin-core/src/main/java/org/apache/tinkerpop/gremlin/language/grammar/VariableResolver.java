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

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Resolves parameters in Gremlin to objects.
 */
public interface VariableResolver extends BiFunction<String, GremlinParser.VariableContext, Object> {

    /**
     * This function resolves a variable name and the given parsers context to an object.
     */
    @Override
    Object apply(final String varName, final GremlinParser.VariableContext variableContext);

    /**
     * This {@link VariableResolver} implementation throws exceptions for all variable names.
     */
    class NoVariableResolver implements VariableResolver {
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
     * values are the parameter values to be injected into the traversal in their place.
     */
    class DefaultVariableResolver implements VariableResolver {

        private final Map<String, Object> variables;

        public DefaultVariableResolver(final Map<String, Object> variables) {
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