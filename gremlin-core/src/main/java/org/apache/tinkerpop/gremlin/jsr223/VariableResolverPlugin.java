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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A plugin that allows for the configuration of a {@link VariableResolver} implementation to be used by the
 * {@link GremlinLangScriptEngine}. By default, it will use the {@link VariableResolver.DirectVariableResolver} which
 * directly resolves variable name to a value from the binding in the script engine context. This is the most common
 * usage relevant for most users and providers. Other options are reserved for more advanced use cases.
 */
public class VariableResolverPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.variableResolver";

    public static final Map<String, Function<Map<String, Object>, VariableResolver>> VARIABLE_RESOLVERS =
            new HashMap<String, Function<Map<String, Object>, VariableResolver>>() {{
                put(VariableResolver.DirectVariableResolver.class.getSimpleName(), VariableResolver.DirectVariableResolver::new);
                put(VariableResolver.DefaultVariableResolver.class.getSimpleName(), VariableResolver.DefaultVariableResolver::new);
                put(VariableResolver.NoVariableResolver.class.getSimpleName(), m -> VariableResolver.NoVariableResolver.instance());
                put(VariableResolver.NullVariableResolver.class.getSimpleName(), m -> VariableResolver.NullVariableResolver.instance());
    }};

    private VariableResolverPlugin(final VariableResolverPlugin.Builder builder) {
        super(NAME, new VariableResolverCustomizer(builder.variableResolverMaker));
    }

    /**
     * Builds a set of static bindings.
     */
    public static VariableResolverPlugin.Builder build() {
        return new VariableResolverPlugin.Builder();
    }

    public static final class Builder {

        Function<Map<String,Object>, VariableResolver> variableResolverMaker = VariableResolver.DirectVariableResolver::new;

        private Builder() {}

        /**
         * Sets the type of {@link VariableResolver} to use by specifying a simple class name associated with the
         * inner classes in that interface or a fully qualified class name. The assumption is that implementations
         * will allow a constructor that takes a {@code Map} which contains the bindings from the script engine context.
         * Implementations are can then decide how to resolve variables in the script based on that {@code Map} or some
         * other mechanism.
         */
        public VariableResolverPlugin.Builder resolver(final String resolverName) {
            if (VARIABLE_RESOLVERS.containsKey(resolverName)) {
                this.variableResolverMaker = VARIABLE_RESOLVERS.get(resolverName);
            } else {
                try {
                    // Assuming resolverName is a fully qualified class name if it's not in the simple name map
                    final Class<?> clazz = Class.forName(resolverName);
                    if (VariableResolver.class.isAssignableFrom(clazz)) {
                        this.variableResolverMaker = map -> {
                            try {
                                return (VariableResolver) clazz.getConstructor(Map.class).newInstance(map);
                            } catch (Exception e) {
                                throw new RuntimeException("Error instantiating VariableResolver", e);
                            }
                        };
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("VariableResolver class not found: " + resolverName, e);
                }
            }
            return this;
        }

        public VariableResolverPlugin create() {
            return new VariableResolverPlugin(this);
        }
    }
}
