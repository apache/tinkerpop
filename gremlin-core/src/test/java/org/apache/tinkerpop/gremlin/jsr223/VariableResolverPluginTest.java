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

import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

public class VariableResolverPluginTest {

    @Test
    public void shouldProduceNullVariableResolver() {
        final Map<String, Object> bindings = new HashMap<>();
        final VariableResolverPlugin plugin = VariableResolverPlugin.build().
                resolver(VariableResolver.NullVariableResolver.class.getSimpleName()).create();
        final VariableResolver resolver = ((VariableResolverCustomizer) plugin.getCustomizers().
                get()[0]).getVariableResolverMaker().apply(bindings);
        assertThat(resolver, instanceOf(VariableResolver.NullVariableResolver.class));
    }

    @Test
    public void shouldProduceNoVariableResolver() {
        final Map<String, Object> bindings = new HashMap<>();
        final VariableResolverPlugin plugin = VariableResolverPlugin.build().
                resolver(VariableResolver.NoVariableResolver.class.getSimpleName()).create();
        final VariableResolver resolver = ((VariableResolverCustomizer) plugin.getCustomizers().
                get()[0]).getVariableResolverMaker().apply(bindings);
        assertThat(resolver, instanceOf(VariableResolver.NoVariableResolver.class));
    }

    @Test
    public void shouldProduceDefaultVariableResolver() {
        final Map<String, Object> bindings = new HashMap<>();
        final VariableResolverPlugin plugin = VariableResolverPlugin.build().
                resolver(VariableResolver.DefaultVariableResolver.class.getSimpleName()).create();
        final VariableResolver resolver = ((VariableResolverCustomizer) plugin.getCustomizers().
                get()[0]).getVariableResolverMaker().apply(bindings);
        assertThat(resolver, instanceOf(VariableResolver.DefaultVariableResolver.class));
    }

    @Test
    public void shouldProduceDirectVariableResolver() {
        final Map<String, Object> bindings = new HashMap<>();
        final VariableResolverPlugin plugin = VariableResolverPlugin.build().
                resolver(VariableResolver.DirectVariableResolver.class.getSimpleName()).create();
        final VariableResolver resolver = ((VariableResolverCustomizer) plugin.getCustomizers().
                get()[0]).getVariableResolverMaker().apply(bindings);
        assertThat(resolver, instanceOf(VariableResolver.DirectVariableResolver.class));
    }

    @Test
    public void shouldProduceDefaultedVariableResolverWhenNoSpecified() {
        final Map<String, Object> bindings = new HashMap<>();
        final VariableResolverPlugin plugin = VariableResolverPlugin.build().create();
        final VariableResolver resolver = ((VariableResolverCustomizer) plugin.getCustomizers().
                get()[0]).getVariableResolverMaker().apply(bindings);
        assertThat(resolver, instanceOf(VariableResolver.DirectVariableResolver.class));
    }

    @Test
    public void shouldProduceCustomVariableResolverForFullyQualifiedClassName() {
        final Map<String, Object> bindings = new HashMap<>();
        final VariableResolverPlugin plugin = VariableResolverPlugin.build().
                resolver(CustomVariableResolver.class.getName()).create();
        final VariableResolver resolver = ((VariableResolverCustomizer) plugin.getCustomizers().
                get()[0]).getVariableResolverMaker().apply(bindings);
        assertThat(resolver, instanceOf(CustomVariableResolver.class));
    }

    @Test
    public void shouldProduceExceptionVariableResolverIfResolverNotFound() {
        try {
            final VariableResolverPlugin plugin = VariableResolverPlugin.build().
                    resolver("NotAResolver").create();
            fail("The resolver does not exist and should have failed");
        } catch (Exception re) {
            assertThat(re.getMessage(), containsString("VariableResolver class not found: NotAResolver"));
        }
    }

    /**
     * Custom test implementation of {@link VariableResolver} that is not part of the standard set of implementations.
     * This class does nothing.
     */
    public static class CustomVariableResolver implements VariableResolver<Object> {
        public CustomVariableResolver(final Map<String, Object> m) {
        }

        @Override
        public Object apply(final String varName, final GremlinParser.VariableContext variableContext) {
            return null;
        }
    }
}