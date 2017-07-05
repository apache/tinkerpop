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

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A plugin that allows {@code Bindings} to be applied to a {@link GremlinScriptEngine} at the time of creation.
 * {@code Bindings} defined with this plugin will always be assigned as {@code ScriptContext.GLOBAL_SCOPE} and as such
 * will be visible to all {@link GremlinScriptEngine} instances.
 * <p/>
 * Note that bindings are applied in the order in which the {@code BindingsGremlinPlugin} instances are added to the
 * {@link GremlinScriptEngineManager}. Therefore if there are two plugins added and both include a variable called "x"
 * then the value of "x" will be the equal to the value provided by the second plugin that overrides the first.
 * <p/>
 * This {@link GremlinPlugin} is not enabled for the {@code ServiceLoader}. It is designed to be instantiated manually.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BindingsGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.bindings";

    private BindingsGremlinPlugin(final Builder builder) {
        super(NAME, new DefaultBindingsCustomizer(builder.bindings));
    }

    BindingsGremlinPlugin(final Supplier<Bindings> bindingsSupplier) {
        super(NAME, new LazyBindingsCustomizer(bindingsSupplier));
    }

    /**
     * Builds a set of static bindings.
     */
    public static BindingsGremlinPlugin.Builder build() {
        return new Builder();
    }

    public static final class Builder {

        private Bindings bindings = new SimpleBindings();

        private Builder() {}

        public Builder bindings(final Map<String, Object> bindings) {
            this.bindings = new SimpleBindings(bindings);
            return this;
        }

        public BindingsGremlinPlugin create() {
            return new BindingsGremlinPlugin(this);
        }
    }
}
