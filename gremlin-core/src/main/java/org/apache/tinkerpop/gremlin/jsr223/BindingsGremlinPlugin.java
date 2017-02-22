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
 * A module that allows {@code Bindings} to be applied to a {@link GremlinScriptEngine}. The bindings are controled by
 * their {@code scope} which are determined by the {@code ScriptContext} values where "100" is {@code ENGINE_SCOPE}
 * (bindings apply to the current {@link GremlinScriptEngine}) and "200" is {@code GLOBAL_SCOPE} (bindings apply to the
 * engines created by the current {@link GremlinScriptEngineManager}.
 * <p/>
 * Note that bindings are applied in the following order:
 * <ol>
 *   <li>The order in which the {@link GremlinScriptEngine} is requested from the {@link GremlinScriptEngineManager}</li>
 *   <li>The order in which the {@code BindingsGremlinPlugin} instances are added to the {@link GremlinScriptEngineManager}</li>
 * </ol>
 * <p/>
 * Moreover, they will override one another within a scope and among scopes. For instance, {@code ENGINE_SCOPE} bindings
 * will override {@code GLOBAL_SCOPE}. Those bindings that are {@code GLOBAL_SCOPE} and applied to a single
 * {@link GremlinScriptEngine} via an {@link Builder#appliesTo} configuration will still appear present to all other
 * {@link GremlinScriptEngine} created by the {@link GremlinScriptEngineManager} that the plugin was bound to.
 * <p/>
 * This {@link GremlinPlugin} is not enabled for the {@code ServiceLoader}. It is designed to be instantiated manually.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BindingsGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.bindings";

    private BindingsGremlinPlugin(final Builder builder) {
        super(NAME, builder.appliesTo, new DefaultBindingsCustomizer(builder.bindings, builder.scope));
    }

    public BindingsGremlinPlugin(final Bindings bindings, final int scope) {
        super(NAME, new DefaultBindingsCustomizer(bindings, scope));
    }

    public BindingsGremlinPlugin(final Supplier<Bindings> bindingsSupplier, final int scope) {
        super(NAME, new LazyBindingsCustomizer(bindingsSupplier, scope));
    }

    public static BindingsGremlinPlugin.Builder build() {
        return new Builder();
    }

    public static final class Builder {

        private Bindings bindings = new SimpleBindings();
        private int scope = ScriptContext.ENGINE_SCOPE;
        private final Set<String> appliesTo = new HashSet<>();

        private Builder() {}

        /**
         * The name of the {@link GremlinScriptEngine} that this module will apply to. Setting no values here will
         * make the module available to all the engines.
         */
        public BindingsGremlinPlugin.Builder appliesTo(final Collection<String> scriptEngineName) {
            this.appliesTo.addAll(scriptEngineName);
            return this;
        }

        public Builder bindings(final Map<String, Object> bindings) {
            this.bindings = new SimpleBindings(bindings);
            return this;
        }

        public Builder scope(final int scope) {
            this.scope = scope;
            return this;
        }

        public BindingsGremlinPlugin create() {
            return new BindingsGremlinPlugin(this);
        }
    }
}
