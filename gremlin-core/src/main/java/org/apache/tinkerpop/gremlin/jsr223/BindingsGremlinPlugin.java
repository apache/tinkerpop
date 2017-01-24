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
import javax.script.SimpleBindings;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BindingsGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.bindings";

    private BindingsGremlinPlugin(final Builder builder) {
        this(builder.bindings);
    }

    public BindingsGremlinPlugin(final Bindings bindings) {
        super(NAME, new DefaultBindingsCustomizer(bindings));
    }

    public BindingsGremlinPlugin(final Supplier<Bindings> bindingsSupplier) {
        super(NAME, new LazyBindingsCustomizer(bindingsSupplier));
    }

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
