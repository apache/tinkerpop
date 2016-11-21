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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinModule implements GremlinModule {
    protected final String moduleName;
    protected final Customizer[] customizers;
    protected final Set<String> appliesTo;

    /**
     * Creates a base {@link GremlinModule} that will apply to all {@link GremlinScriptEngine} instances.
     */
    public AbstractGremlinModule(final String moduleName, final Customizer... customizers) {
        this(moduleName, Collections.emptySet(), customizers);
    }
    /**
     * Creates a base {@link GremlinModule} that will apply to specific {@link GremlinScriptEngine} instances.
     */
    public AbstractGremlinModule(final String moduleName, final Set<String> appliesTo, final Customizer... customizers) {
        this.moduleName = moduleName;
        this.appliesTo = appliesTo;
        this.customizers = customizers;
    }

    @Override
    public String getName() {
        return moduleName;
    }

    @Override
    public Optional<Customizer[]> getCustomizers(final String scriptEngineName) {
        return null == scriptEngineName || appliesTo.isEmpty() || appliesTo.contains(scriptEngineName) ?
                Optional.of(customizers) : Optional.empty();
    }
}
