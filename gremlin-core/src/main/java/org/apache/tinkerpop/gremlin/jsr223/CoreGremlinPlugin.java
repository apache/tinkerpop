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

import java.util.Optional;

/**
 * This module is required for a {@code ScriptEngine} to be Gremlin-enabled. This {@link GremlinPlugin} is not enabled
 * for the {@code ServiceLoader}. It is designed to be instantiated manually and compliant {@link GremlinScriptEngine}
 * instances will automatically install it by default when created.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class CoreGremlinPlugin implements GremlinPlugin {

    private static final String NAME = "tinkerpop.core";

    private static final ImportCustomizer gremlinCore = DefaultImportCustomizer.build()
            .addClassImports(CoreImports.getClassImports())
            .addFieldImports(CoreImports.getFieldImports())
            .addEnumImports(CoreImports.getEnumImports())
            .addMethodImports(CoreImports.getMethodImports()).create();

    private static final Customizer[] customizers = new Customizer[] {gremlinCore};

    private static final CoreGremlinPlugin INSTANCE = new CoreGremlinPlugin();

    private CoreGremlinPlugin() {}

    public static CoreGremlinPlugin instance() {
        return INSTANCE;
    }

    @Override
    public Optional<Customizer[]> getCustomizers(final String scriptEngineName) {
        return Optional.of(customizers);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
