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
package org.apache.tinkerpop.gremlin.server.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook;

/**
 * A {@link GremlinPlugin} implementation that adds Gremlin Server specific classes to the imports.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GremlinServerGremlinPlugin extends AbstractGremlinPlugin {
    private static final String MODULE_NAME = "tinkerpop.gremlin-server";
    private static final GremlinServerGremlinPlugin instance = new GremlinServerGremlinPlugin();

    private GremlinServerGremlinPlugin() {
        super(MODULE_NAME, DefaultImportCustomizer.build().addClassImports(LifeCycleHook.class, LifeCycleHook.Context.class).create());
    }

    public static GremlinServerGremlinPlugin instance() {
        return instance;
    }
}
