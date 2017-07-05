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

/**
 * This is a "do nothing" implementation of the {@link GremlinScriptEngineFactory} which can be used to help test plugin
 * implementations which don't have reference to a {@link GremlinScriptEngine} as a dependency.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MockGremlinScriptEngineFactory extends AbstractGremlinScriptEngineFactory {

    public static final String ENGINE_NAME = "gremlin-mock";

    public MockGremlinScriptEngineFactory() {
        super(ENGINE_NAME, ENGINE_NAME, Collections.singletonList("script"), Collections.singletonList("plain"));
    }

    @Override
    public GremlinScriptEngine getScriptEngine() {
        return new MockGremlinScriptEngine();
    }

    @Override
    public String getMethodCallSyntax(final String obj, final String m, final String... args) {
        return null;
    }

    @Override
    public String getOutputStatement(final String toDisplay) {
        return null;
    }
}
