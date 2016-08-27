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

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite.ENGINE_TO_TEST;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedGremlinScriptEngineManagerTest {
    private static final GremlinScriptEngineManager manager = new CachedGremlinScriptEngineManager();

    @Test
    public void shouldBeSameInstance() {
        final Set<GremlinScriptEngine> scriptEngines = new HashSet<>();
        IntStream.range(0,100).forEach(i -> scriptEngines.add(manager.getEngineByName(ENGINE_TO_TEST)));
        assertEquals(1, scriptEngines.size());
    }

    @Test
    public void shouldBeSameInstanceInSingleManager() {
        final Set<GremlinScriptEngine> scriptEngines = new HashSet<>();
        IntStream.range(0,100).forEach(i -> scriptEngines.add(SingleGremlinScriptEngineManager.get(ENGINE_TO_TEST)));
        assertEquals(1, scriptEngines.size());
    }
}
