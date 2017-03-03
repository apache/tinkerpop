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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite.ENGINE_TO_TEST;
import static org.junit.Assert.assertEquals;

/**
 * This is an important test case in that it validates that core features of {@code ScriptEngine} instances that claim
 * to be "Gremlin-enabled" work in the expected fashion.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinEnabledScriptEngineTest {
    private static final GremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();

    @Test
    public void shouldGetEngineByName() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(ENGINE_TO_TEST, scriptEngine.getFactory().getEngineName());
    }

    @Test
    public void shouldHaveCoreImportsInPlace() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final List<Class> classesToCheck = Arrays.asList(Vertex.class, Edge.class, Graph.class, VertexProperty.class);
        for (Class clazz : classesToCheck) {
            assertEquals(clazz, scriptEngine.eval(clazz.getSimpleName()));
        }
    }

    @Test
    public void shouldReturnNoCustomizers() {
        final GremlinScriptEngineManager mgr = new DefaultGremlinScriptEngineManager();
        mgr.addPlugin(ImportGremlinPlugin.build()
                .classImports(java.awt.Color.class)
                .appliesTo(Collections.singletonList("fake-script-engine")).create());
        assertEquals(0, mgr.getCustomizers(ENGINE_TO_TEST).size());
    }
}
