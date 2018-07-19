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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.util.CustomId;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * It was hard to test the {@link IO#registry} configuration as a generic test. Opted to test it as a bit of a
 * standalone test with TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraphIoStepTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    @Test
    public void shouldWriteReadWithCustomIoRegistryGryo() throws Exception {
        final UUID uuid = UUID.randomUUID();
        g.addV("person").property("name","stephen").property("custom", new CustomId("a", uuid)).iterate();

        final File file = TestHelper.generateTempFile(TinkerGraphIoStepTest.class, "shouldWriteReadWithCustomIoRegistryGryo", ".kryo");
        g.io(file.getAbsolutePath()).with(IO.registry, CustomId.CustomIdIoRegistry.class.getName()).write().iterate();

        final Graph emptyGraph = TinkerGraph.open();
        final GraphTraversalSource emptyG = emptyGraph.traversal();

        try {
            emptyG.io(file.getAbsolutePath()).read().iterate();
            fail("Can't read without a registry");
        } catch (Exception ignored) {
            // do nothing
        }

        emptyG.io(file.getAbsolutePath()).with(IO.registry, CustomId.CustomIdIoRegistry.instance()).read().iterate();

        assertEquals(1, emptyG.V().has("custom", new CustomId("a", uuid)).count().next().intValue());
    }
}
