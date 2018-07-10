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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ReadTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Map<String,Object>, Map<String,Object>> get_g_read()  throws IOException;

    @Test
    public void g_read() throws IOException {
        final Traversal<Map<String,Object>, Map<String,Object>> traversal = get_g_read();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());

        IoTest.assertModernGraph(graph, false, true);
    }

    public static class Traversals extends ReadTest {
        @Override
        public Traversal<Map<String,Object>, Map<String,Object>> get_g_read() throws IOException {
            final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GryoResourceAccess.class, "tinkerpop-modern-v3d0.kryo", "").getAbsolutePath().replace('\\', '/');
            return g.read(fileToRead);
        }
    }
}
