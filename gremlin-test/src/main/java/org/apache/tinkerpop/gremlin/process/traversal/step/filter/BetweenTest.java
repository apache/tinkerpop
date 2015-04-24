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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class BetweenTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outE_betweenXweight_0_06X_inV(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_betweenXweight_0_06X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_betweenXweight_0_06X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
        }
        assertFalse(traversal.hasNext());
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends BetweenTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_betweenXweight_0_06X_inV(final Object v1Id) {
            return g.V(v1Id).outE().between("weight", 0.0d, 0.6d).inV();
        }
    }
}