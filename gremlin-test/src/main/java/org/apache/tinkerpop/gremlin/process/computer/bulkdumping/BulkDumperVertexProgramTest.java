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
package org.apache.tinkerpop.gremlin.process.computer.bulkdumping;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BulkDumperVertexProgramTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldDumpWholeGraph() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.EDGES)) {
            final ComputerResult result = graphProvider.getGraphComputer(graph).program(BulkDumperVertexProgram.build().create(graph)).submit().get();
            result.graph().traversal().V().forEachRemaining(v -> {
                assertEquals(2, v.keys().size());
                assertTrue(v.keys().contains("name"));
                assertTrue(v.keys().contains("age") || v.keys().contains("lang"));
                assertEquals(1, IteratorUtils.count(v.values("name")));
                assertEquals(1, IteratorUtils.count(v.values("age", "lang")));
                final String name = v.value("name");
                if (name.equals("marko")) {
                    assertEquals("person", v.label());
                    assertEquals(Integer.valueOf(29), v.value("age"));
                    assertEquals(3, IteratorUtils.count(v.edges(Direction.OUT)));
                    assertEquals(2, IteratorUtils.count(v.edges(Direction.OUT, "knows")));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.OUT, "created")));
                    assertEquals(0, IteratorUtils.count(v.edges(Direction.IN)));
                } else if (name.equals("vadas")) {
                    assertEquals("person", v.label());
                    assertEquals(Integer.valueOf(27), v.value("age"));
                    assertEquals(0, IteratorUtils.count(v.edges(Direction.OUT)));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.IN)));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.IN, "knows")));
                } else if (name.equals("lop")) {
                    assertEquals("software", v.label());
                    assertEquals("java", v.value("lang"));
                    assertEquals(0, IteratorUtils.count(v.edges(Direction.OUT)));
                    assertEquals(3, IteratorUtils.count(v.edges(Direction.IN)));
                    assertEquals(3, IteratorUtils.count(v.edges(Direction.IN, "created")));
                } else if (name.equals("josh")) {
                    assertEquals("person", v.label());
                    assertEquals(Integer.valueOf(32), v.value("age"));
                    assertEquals(2, IteratorUtils.count(v.edges(Direction.OUT)));
                    assertEquals(2, IteratorUtils.count(v.edges(Direction.OUT, "created")));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.IN)));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.IN, "knows")));
                } else if (name.equals("ripple")) {
                    assertEquals("software", v.label());
                    assertEquals("java", v.value("lang"));
                    assertEquals(0, IteratorUtils.count(v.edges(Direction.OUT)));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.IN)));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.IN, "created")));
                } else if (name.equals("peter")) {
                    assertEquals("person", v.label());
                    assertEquals(Integer.valueOf(35), v.value("age"));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.OUT)));
                    assertEquals(1, IteratorUtils.count(v.edges(Direction.OUT, "created")));
                    assertEquals(0, IteratorUtils.count(v.edges(Direction.IN)));
                } else
                    throw new IllegalStateException("The following vertex should not exist in the graph: " + name);
            });
            assertEquals(3.5, (Double) result.graph().traversal().E().values("weight").sum().next(), 0.01);
            assertEquals(1.5, (Double) result.graph().traversal().E().hasLabel("knows").values("weight").sum().next(), 0.01);
            assertEquals(2.0, (Double) result.graph().traversal().E().hasLabel("created").values("weight").sum().next(), 0.01);
            assertEquals(result.memory().getIteration(), 0);
            assertEquals(result.memory().asMap().size(), 0);
        }
    }
}