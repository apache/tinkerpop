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
package org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgramTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecutePageRank() throws Exception {
        if (g.getGraphComputer().get().features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = graph.compute(g.getGraphComputer().get().getClass()).program(PageRankVertexProgram.build().create(graph)).submit().get();
            result.graph().traversal().V().forEachRemaining(v -> {
                assertEquals(4, v.keys().size()); // name, age/lang, edgeCount, pageRank
                assertTrue(v.keys().contains("name"));
                assertTrue(v.keys().contains(PageRankVertexProgram.EDGE_COUNT));
                assertTrue(v.keys().contains(PageRankVertexProgram.PAGE_RANK));
                assertEquals(1, IteratorUtils.count(v.values("name")));
                assertEquals(1, IteratorUtils.count(v.values(PageRankVertexProgram.PAGE_RANK)));
                final String name = v.value("name");
                final Double pageRank = v.value(PageRankVertexProgram.PAGE_RANK);
                //System.out.println(name + "-----" + pageRank);
                if (name.equals("marko"))
                    assertTrue(pageRank > 0.14 && pageRank < 0.16);
                else if (name.equals("vadas"))
                    assertTrue(pageRank > 0.19 && pageRank < 0.20);
                else if (name.equals("lop"))
                    assertTrue(pageRank > 0.40 && pageRank < 0.41);
                else if (name.equals("josh"))
                    assertTrue(pageRank > 0.19 && pageRank < 0.20);
                else if (name.equals("ripple"))
                    assertTrue(pageRank > 0.23 && pageRank < 0.24);
                else if (name.equals("peter"))
                    assertTrue(pageRank > 0.14 && pageRank < 0.16);
                else
                    throw new IllegalStateException("The following vertex should not exist in the graph: " + name);
            });
            assertEquals(result.memory().getIteration(), 30);
            assertEquals(result.memory().asMap().size(), 0);
        }
    }

    /*@Test
    @LoadGraphWith(MODERN)
    public void shouldExecutePageRankWithNormalizedValues() throws Exception {
        final ComputerResult result = graph.compute().program(PageRankVertexProgram.build().vertexCount(6).create()).submit().get();
        final double sum = result.graph().traversal().V().values(PageRankVertexProgram.PAGE_RANK).sum().next();
        System.out.println(sum);
        assertEquals(1.0d,sum,0.01);
    }*/


}