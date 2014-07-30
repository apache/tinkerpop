package com.tinkerpop.gremlin.process.computer.ranking;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgramTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(CLASSIC)
    public void testPageRank() throws Exception {
        final Pair<Graph, SideEffects> pair = g.compute().program(PageRankVertexProgram.build().create()).submit().get();
        final Graph viewGraph = pair.getValue0();
        final SideEffects sideEffects = pair.getValue1();
        viewGraph.V().forEach(v -> {
            assertTrue(v.keys().contains("name"));
            assertTrue(v.hiddenKeys().contains(Graph.Key.unHide(PageRankVertexProgram.PAGE_RANK)));
            //System.out.println(v.value("name") + ":" + v.value(PageRankVertexProgram.PAGE_RANK));
            final String name = v.value("name");
            final Double pageRank = v.value(PageRankVertexProgram.PAGE_RANK);
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
        assertEquals(sideEffects.getIteration(), 30);
    }

}