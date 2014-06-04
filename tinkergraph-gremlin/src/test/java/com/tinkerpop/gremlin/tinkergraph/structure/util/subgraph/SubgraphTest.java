package com.tinkerpop.gremlin.tinkergraph.structure.util.subgraph;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.SubgraphStrategy;
import com.tinkerpop.gremlin.structure.util.subgraph.Subgraph;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubgraphTest {

    @Test
    public void testVertexCriterion() throws Exception {
        Graph g = TinkerFactory.createClassic();

        Function<Vertex, Boolean> vertexCriterion = vertex -> (int) vertex.id() < 4;
        Function<Edge, Boolean> edgeCriterion = edge -> true;

        Subgraph sg = new Subgraph(g, vertexCriterion, edgeCriterion);

        /*
        Optional<GraphStrategy> strategyToTest = Optional.of(new SubgraphStrategy(vertexCriterion, edgeCriterion));
        TinkerGraphGraphProvider gp = new TinkerGraphGraphProvider();
        Graph sg = gp.openTestGraph(gp.standardGraphConfiguration(), strategyToTest);
        */

        // three vertices are included in the subgraph
        assertEquals(6, count(g.V().toList()));
        assertEquals(3, count(sg.V().toList()));

        // only two edges are present, even though edges are not explicitly excluded
        // (edges require their incident vertices)
        assertEquals(6, count(g.E().toList()));
        assertEquals(2, count(sg.E().toList()));

        Vertex v1_g = g.v(1);
        Vertex v1_sg = sg.v(1);
        assertEquals(2, count(v1_g.out("knows").toList()));
        assertEquals(1, count(v1_sg.out("knows").toList()));

        assertEquals(2, count(g.v(1).out("knows").toList()));
        assertEquals(1, count(sg.v(1).out("knows").toList()));

        assertEquals(2, count(g.v(1).outE("knows").toList()));
        assertEquals(1, count(sg.v(1).outE("knows").toList()));
    }

    @Test
    public void testEdgeCriterion() throws Exception {
        Set<Integer> includedEdgeIds = new HashSet<>();
        includedEdgeIds.add(8);
        includedEdgeIds.add(9);
        includedEdgeIds.add(10);

        Graph g = TinkerFactory.createClassic();

        Function<Vertex, Boolean> vertexCriterion = vertex -> true;
        Function<Edge, Boolean> edgeCriterion = edge -> includedEdgeIds.contains((int) edge.id());

        Subgraph sg = new Subgraph(g, vertexCriterion, edgeCriterion);

        // all vertices are here
        assertEquals(6, count(g.V().toList()));
        assertEquals(6, count(sg.V().toList()));

        // only the given edges are included
        assertEquals(6, count(g.E().toList()));
        assertEquals(3, count(sg.E().toList()));

        assertEquals(2, count(g.v(1).outE("knows").toList()));
        assertEquals(1, count(sg.v(1).outE("knows").toList()));

        // wrapped Traversal<Vertex, Vertex> takes into account the edges it must pass through
        assertEquals(2, count(g.v(1).out("knows").toList()));
        assertEquals(1, count(sg.v(1).out("knows").toList()));
        assertEquals(2, count(g.v(4).out("created").toList()));
        assertEquals(1, count(sg.v(4).out("created").toList()));
    }

    private <T> long count(final Iterable<T> iter) {
        long count = 0;
        for (T anIter : iter) {
            count++;
        }

        return count;
    }
}
