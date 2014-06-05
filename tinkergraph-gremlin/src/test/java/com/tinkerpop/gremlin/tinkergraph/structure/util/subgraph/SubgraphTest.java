package com.tinkerpop.gremlin.tinkergraph.structure.util.subgraph;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
import com.tinkerpop.gremlin.structure.strategy.SubgraphStrategy;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
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
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubgraphTest {

    @Test
    public void testVertexCriterion() throws Exception {
		final Graph g = TinkerFactory.createClassic();

		final Predicate<Vertex> vertexCriterion = vertex -> (int) vertex.id() < 4;
		final Predicate<Edge> edgeCriterion = edge -> true;

        //Subgraph sg = new Subgraph(g, vertexCriterion, edgeCriterion);

		final Optional<GraphStrategy> strategyToTest = Optional.<GraphStrategy>of(new SubgraphStrategy(vertexCriterion, edgeCriterion));
		final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
		sg.strategy().setGraphStrategy(strategyToTest);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count());
        assertEquals(3, sg.V().count());

        // only two edges are present, even though edges are not explicitly excluded
        // (edges require their incident vertices)
        assertEquals(6, g.E().count());
        assertEquals(2, sg.E().count());

		final Vertex v1_g = g.v(1);
		final Vertex v1_sg = sg.v(1);
        assertEquals(2, v1_g.out("knows").count());
        assertEquals(1, v1_sg.out("knows").count());

        assertEquals(2, g.v(1).out("knows").count());
        assertEquals(1, sg.v(1).out("knows").count());

        assertEquals(2, g.v(1).outE("knows").count());
        assertEquals(1, sg.v(1).outE("knows").count());
    }

    @Test
    public void testEdgeCriterion() throws Exception {
        Set<Integer> includedEdgeIds = new HashSet<>();
        includedEdgeIds.add(8);
        includedEdgeIds.add(9);
        includedEdgeIds.add(10);

        Graph g = TinkerFactory.createClassic();

        //*
        Function<Vertex, Boolean> vertexCriterion = vertex -> true;
        Function<Edge, Boolean> edgeCriterion = edge -> includedEdgeIds.contains((int) edge.id());

        Subgraph sg = new Subgraph(g, vertexCriterion, edgeCriterion);
        //*/

        /*
        Predicate<Vertex> vertexCriterion = vertex -> true;
        Predicate<Edge> edgeCriterion = edge -> includedEdgeIds.contains((int) edge.id());

        final Optional<GraphStrategy> strategyToTest = Optional.<GraphStrategy>of(new SubgraphStrategy(vertexCriterion, edgeCriterion));
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);
        //*/

        // all vertices are here
        assertEquals(6, g.V().count());
        assertEquals(6, sg.V().count());

        // only the given edges are included
        assertEquals(6, g.E().count());
        assertEquals(3, sg.E().count());

        assertEquals(2, g.v(1).outE("knows").count());
        assertEquals(1, sg.v(1).outE("knows").count());

        assertEquals(2, g.v(1).outE("knows").count());
        assertEquals(1, sg.v(1).outE("knows").count());

        // wrapped Traversal<Vertex, Vertex> takes into account the edges it must pass through
        assertEquals(2, g.v(1).out("knows").count());
        assertEquals(1, sg.v(1).out("knows").count());
        assertEquals(2, g.v(4).out("created").count());
        assertEquals(1, sg.v(4).out("created").count());
    }
}
