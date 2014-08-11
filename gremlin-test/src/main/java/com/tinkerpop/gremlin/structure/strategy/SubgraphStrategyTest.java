package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubgraphStrategyTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(CLASSIC)
    public void testVertexCriterion() throws Exception {
        Predicate<Vertex> vertexCriterion = vertex -> (int) vertex.id() >= 3 && (int) vertex.id() <= 5;
        Predicate<Edge> edgeCriterion = edge -> true;

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // only two edges are present, even though edges are not explicitly excluded
        // (edges require their incident vertices)
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(2, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.v(4).outE().count().next().longValue());
        assertEquals(2, sg.v(4).outE().count().next().longValue());
        assertEquals(2, g.v(4).out().count().next().longValue());
        assertEquals(2, sg.v(4).out().count().next().longValue());

        assertEquals(1, g.v(4).inE().count().next().longValue());
        assertEquals(0, sg.v(4).inE().count().next().longValue());
        assertEquals(1, g.v(4).in().count().next().longValue());
        assertEquals(0, sg.v(4).in().count().next().longValue());

        assertEquals(3, g.v(4).bothE().count().next().longValue());
        assertEquals(2, sg.v(4).bothE().count().next().longValue());
        assertEquals(3, g.v(4).both().count().next().longValue());
        assertEquals(2, sg.v(4).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(4).outE("created").count().next().longValue());
        assertEquals(2, sg.v(4).outE("created").count().next().longValue());
        assertEquals(2, g.v(4).out("created").count().next().longValue());
        assertEquals(2, sg.v(4).out("created").count().next().longValue());
        assertEquals(2, g.v(4).bothE("created").count().next().longValue());
        assertEquals(2, sg.v(4).bothE("created").count().next().longValue());
        assertEquals(2, g.v(4).both("created").count().next().longValue());
        assertEquals(2, sg.v(4).both("created").count().next().longValue());

        assertEquals(1, g.v(4).inE("knows").count().next().longValue());
        assertEquals(0, sg.v(4).inE("knows").count().next().longValue());
        assertEquals(1, g.v(4).in("knows").count().next().longValue());
        assertEquals(0, sg.v(4).in("knows").count().next().longValue());
        assertEquals(1, g.v(4).bothE("knows").count().next().longValue());
        assertEquals(0, sg.v(4).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(4).both("knows").count().next().longValue());
        assertEquals(0, sg.v(4).both("knows").count().next().longValue());

        // with label and branch factor

        assertEquals(1, g.v(4).outE(1, "created").count().next().longValue());
        assertEquals(1, sg.v(4).outE(1, "created").count().next().longValue());
        assertEquals(1, g.v(4).out(1, "created").count().next().longValue());
        assertEquals(1, sg.v(4).out(1, "created").count().next().longValue());
        assertEquals(1, g.v(4).bothE(1, "created").count().next().longValue());
        assertEquals(1, sg.v(4).bothE(1, "created").count().next().longValue());
        assertEquals(1, g.v(4).both(1, "created").count().next().longValue());
        assertEquals(1, sg.v(4).both(1, "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(11).bothV().count().next().longValue());
        assertEquals(2, sg.e(11).bothV().count().next().longValue());

        assertEquals(2, g.e(12).bothV().count().next().longValue());
        assertEquals(1, sg.e(12).bothV().count().next().longValue());

        assertEquals(2, g.e(7).bothV().count().next().longValue());
        assertEquals(0, sg.e(7).bothV().count().next().longValue());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @org.junit.Ignore   // todo: get test to pass
    public void testEdgeCriterion() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> true;
        Predicate<Edge> edgeCriterion = edge -> (int) edge.id() >= 8 && (int) edge.id() <= 10;

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        // all vertices are here
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(6, sg.V().count().next().longValue());

        // only the given edges are included
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(3, sg.E().count().next().longValue());

        assertEquals(2, g.v(1).outE("knows").count().next().longValue());
        assertEquals(1, sg.v(1).outE("knows").count().next().longValue());

        // wrapped Traversal<Vertex, Vertex> takes into account the edges it must pass through
        assertEquals(2, g.v(1).out("knows").count().next().longValue());
        assertEquals(1, sg.v(1).out("knows").count().next().longValue());
        assertEquals(2, g.v(4).out("created").count().next().longValue());
        assertEquals(1, sg.v(4).out("created").count().next().longValue());

        // from vertex

        assertEquals(2, g.v(4).outE().count().next().longValue());
        assertEquals(1, sg.v(4).outE().count().next().longValue());
        assertEquals(2, g.v(4).out().count().next().longValue());
        assertEquals(1, sg.v(4).out().count().next().longValue());

        assertEquals(1, g.v(4).inE().count().next().longValue());
        assertEquals(1, sg.v(4).inE().count().next().longValue());
        assertEquals(1, g.v(4).in().count().next().longValue());
        assertEquals(1, sg.v(4).in().count().next().longValue());

        assertEquals(3, g.v(4).bothE().count().next().longValue());
        assertEquals(2, sg.v(4).bothE().count().next().longValue());
        assertEquals(3, g.v(4).both().count().next().longValue());
        assertEquals(2, sg.v(4).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(4).outE("created").count().next().longValue());
        assertEquals(1, sg.v(4).outE("created").count().next().longValue());
        assertEquals(2, g.v(4).out("created").count().next().longValue());
        assertEquals(1, sg.v(4).out("created").count().next().longValue());
        assertEquals(2, g.v(4).bothE("created").count().next().longValue());
        assertEquals(1, sg.v(4).bothE("created").count().next().longValue());
        assertEquals(2, g.v(4).both("created").count().next().longValue());
        assertEquals(1, sg.v(4).both("created").count().next().longValue());

        assertEquals(1, g.v(4).inE("knows").count().next().longValue());
        assertEquals(1, sg.v(4).inE("knows").count().next().longValue());
        assertEquals(1, g.v(4).in("knows").count().next().longValue());
        assertEquals(1, sg.v(4).in("knows").count().next().longValue());
        assertEquals(1, g.v(4).bothE("knows").count().next().longValue());
        assertEquals(1, sg.v(4).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(4).both("knows").count().next().longValue());
        assertEquals(1, sg.v(4).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.v(4).bothE(1).count().next().longValue());
        assertEquals(1, sg.v(4).bothE(1).count().next().longValue());
        assertEquals(1, g.v(4).both(1).count().next().longValue());
        assertEquals(1, sg.v(4).both(1).count().next().longValue());
        assertEquals(1, g.v(4).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(4).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, g.v(4).both(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(4).both(1, "knows", "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(8).bothV().count().next().longValue());
        assertEquals(2, sg.e(8).bothV().count().next().longValue());

        assertEquals(3, g.e(8).outV().outE().count().next().longValue());
        assertEquals(2, sg.e(8).outV().outE().count().next().longValue());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @org.junit.Ignore  // todo: get test to pass
    public void testMixedCriteria() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> (int) vertex.id() >= 3 && (int) vertex.id() <= 5;
        Predicate<Edge> edgeCriterion = edge -> (int) edge.id() >= 9 && (int) edge.id() <= 11;

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // three edges are explicitly included, but one is missing its out-vertex due to the vertex criteria
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(2, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.v(4).outE().count().next().longValue());
        assertEquals(2, sg.v(4).outE().count().next().longValue());
        assertEquals(2, g.v(4).out().count().next().longValue());
        assertEquals(2, sg.v(4).out().count().next().longValue());

        assertEquals(1, g.v(4).inE().count().next().longValue());
        assertEquals(0, sg.v(4).inE().count().next().longValue());
        assertEquals(1, g.v(4).in().count().next().longValue());
        assertEquals(0, sg.v(4).in().count().next().longValue());

        assertEquals(3, g.v(4).bothE().count().next().longValue());
        assertEquals(2, sg.v(4).bothE().count().next().longValue());
        assertEquals(3, g.v(4).both().count().next().longValue());
        assertEquals(2, sg.v(4).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(4).outE("created").count().next().longValue());
        assertEquals(2, sg.v(4).outE("created").count().next().longValue());
        assertEquals(2, g.v(4).out("created").count().next().longValue());
        assertEquals(2, sg.v(4).out("created").count().next().longValue());
        assertEquals(2, g.v(4).bothE("created").count().next().longValue());
        assertEquals(2, sg.v(4).bothE("created").count().next().longValue());
        assertEquals(2, g.v(4).both("created").count().next().longValue());
        assertEquals(2, sg.v(4).both("created").count().next().longValue());

        assertEquals(1, g.v(4).inE("knows").count().next().longValue());
        assertEquals(0, sg.v(4).inE("knows").count().next().longValue());
        assertEquals(1, g.v(4).in("knows").count().next().longValue());
        assertEquals(0, sg.v(4).in("knows").count().next().longValue());
        assertEquals(1, g.v(4).bothE("knows").count().next().longValue());
        assertEquals(0, sg.v(4).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(4).both("knows").count().next().longValue());
        assertEquals(0, sg.v(4).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.v(4).bothE(1).count().next().longValue());
        assertEquals(1, sg.v(4).bothE(1).count().next().longValue());
        assertEquals(1, g.v(4).both(1).count().next().longValue());
        assertEquals(1, sg.v(4).both(1).count().next().longValue());
        assertEquals(1, g.v(4).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(4).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, g.v(4).both(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(4).both(1, "knows", "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(9).bothV().count().next().longValue());
        assertEquals(1, sg.e(9).bothV().count().next().longValue());
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(CLASSIC)
    public void testGetExcludedVertex() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> (int) vertex.id() >= 3 && (int) vertex.id() <= 5;
        Predicate<Edge> edgeCriterion = edge -> true;

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        sg.e(1);
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(CLASSIC)
    public void testGetExcludedEdge() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> true;
        Predicate<Edge> edgeCriterion = edge -> (int) edge.id() >= 8 && (int) edge.id() <= 10;

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        sg.e(7);
    }
}
