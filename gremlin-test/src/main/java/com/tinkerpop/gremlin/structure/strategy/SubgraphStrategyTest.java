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
        Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
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

        assertEquals(2, g.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.v(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with label and branch factor

        assertEquals(1, g.v(convertToVertexId("josh")).outE(1, "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).outE(1, "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).out(1, "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).out(1, "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1, "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1, "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1, "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1, "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(convertToEdgeId("josh", "created", "lop")).bothV().count().next().longValue());
        assertEquals(2, sg.e(convertToEdgeId("josh", "created", "lop")).bothV().count().next().longValue());

        assertEquals(2, g.e(convertToEdgeId("peter", "created", "lop")).bothV().count().next().longValue());
        assertEquals(1, sg.e(convertToEdgeId("peter", "created", "lop")).bothV().count().next().longValue());

        assertEquals(2, g.e(convertToEdgeId("marko", "knows", "vadas")).bothV().count().next().longValue());
        assertEquals(0, sg.e(convertToEdgeId("marko", "knows", "vadas")).bothV().count().next().longValue());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void shouldFilterEdgeCriterion() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> true;
        Predicate<Edge> edgeCriterion = edge -> {
            // 8
            if (edge.<Float>value("weight") == 1.0f && edge.label().equals("knows"))
                return true;
                // 9
            else if (edge.<Float>value("weight") == 0.4f && edge.label().equals("created") && edge.outV().next().value("name").equals("marko"))
                return true;
                // 10
            else if (edge.<Float>value("weight") == 1.0f && edge.label().equals("created"))
                return true;
            else return false;
        };

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        // all vertices are here
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(6, sg.V().count().next().longValue());

        // only the given edges are included
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(3, sg.E().count().next().longValue());

        assertEquals(2, g.v(convertToVertexId("marko")).outE("knows").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("marko")).outE("knows").count().next().longValue());

        // wrapped Traversal<Vertex, Vertex> takes into account the edges it must pass through
        assertEquals(2, g.v(convertToVertexId("marko")).out("knows").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("marko")).out("knows").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).out("created").count().next().longValue());

        // from vertex

        assertEquals(2, g.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.v(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1).count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1).count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1).count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1).count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(convertToEdgeId("marko", "knows", "josh")).bothV().count().next().longValue());
        assertEquals(2, sg.e(convertToEdgeId("marko", "knows", "josh")).bothV().count().next().longValue());

        assertEquals(3, g.e(convertToEdgeId("marko", "knows", "josh")).outV().outE().count().next().longValue());
        assertEquals(2, sg.e(convertToEdgeId("marko", "knows", "josh")).outV().outE().count().next().longValue());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void shouldFilterMixedCriteria() throws Exception {
        Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
        Predicate<Edge> edgeCriterion = edge -> {
            // 9 && 11
            if (edge.<Float>value("weight") == 0.4f && edge.label().equals("created"))
                return true;
                // 10
            else if (edge.<Float>value("weight") == 1.0f && edge.label().equals("created"))
                return true;
            else return false;
        };

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // three edges are explicitly included, but one is missing its out-vertex due to the vertex criterion
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(2, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.v(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1).count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1).count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1).count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1).count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
// TODO: Why Neo4j not happy?
//        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
        assertEquals(1, sg.e(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(CLASSIC)
    public void shouldGetExcludedVertex() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
        Predicate<Edge> edgeCriterion = edge -> true;

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        sg.v(convertToVertexId("marko"));
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(CLASSIC)
    public void shouldGetExcludedEdge() throws Exception {

        Predicate<Vertex> vertexCriterion = vertex -> true;
        Predicate<Edge> edgeCriterion = edge -> {
            // 8
            if (edge.<Float>value("weight") == 1.0f && edge.label().equals("knows"))
                return true;
                // 9
            else if (edge.<Float>value("weight") == 0.4f && edge.label().equals("created") && edge.outV().next().value("name").equals("marko"))
                return true;
                // 10
            else if (edge.<Float>value("weight") == 1.0f && edge.label().equals("created"))
                return true;
            else return false;
        };

        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        sg.e(sg.e(convertToEdgeId("marko", "knows", "vadas")));
    }
}
