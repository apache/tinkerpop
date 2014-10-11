package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubgraphStrategyTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(MODERN)
    public void testVertexCriterion() throws Exception {
        final Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
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
        try {
            sg.e(convertToEdgeId("peter", "created", "lop"));
            fail("Edge 12 should not be in the graph because peter is not a vertex");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }

        assertEquals(2, g.e(convertToEdgeId("marko", "knows", "vadas")).bothV().count().next().longValue());
        try {
            sg.e(convertToEdgeId("marko", "knows", "vadas"));
            fail("Edge 7 should not be in the graph because marko is not a vertex");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterEdgeCriterion() throws Exception {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> {
            // 8
            if (edge.<Double>value("weight") == 1.0d && edge.label().equals("knows"))
                return true;
                // 9
            else if (edge.<Double>value("weight") == 0.4d && edge.label().equals("created") && edge.outV().next().value("name").equals("marko"))
                return true;
                // 10
            else if (edge.<Double>value("weight") == 1.0d && edge.label().equals("created"))
                return true;
            else return false;
        };

        final GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
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
    @LoadGraphWith(MODERN)
    public void shouldFilterMixedCriteria() throws Exception {
        final Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
        final Predicate<Edge> edgeCriterion = edge -> {
            // 9 isn't present because marko is not in the vertex list
            // 11
            if (edge.<Double>value("weight") == 0.4d && edge.label().equals("created"))
                return true;
                // 10
            else if (edge.<Double>value("weight") == 1.0d && edge.label().equals("created"))
                return true;
            else return false;
        };

        final GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
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
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
        try {
            sg.e(convertToEdgeId("marko", "created", "lop"));
            fail("Edge 9 should not be in the graph because marko is not a vertex");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(MODERN)
    public void shouldGetExcludedVertex() throws Exception {
        final Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        sg.v(convertToVertexId("marko"));
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(MODERN)
    public void shouldGetExcludedEdge() throws Exception {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> {
            // 8
            if (edge.<Double>value("weight") == 1.0d && edge.label().equals("knows"))
                return true;
                // 9
            else if (edge.<Double>value("weight") == 0.4d && edge.label().equals("created") && edge.outV().next().value("name").equals("marko"))
                return true;
                // 10
            else if (edge.<Double>value("weight") == 1.0d && edge.label().equals("created"))
                return true;
            else return false;
        };

        final GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        sg.e(sg.e(convertToEdgeId("marko", "knows", "vadas")));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldReturnWrappedVertexToString() {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategy = new SubgraphStrategy(vertexCriterion, edgeCriterion);

        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(strategy);
        final Vertex v1 = swg.addVertex(T.label, "Person");
        final Vertex originalVertex = ((StrategyWrappedVertex) v1).getBaseVertex();
        assertEquals(StringFactory.graphStrategyVertexString(swg.strategy().getGraphStrategy().get(), originalVertex), v1.toString());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldReturnWrappedEdgeToString() {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategy = new SubgraphStrategy(vertexCriterion, edgeCriterion);

        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(strategy);
        final Vertex v1 = swg.addVertex(T.label, "Person");
        final Vertex v2 = swg.addVertex(T.label, "Person");
        final Edge e1 = v1.addEdge("friend", v2);
        final Edge originalEdge = ((StrategyWrappedEdge) e1).getBaseEdge();
        assertEquals(StringFactory.graphStrategyEdgeString(swg.strategy().getGraphStrategy().get(), originalEdge), e1.toString());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldReturnWrappedVertexPropertyToString() {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategy = new SubgraphStrategy(vertexCriterion, edgeCriterion);

        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(strategy);
        final Vertex v1 = swg.addVertex(T.label, "Person", "age", "one");
        final VertexProperty age = v1.property("age");
        final Vertex originalVertex = ((StrategyWrappedVertex) v1).getBaseVertex();
        final VertexProperty originalVertexProperty = originalVertex.property("age");
        assertEquals(StringFactory.graphStrategyPropertyString(swg.strategy().getGraphStrategy().get(), originalVertexProperty), age.toString());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldReturnWrappedPropertyToString() {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategy = new SubgraphStrategy(vertexCriterion, edgeCriterion);

        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(strategy);
        final Vertex v1 = swg.addVertex(T.label, "Person");
        final Vertex v2 = swg.addVertex(T.label, "Person");
        final Edge e1 = v1.addEdge("friend", v2, "weight", "fifty");
        final Property weight = e1.property("weight");
        final Edge originalEdge = ((StrategyWrappedEdge) e1).getBaseEdge();
        final Property originalProperty = originalEdge.property("weight");
        assertEquals(StringFactory.graphStrategyPropertyString(swg.strategy().getGraphStrategy().get(), originalProperty), weight.toString());
    }

    @Test
    public void shouldReturnWrappedToString() {
        final Predicate<Vertex> vertexCriterion = vertex -> true;
        final Predicate<Edge> edgeCriterion = edge -> true;

        final GraphStrategy strategy = new SubgraphStrategy(vertexCriterion, edgeCriterion);

        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(strategy);
        assertNotEquals(g.toString(), swg.toString());
        assertEquals(StringFactory.graphStrategyString(strategy, g), swg.toString());
    }
}
