package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.GraphProvider;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_PERSISTENCE;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTest extends AbstractGremlinTest {

    /**
     * Ensure compliance with Features by checking that all Features are exposed by the implementation.
     */
    @Test
    public void shouldImplementAndExposeFeatures() {
        final Graph.Features features = g.getFeatures();
        assertNotNull(features);

        final AtomicInteger counter = new AtomicInteger(0);

        // get all features.
        final List<Method> methods = Arrays.asList(features.getClass().getMethods()).stream()
                .filter(m -> Graph.Features.FeatureSet.class.isAssignableFrom(m.getReturnType()))
                .collect(Collectors.toList());

        methods.forEach(m -> {
            try {
                assertNotNull(m.invoke(features));
                counter.incrementAndGet();
            } catch (Exception ex) {
                ex.printStackTrace();
                fail("Exception while dynamically checking compliance on Feature implementation");
            }
        });

        // always should be some feature methods
        assertTrue(methods.size() > 0);

        // ensure that every method exposed was checked
        assertEquals(methods.size(), counter.get());
    }

    /**
     * Graphs should have a standard toString representation where the value starts with the lower case representation
     * of the class name of the Graph instance.
     */
    @Test
    public void shouldHaveStandardStringRepresentation() throws Exception {
        assertNotNull(g.toString());
        assertTrue(g.toString().startsWith(g.getClass().getSimpleName().toLowerCase()));
    }

    /**
     * Test graph counts with addition and removal of vertices.
     */
    @Test
    public void shouldProperlyCountVerticesAndEdgesOnAddRemove() {
        final Vertex v = g.addVertex();
        StructureStandardSuite.assertVertexEdgeCounts(1, 0).accept(g);
        assertEquals(v, g.V().next());
        assertEquals(v.getId(), g.V().next().getId());
        assertEquals(v.getLabel(), g.V().next().getLabel());
        v.remove();
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(0, 0));
        g.addVertex();
        g.addVertex();
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(2, 0));
        g.V().forEach(Vertex::remove);
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(0, 0));

        final String edgeLabel = GraphManager.get().convertLabel("test");
        Vertex v1 = g.addVertex();
        Vertex v2 = g.addVertex();
        Edge e = v1.addEdge(edgeLabel, v2);
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(2, 1));

        // test removal of the edge itself
        e.remove();
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(2, 0));

        v1.addEdge(edgeLabel, v2);
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(2, 1));

        // test removal of the out vertex to remove the edge
        v1.remove();
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(1, 0));

        // test removal of the in vertex to remove the edge
        v1 = g.addVertex();
        v1.addEdge(edgeLabel, v2);
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(2, 1));
        v2.remove();
        tryCommit(g, StructureStandardSuite.assertVertexEdgeCounts(1, 0));
    }

    /**
     * Generate a graph with lots of edges and vertices, then test vertex/edge counts on removal of each edge.
     */
    @Test
    public void shouldRemoveEdges() {
        final int vertexCount = 100;
        final int edgeCount = 200;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();
        final Random random = new Random();

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(g.addVertex(Element.ID, i)));
        tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, 0));

        IntStream.range(0, edgeCount).forEach(i -> {
            boolean created = false;
            while (!created) {
                final Vertex a = vertices.get(random.nextInt(vertices.size()));
                final Vertex b = vertices.get(random.nextInt(vertices.size()));
                if (a != b) {
                    edges.add(a.addEdge(GraphManager.get().convertLabel("a" + UUID.randomUUID()), b));
                    created = true;
                }
            }
        });

        tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, edgeCount));

        int counter = 0;
        for (Edge e : edges) {
            counter = counter + 1;
            e.remove();

            final int currentCounter = counter;
            tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, edgeCount - currentCounter));
        }
    }

    /**
     * Generate a graph with lots of edges and vertices, then test vertex/edge counts on removal of each vertex.
     */
    @Test
    public void shouldRemoveVertices() {
        final int vertexCount = 500;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(g.addVertex(Element.ID, i)));
        tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, 0));

        for (int i = 0; i < vertexCount; i = i + 2) {
            final Vertex a = vertices.get(i);
            final Vertex b = vertices.get(i + 1);
            edges.add(a.addEdge(GraphManager.get().convertLabel("a" + UUID.randomUUID()), b));
        }

        tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, vertexCount / 2));

        int counter = 0;
        for (Vertex v : vertices) {
            counter = counter + 1;
            v.remove();

            if ((counter + 1) % 2 == 0) {
                final int currentCounter = counter;
                tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(
                        vertexCount - currentCounter, edges.size() - ((currentCounter + 1) / 2)));
            }
        }
    }

    /**
     * Create a small {@link com.tinkerpop.gremlin.structure.Graph} and ensure that counts of edges per vertex are correct.
     */
    @Test
    public void shouldEvaluateConnectivityPatterns() {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = this.g;

        final Vertex a = graph.addVertex(Element.ID, graphProvider.convertId("1"));
        final Vertex b = graph.addVertex(Element.ID, graphProvider.convertId("2"));
        final Vertex c = graph.addVertex(Element.ID, graphProvider.convertId("3"));
        final Vertex d = graph.addVertex(Element.ID, graphProvider.convertId("4"));

        tryCommit(graph, AbstractGremlinSuite.assertVertexEdgeCounts(4, 0));

        final Edge e = a.addEdge(graphProvider.convertLabel("knows"), b);
        final Edge f = b.addEdge(graphProvider.convertLabel("knows"), c);
        final Edge g = c.addEdge(graphProvider.convertLabel("knows"), d);
        final Edge h = d.addEdge(graphProvider.convertLabel("knows"), a);

        tryCommit(graph, AbstractGremlinSuite.assertVertexEdgeCounts(4, 4));

        for (Vertex v : graph.V().toList()) {
            assertEquals(1, v.outE().count());
            assertEquals(1, v.inE().count());
        }

        for (Edge x : graph.E().toList()) {
            assertEquals(graphProvider.convertLabel("knows"), x.getLabel());
        }

        if (graph.getFeatures().vertex().supportsUserSuppliedIds()) {
            final Vertex va = graph.v(graphProvider.convertId("1"));
            final Vertex vb = graph.v(graphProvider.convertId("2"));
            final Vertex vc = graph.v(graphProvider.convertId("3"));
            final Vertex vd = graph.v(graphProvider.convertId("4"));

            assertEquals(a, va);
            assertEquals(b, vb);
            assertEquals(c, vc);
            assertEquals(d, vd);

            assertEquals(1, va.inE().count());
            assertEquals(1, va.outE().count());
            assertEquals(1, vb.inE().count());
            assertEquals(1, vb.outE().count());
            assertEquals(1, vc.inE().count());
            assertEquals(1, vc.outE().count());
            assertEquals(1, vd.inE().count());
            assertEquals(1, vd.outE().count());

            final Edge i = a.addEdge(graphProvider.convertLabel("hates"), b);

            assertEquals(1, va.inE().count());
            assertEquals(2, va.outE().count());
            assertEquals(2, vb.inE().count());
            assertEquals(1, vb.outE().count());
            assertEquals(1, vc.inE().count());
            assertEquals(1, vc.outE().count());
            assertEquals(1, vd.inE().count());
            assertEquals(1, vd.outE().count());

            for (Edge x : a.outE().toList()) {
                assertTrue(x.getLabel().equals(graphProvider.convertId("knows")) || x.getLabel().equals(graphProvider.convertId("hates")));
            }

            assertEquals(graphProvider.convertId("hates"), i.getLabel());
            assertEquals(graphProvider.convertId("2"), i.getVertex(Direction.IN).getId().toString());
            assertEquals(graphProvider.convertId("1"), i.getVertex(Direction.OUT).getId().toString());
        }

        final Set<Object> vertexIds = new HashSet<>();
        vertexIds.add(a.getId());
        vertexIds.add(a.getId());
        vertexIds.add(b.getId());
        vertexIds.add(b.getId());
        vertexIds.add(c.getId());
        vertexIds.add(d.getId());
        vertexIds.add(d.getId());
        vertexIds.add(d.getId());
        assertEquals(4, vertexIds.size());
    }

    @Test
    public void shouldTraverseInOutFromVertexWithSingleEdgeLabelFilter() {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = g;

        final Vertex a = graph.addVertex();
        final Vertex b = graph.addVertex();
        final Vertex c = graph.addVertex();

        final String labelFriend = graphProvider.convertLabel("friend");
        final String labelHate = graphProvider.convertLabel("hate");

        final Edge aFriendB = a.addEdge(labelFriend, b);
        final Edge aFriendC = a.addEdge(labelFriend, c);
        final Edge aHateC = a.addEdge(labelHate, c);
        final Edge cHateA = c.addEdge(labelHate, a);
        final Edge cHateB = c.addEdge(labelHate, b);

        List<Edge> results = a.outE().toList();
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = a.outE(labelFriend).toList();
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));

        results = a.outE(labelHate).toList();
        assertEquals(1, results.size());
        assertTrue(results.contains(aHateC));

        results = a.inE(labelHate).toList();
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = a.inE(labelFriend).toList();
        assertEquals(0, results.size());

        results = b.inE(labelHate).toList();
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateB));

        results = b.inE(labelFriend).toList();
        assertEquals(1, results.size());
        assertTrue(results.contains(aFriendB));
    }

    @Test
    public void shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter() {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = g;
        final Vertex a = graph.addVertex();
        final Vertex b = graph.addVertex();
        final Vertex c = graph.addVertex();

        final String labelFriend = graphProvider.convertLabel("friend");
        final String labelHate = graphProvider.convertLabel("hate");

        final Edge aFriendB = a.addEdge(labelFriend, b);
        final Edge aFriendC = a.addEdge(labelFriend, c);
        final Edge aHateC = a.addEdge(labelHate, c);
        final Edge cHateA = c.addEdge(labelHate, a);
        final Edge cHateB = c.addEdge(labelHate, b);

        List<Edge> results = a.outE(labelFriend, labelHate).toList();
        assertEquals(results.size(), 3);
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = a.inE(labelFriend, labelHate).toList();
        assertEquals(results.size(), 1);
        assertTrue(results.contains(cHateA));

        results = b.inE(labelFriend, labelHate).toList();
        assertEquals(results.size(), 2);
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(cHateB));

        results = b.inE(graphProvider.convertLabel("blah1"), graphProvider.convertLabel("blah2")).toList();
        assertEquals(results.size(), 0);
    }

    @Test
    public void shouldTestTreeConnectivity() {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = g;

        int branchSize = 11;
        final Vertex start = graph.addVertex();
        for (int i = 0; i < branchSize; i++) {
            final Vertex a = graph.addVertex();
            start.addEdge(graphProvider.convertLabel("test1"), a);
            for (int j = 0; j < branchSize; j++) {
                final Vertex b = graph.addVertex();
                a.addEdge(graphProvider.convertLabel("test2"), b);
                for (int k = 0; k < branchSize; k++) {
                    final Vertex c = graph.addVertex();
                    b.addEdge(graphProvider.convertLabel("test3"), c);
                }
            }
        }

        assertEquals(0, start.inE().count());
        assertEquals(branchSize, start.outE().count());
        for (Edge e : start.outE().toList()) {
            assertEquals(graphProvider.convertId("test1"), e.getLabel());
            assertEquals(branchSize, e.getVertex(Direction.IN).out().count());
            assertEquals(1, e.getVertex(Direction.IN).inE().count());
            for (Edge f : e.getVertex(Direction.IN).outE().toList()) {
                assertEquals(graphProvider.convertId("test2"), f.getLabel());
                assertEquals(branchSize, f.getVertex(Direction.IN).out().count());
                assertEquals(1, f.getVertex(Direction.IN).in().count());
                for (Edge g : f.getVertex(Direction.IN).outE().toList()) {
                    assertEquals(graphProvider.convertId("test3"), g.getLabel());
                    assertEquals(0, g.getVertex(Direction.IN).out().count());
                    assertEquals(1, g.getVertex(Direction.IN).in().count());
                }
            }
        }

        int totalVertices = 0;
        for (int i = 0; i < 4; i++) {
            totalVertices = totalVertices + (int) Math.pow(branchSize, i);
        }

        tryCommit(graph, AbstractGremlinSuite.assertVertexEdgeCounts(totalVertices, totalVertices - 1));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_PERSISTENCE)
    public void shouldPersistDataOnClose() throws Exception {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = g;

        final Vertex v = graph.addVertex();
        final Vertex u = graph.addVertex();
        if (graph.getFeatures().edge().properties().supportsStringValues()) {
            v.setProperty("name", "marko");
            u.setProperty("name", "pavel");
        }

        final Edge e = v.addEdge(graphProvider.convertLabel("collaborator"), u);
        if (graph.getFeatures().edge().properties().supportsStringValues())
            e.setProperty("location", "internet");

        tryCommit(graph, AbstractGremlinSuite.assertVertexEdgeCounts(2, 1));
        graph.close();

        final Graph reopenedGraph = graphProvider.standardTestGraph();
        AbstractGremlinSuite.assertVertexEdgeCounts(2, 1).accept(reopenedGraph);

        if (graph.getFeatures().vertex().properties().supportsStringValues()) {
            for (Vertex vertex : reopenedGraph.V().toList()) {
                assertTrue(vertex.getProperty("name").get().equals("marko") || vertex.getProperty("name").get().equals("pavel"));
            }
        }

        for (Edge edge : reopenedGraph.E().toList()) {
            assertEquals(graphProvider.convertId("collaborator"), edge.getLabel());
            if (graph.getFeatures().edge().properties().supportsStringValues())
                assertEquals("internet", edge.getProperty("location").get());
        }

        graphProvider.clear(reopenedGraph, graphProvider.standardGraphConfiguration());
    }
}
