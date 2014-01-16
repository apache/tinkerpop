package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.StreamFactory;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTest extends AbstractBlueprintsTest {

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
        BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0);
        assertEquals(v, g.query().vertices().iterator().next());
        assertEquals(v.getId(), g.query().vertices().iterator().next().getId());
        assertEquals(v.getLabel(), g.query().vertices().iterator().next().getLabel());
        v.remove();
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(graph, 0, 0));
        g.addVertex();
        g.addVertex();
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 2, 0));
        g.query().vertices().forEach(Vertex::remove);
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 0, 0));

        final String edgeLabel = BlueprintsStandardSuite.GraphManager.get().convertLabel("test");
        Vertex v1 = g.addVertex();
        Vertex v2 = g.addVertex();
        Edge e = v1.addEdge(edgeLabel, v2);
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 2, 1));

        // test removal of the edge itself
        e.remove();
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 2, 0));

        v1.addEdge(edgeLabel, v2);
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 2, 1));

        // test removal of the out vertex to remove the edge
        v1.remove();
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0));

        // test removal of the in vertex to remove the edge
        v1 = g.addVertex();
        v1.addEdge(edgeLabel, v2);
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 2, 1));
        v2.remove();
        tryCommit(g, graph-> BlueprintsStandardSuite.assertVertexEdgeCounts(g, 1, 0));
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

        IntStream.range(0, vertexCount).forEach(i->vertices.add(g.addVertex(Property.Key.ID, i)));
        tryCommit(g, graph-> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, 0));

        IntStream.range(0, edgeCount).forEach(i -> {
            boolean created = false;
            while (!created) {
                final Vertex a = vertices.get(random.nextInt(vertices.size()));
                final Vertex b = vertices.get(random.nextInt(vertices.size()));
                if (a != b) {
                    edges.add(a.addEdge(AbstractBlueprintsSuite.GraphManager.get().convertLabel("a" + UUID.randomUUID()), b));
                    created = true;
                }
            }
        });

        tryCommit(g, graph-> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, edgeCount));

        int counter = 0;
        for (Edge e : edges) {
            counter = counter + 1;
            e.remove();

            final int currentCounter = counter;
            tryCommit(g, graph -> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, edgeCount - currentCounter));
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

        IntStream.range(0, vertexCount).forEach(i->vertices.add(g.addVertex(Property.Key.ID, i)));
        tryCommit(g, graph -> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, 0));

        for (int i = 0; i < vertexCount; i = i + 2) {
            final Vertex a = vertices.get(i);
            final Vertex b = vertices.get(i + 1);
            edges.add(a.addEdge(AbstractBlueprintsSuite.GraphManager.get().convertLabel("a" + UUID.randomUUID()), b));
        }

        tryCommit(g, graph -> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, vertexCount / 2));

        int counter = 0;
        for (Vertex v : vertices) {
            counter = counter + 1;
            v.remove();

            if ((counter + 1) % 2 == 0) {
                final int currentCounter = counter;
                tryCommit(g, graph-> AbstractBlueprintsSuite.assertVertexEdgeCounts(
                        g, vertexCount - currentCounter, edges.size() - ((currentCounter + 1) / 2)));
            }
        }
    }

    /**
     * Create a small {@link Graph} and ensure that counts of edges per vertex are correct.
     */
    @Test
    public void shouldEvaluateConnectivityPatterns() {
        final AbstractBlueprintsSuite.GraphProvider graphProvider = AbstractBlueprintsSuite.GraphManager.get();
        final Graph graph = this.g;

        final Vertex a = graph.addVertex(Property.Key.ID, graphProvider.convertId("1"));
        final Vertex b = graph.addVertex(Property.Key.ID, graphProvider.convertId("2"));
        final Vertex c = graph.addVertex(Property.Key.ID, graphProvider.convertId("3"));
        final Vertex d = graph.addVertex(Property.Key.ID, graphProvider.convertId("4"));

        tryCommit(graph, ig-> AbstractBlueprintsSuite.assertVertexEdgeCounts(ig, 4, 0));

        final Edge e = a.addEdge(graphProvider.convertLabel("knows"), b);
        final Edge f = b.addEdge(graphProvider.convertLabel("knows"), c);
        final Edge g = c.addEdge(graphProvider.convertLabel("knows"), d);
        final Edge h = d.addEdge(graphProvider.convertLabel("knows"), a);

        tryCommit(graph, ig-> AbstractBlueprintsSuite.assertVertexEdgeCounts(ig, 4, 4));

        for (Vertex v : graph.query().vertices()) {
            assertEquals(1, v.query().direction(Direction.OUT).count());
            assertEquals(1, v.query().direction(Direction.IN).count());
        }

        for (Edge x : graph.query().edges()) {
            assertEquals(graphProvider.convertLabel("knows"), x.getLabel());
        }

        if (graph.getFeatures().vertex().supportsUserSuppliedIds()) {
            final Vertex va = graph.query().ids(graphProvider.convertId("1")).vertices().iterator().next();
            final Vertex vb = graph.query().ids(graphProvider.convertId("2")).vertices().iterator().next();
            final Vertex vc = graph.query().ids(graphProvider.convertId("3")).vertices().iterator().next();
            final Vertex vd = graph.query().ids(graphProvider.convertId("4")).vertices().iterator().next();

            assertEquals(a, va);
            assertEquals(b, vb);
            assertEquals(c, vc);
            assertEquals(d, vd);

            assertEquals(1, va.query().direction(Direction.IN).count());
            assertEquals(1, va.query().direction(Direction.OUT).count());
            assertEquals(1, vb.query().direction(Direction.IN).count());
            assertEquals(1, vb.query().direction(Direction.OUT).count());
            assertEquals(1, vc.query().direction(Direction.IN).count());
            assertEquals(1, vc.query().direction(Direction.OUT).count());
            assertEquals(1, vd.query().direction(Direction.IN).count());
            assertEquals(1, vd.query().direction(Direction.OUT).count());

            final Edge i = a.addEdge(graphProvider.convertLabel("hates"), b);

            assertEquals(1, va.query().direction(Direction.IN).count());
            assertEquals(2, va.query().direction(Direction.OUT).count());
            assertEquals(2, vb.query().direction(Direction.IN).count());
            assertEquals(1, vb.query().direction(Direction.OUT).count());
            assertEquals(1, vc.query().direction(Direction.IN).count());
            assertEquals(1, vc.query().direction(Direction.OUT).count());
            assertEquals(1, vd.query().direction(Direction.IN).count());
            assertEquals(1, vd.query().direction(Direction.OUT).count());

            for (Edge x : a.query().direction(Direction.OUT).edges()) {
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
        final AbstractBlueprintsSuite.GraphProvider graphProvider = AbstractBlueprintsSuite.GraphManager.get();
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

        List<Edge> results = StreamFactory.stream(a.query().direction(Direction.OUT).edges()).collect(Collectors.toList());
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = StreamFactory.stream(a.query().direction(Direction.OUT).labels(labelFriend).edges()).collect(Collectors.toList());
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));

        results = StreamFactory.stream(a.query().direction(Direction.OUT).labels(labelHate).edges()).collect(Collectors.toList());
        assertEquals(1, results.size());
        assertTrue(results.contains(aHateC));

        results = StreamFactory.stream(a.query().direction(Direction.IN).labels(labelHate).edges()).collect(Collectors.toList());
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = StreamFactory.stream(a.query().direction(Direction.IN).labels(labelFriend).edges()).collect(Collectors.toList());
        assertEquals(0, results.size());

        results = StreamFactory.stream(b.query().direction(Direction.IN).labels(labelHate).edges()).collect(Collectors.toList());
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateB));

        results = StreamFactory.stream(b.query().direction(Direction.IN).labels(labelFriend).edges()).collect(Collectors.toList());
        assertEquals(1, results.size());
        assertTrue(results.contains(aFriendB));
    }

    @Test
    public void shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter() {
        final AbstractBlueprintsSuite.GraphProvider graphProvider = AbstractBlueprintsSuite.GraphManager.get();
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

        List<Edge> results = StreamFactory.stream(a.query().direction(Direction.OUT).labels(labelFriend, labelHate).edges()).collect(Collectors.toList());
        assertEquals(results.size(), 3);
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = StreamFactory.stream(a.query().direction(Direction.IN).labels(labelFriend, labelHate).edges()).collect(Collectors.toList());
        assertEquals(results.size(), 1);
        assertTrue(results.contains(cHateA));

        results = StreamFactory.stream(b.query().direction(Direction.IN).labels(labelFriend, labelHate).edges()).collect(Collectors.toList());
        assertEquals(results.size(), 2);
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(cHateB));

        results = StreamFactory.stream(b.query().direction(Direction.IN).labels(graphProvider.convertLabel("blah1"), graphProvider.convertLabel("blah2")).edges()).collect(Collectors.toList());
        assertEquals(results.size(), 0);
    }

    @Test
    public void shouldTestTreeConnectivity() {
        final AbstractBlueprintsSuite.GraphProvider graphProvider = AbstractBlueprintsSuite.GraphManager.get();
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

        assertEquals(0, start.query().direction(Direction.IN).count());
        assertEquals(branchSize, start.query().direction(Direction.OUT).count());
        for (Edge e : start.query().direction(Direction.OUT).edges()) {
            assertEquals(graphProvider.convertId("test1"), e.getLabel());
            assertEquals(branchSize, e.getVertex(Direction.IN).query().direction(Direction.OUT).count());
            assertEquals(1, e.getVertex(Direction.IN).query().direction(Direction.IN).count());
            for (Edge f : e.getVertex(Direction.IN).query().direction(Direction.OUT).edges()) {
                assertEquals(graphProvider.convertId("test2"), f.getLabel());
                assertEquals(branchSize, f.getVertex(Direction.IN).query().direction(Direction.OUT).count());
                assertEquals(1, f.getVertex(Direction.IN).query().direction(Direction.IN).count());
                for (Edge g : f.getVertex(Direction.IN).query().direction(Direction.OUT).edges()) {
                    assertEquals(graphProvider.convertId("test3"), g.getLabel());
                    assertEquals(0, g.getVertex(Direction.IN).query().direction(Direction.OUT).count());
                    assertEquals(1, g.getVertex(Direction.IN).query().direction(Direction.IN).count());
                }
            }
        }

        int totalVertices = 0;
        for (int i = 0; i < 4; i++) {
            totalVertices = totalVertices + (int) Math.pow(branchSize, i);
        }

        final List<Vertex> vertices = StreamFactory.stream(graph.query().vertices()).collect(Collectors.toList());
        assertEquals(totalVertices, vertices.size());

        final List<Edge> edges = StreamFactory.stream(graph.query().edges()).collect(Collectors.toList());
        assertEquals(totalVertices - 1, edges.size());
    }
}
