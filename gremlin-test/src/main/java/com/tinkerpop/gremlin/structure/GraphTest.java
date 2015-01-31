package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.GraphProvider;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_PERSISTENCE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
        "vertexWithIdAlreadyExists",
        "elementNotFound"
})
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class GraphTest extends AbstractGremlinTest {

    /**
     * Ensure compliance with Features by checking that all Features are exposed by the implementation.
     */
    @Test
    public void shouldImplementAndExposeFeatures() {
        final Graph.Features features = g.features();
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

    @Test
    public void shouldHaveExceptionConsistencyWhenFindVertexByIdThatIsNonExistentViaTraversal() {
        try {
            g.V(10000l).next();
            fail("Call to g.V(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.elementNotFound(Vertex.class, 10000l).getClass()));
        }

    }

    @Test
    public void shouldHaveExceptionConsistencyWhenFindVertexByIdThatIsNonExistentViaIterator() {
        try {
            g.iterators().vertexIterator(10000l).next();
            fail("Call to g.V(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.elementNotFound(Vertex.class, 10000l).getClass()));
        }

    }

    @Test
    public void shouldHaveExceptionConsistencyWhenFindEdgeByIdThatIsNonExistentViaTraversal() {
        try {
            g.E(10000l).next();
            fail("Call to g.E(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.elementNotFound(Edge.class, 10000l).getClass()));
        }

    }

    @Test
    public void shouldHaveExceptionConsistencyWhenFindEdgeByIdThatIsNonExistentViaIterator() {
        try {
            g.iterators().edgeIterator(10000l).next();
            fail("Call to g.E(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.elementNotFound(Edge.class, 10000l).getClass()));
        }

    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldHaveExceptionConsistencyWhenAssigningSameIdOnVertex() {
        final Object o = GraphManager.get().convertId("1");
        g.addVertex(T.id, o);
        try {
            g.addVertex(T.id, o);
            fail("Assigning the same ID to an Element should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.vertexWithIdAlreadyExists(0).getClass()));
        }

    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldAddVertexWithUserSuppliedNumericId() {
        g.addVertex(T.id, 1000l);
        tryCommit(g, graph -> {
            final Vertex v = g.iterators().vertexIterator(1000l).next();
            assertEquals(1000l, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldAddVertexWithUserSuppliedStringId() {
        g.addVertex(T.id, "1000");
        tryCommit(g, graph -> {
            final Vertex v = g.iterators().vertexIterator("1000").next();
            assertEquals("1000", v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldAddVertexWithUserSuppliedUuidId() {
        final UUID uuid = UUID.randomUUID();
        g.addVertex(T.id, uuid);
        tryCommit(g, graph -> {
            final Vertex v = g.iterators().vertexIterator(uuid).next();
            assertEquals(uuid, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ANY_IDS)
    public void shouldAddVertexWithUserSuppliedAnyId() {
        final UUID uuid = UUID.randomUUID();
        g.addVertex(T.id, uuid);
        tryCommit(g, graph -> {
            final Vertex v = g.iterators().vertexIterator(uuid).next();
            assertEquals(uuid, v.id());
        });

        g.addVertex(T.id, uuid.toString());
        tryCommit(g, graph -> {
            final Vertex v = g.iterators().vertexIterator(uuid.toString()).next();
            assertEquals(uuid.toString(), v.id());
        });

        // this is different from "FEATURE_CUSTOM_IDS" as TinkerGraph does not define a specific id class
        // (i.e. TinkerId) for the identifier.
        IoTest.CustomId customId = new IoTest.CustomId("test", uuid);
        g.addVertex(T.id, customId);
        tryCommit(g, graph -> {
            final Vertex v = g.iterators().vertexIterator(customId).next();
            assertEquals(customId, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES, supported = false)
    public void shouldOverwriteEarlierKeyValuesWithLaterKeyValuesOnAddVertexIfNoMultiProperty() {
        final Vertex v = g.addVertex("test", "A", "test", "B", "test", "C");
        tryCommit(g, graph -> {
            assertEquals(1, IteratorUtils.count(v.iterators().propertyIterator("test")));
            assertTrue(StreamFactory.stream(v.iterators().valueIterator("test")).anyMatch(t -> t.equals("C")));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void shouldOverwriteEarlierKeyValuesWithLaterKeyValuesOnAddVertexIfMultiProperty() {
        final Vertex v = g.addVertex("test", "A", "test", "B", "test", "C");
        tryCommit(g, graph -> {
            assertEquals(3, IteratorUtils.count(v.iterators().propertyIterator("test")));
            assertTrue(StreamFactory.stream(v.iterators().valueIterator("test")).anyMatch(t -> t.equals("A")));
            assertTrue(StreamFactory.stream(v.iterators().valueIterator("test")).anyMatch(t -> t.equals("B")));
            assertTrue(StreamFactory.stream(v.iterators().valueIterator("test")).anyMatch(t -> t.equals("C")));
        });
    }

    /**
     * Graphs should have a standard toString representation where the value is generated by
     * {@link com.tinkerpop.gremlin.structure.util.StringFactory#graphString(Graph, String)}.
     */
    @Test
    public void shouldHaveStandardStringRepresentation() throws Exception {
        assertTrue(g.toString().matches(".*\\[.*\\]"));
    }

    /**
     * Generate a graph with lots of edges and vertices, then test vertex/edge counts on removal of each edge.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
    public void shouldRemoveEdges() {
        final int vertexCount = 100;
        final int edgeCount = 200;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();
        final Random random = new Random();

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(g.addVertex()));
        tryCommit(g, assertVertexEdgeCounts(vertexCount, 0));

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

        tryCommit(g, assertVertexEdgeCounts(vertexCount, edgeCount));

        int counter = 0;
        for (Edge e : edges) {
            counter = counter + 1;
            e.remove();

            final int currentCounter = counter;
            tryCommit(g, assertVertexEdgeCounts(vertexCount, edgeCount - currentCounter));
        }
    }

    /**
     * Generate a graph with lots of edges and vertices, then test vertex/edge counts on removal of each vertex.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    public void shouldRemoveVertices() {
        final int vertexCount = 500;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(g.addVertex()));
        tryCommit(g, assertVertexEdgeCounts(vertexCount, 0));

        for (int i = 0; i < vertexCount; i = i + 2) {
            final Vertex a = vertices.get(i);
            final Vertex b = vertices.get(i + 1);
            edges.add(a.addEdge(GraphManager.get().convertLabel("a" + UUID.randomUUID()), b));
        }

        tryCommit(g, assertVertexEdgeCounts(vertexCount, vertexCount / 2));

        int counter = 0;
        for (Vertex v : vertices) {
            counter = counter + 1;
            v.remove();

            if ((counter + 1) % 2 == 0) {
                final int currentCounter = counter;
                tryCommit(g, assertVertexEdgeCounts(
                        vertexCount - currentCounter, edges.size() - ((currentCounter + 1) / 2)));
            }
        }
    }

    /**
     * Generate a graph with lots of vertices, then iterate the vertices and remove them from the graph
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    public void shouldRemoveVerticesWithoutConcurrentModificationException() {
        for (int i = 0; i < 100; i++) {
            g.addVertex();
        }
        final Iterator<Vertex> vertexIterator = g.iterators().vertexIterator();
        assertTrue(vertexIterator.hasNext());
        while (vertexIterator.hasNext()) {
            vertexIterator.next().remove();
        }
        assertFalse(vertexIterator.hasNext());
        tryCommit(g, g -> assertFalse(g.iterators().vertexIterator().hasNext()));
    }

    /**
     * Generate a graph with lots of edges, then iterate the edges and remove them from the graph
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
    public void shouldRemoveEdgesWithoutConcurrentModificationException() {
        for (int i = 0; i < 50; i++) {
            g.addVertex().addEdge("link", g.addVertex());
        }

        final Iterator<Edge> edgeIterator = g.iterators().edgeIterator();
        assertTrue(edgeIterator.hasNext());
        while (edgeIterator.hasNext()) {
            edgeIterator.next().remove();
        }
        assertFalse(edgeIterator.hasNext());

        // TODO: Neo4j fails on this: tryCommit(g, g -> assertFalse(g.iterators().edgeIterator().hasNext()));
        tryCommit(g);
        assertFalse(g.iterators().edgeIterator().hasNext());
    }

    /**
     * Create a small {@link com.tinkerpop.gremlin.structure.Graph} and ensure that counts of edges per vertex are correct.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldEvaluateConnectivityPatterns() {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = this.g;

        final Vertex a;
        final Vertex b;
        final Vertex c;
        final Vertex d;
        if (graph.features().vertex().supportsUserSuppliedIds()) {
            a = graph.addVertex(T.id, graphProvider.convertId("1"));
            b = graph.addVertex(T.id, graphProvider.convertId("2"));
            c = graph.addVertex(T.id, graphProvider.convertId("3"));
            d = graph.addVertex(T.id, graphProvider.convertId("4"));
        } else {
            a = graph.addVertex();
            b = graph.addVertex();
            c = graph.addVertex();
            d = graph.addVertex();
        }

        tryCommit(graph, assertVertexEdgeCounts(4, 0));

        final Edge e = a.addEdge(graphProvider.convertLabel("knows"), b);
        final Edge f = b.addEdge(graphProvider.convertLabel("knows"), c);
        final Edge g = c.addEdge(graphProvider.convertLabel("knows"), d);
        final Edge h = d.addEdge(graphProvider.convertLabel("knows"), a);

        tryCommit(graph, assertVertexEdgeCounts(4, 4));

        graph.iterators().vertexIterator().forEachRemaining(v -> {
            assertEquals(new Long(1), v.outE().count().next());
            assertEquals(new Long(1), v.inE().count().next());
        });

        graph.iterators().edgeIterator().forEachRemaining(x -> {
            assertEquals(graphProvider.convertLabel("knows"), x.label());
        });

        if (graph.features().vertex().supportsUserSuppliedIds()) {
            final Vertex va = graph.iterators().vertexIterator(graphProvider.convertId("1")).next();
            final Vertex vb = graph.iterators().vertexIterator(graphProvider.convertId("2")).next();
            final Vertex vc = graph.iterators().vertexIterator(graphProvider.convertId("3")).next();
            final Vertex vd = graph.iterators().vertexIterator(graphProvider.convertId("4")).next();

            assertEquals(a, va);
            assertEquals(b, vb);
            assertEquals(c, vc);
            assertEquals(d, vd);

            assertEquals(new Long(1), va.inE().count().next());
            assertEquals(new Long(1), va.outE().count().next());
            assertEquals(new Long(1), vb.inE().count().next());
            assertEquals(new Long(1), vb.outE().count().next());
            assertEquals(new Long(1), vc.inE().count().next());
            assertEquals(new Long(1), vc.outE().count().next());
            assertEquals(new Long(1), vd.inE().count().next());
            assertEquals(new Long(1), vd.outE().count().next());

            final Edge i = a.addEdge(graphProvider.convertLabel("hates"), b);

            assertEquals(new Long(1), va.inE().count().next());
            assertEquals(new Long(2), va.outE().count().next());
            assertEquals(new Long(2), vb.inE().count().next());
            assertEquals(new Long(1), vb.outE().count().next());
            assertEquals(new Long(1), vc.inE().count().next());
            assertEquals(new Long(1), vc.outE().count().next());
            assertEquals(new Long(1), vd.inE().count().next());
            assertEquals(new Long(1), vd.outE().count().next());

            for (Edge x : a.outE().toList()) {
                assertTrue(x.label().equals(graphProvider.convertId("knows")) || x.label().equals(graphProvider.convertId("hates")));
            }

            assertEquals(graphProvider.convertId("hates"), i.label());
            assertEquals(graphProvider.convertId("2"), i.inV().id().next().toString());
            assertEquals(graphProvider.convertId("1"), i.outV().id().next().toString());
        }

        final Set<Object> vertexIds = new HashSet<>();
        vertexIds.add(a.id());
        vertexIds.add(a.id());
        vertexIds.add(b.id());
        vertexIds.add(b.id());
        vertexIds.add(c.id());
        vertexIds.add(d.id());
        vertexIds.add(d.id());
        vertexIds.add(d.id());
        assertEquals(4, vertexIds.size());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
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
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
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
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = a.inE(labelFriend, labelHate).toList();
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = b.inE(labelFriend, labelHate).toList();
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(cHateB));

        results = b.inE(graphProvider.convertLabel("blah1"), graphProvider.convertLabel("blah2")).toList();
        assertEquals(0, results.size());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
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

        assertEquals(new Long(0), start.inE().count().next());
        assertEquals(new Long(branchSize), start.outE().count().next());
        for (Edge e : start.outE().toList()) {
            assertEquals(graphProvider.convertId("test1"), e.label());
            assertEquals(new Long(branchSize), e.inV().out().count().next());
            assertEquals(new Long(1), e.inV().inE().count().next());
            for (Edge f : e.inV().outE().toList()) {
                assertEquals(graphProvider.convertId("test2"), f.label());
                assertEquals(new Long(branchSize), f.inV().out().count().next());
                assertEquals(new Long(1), f.inV().in().count().next());
                for (Edge g : f.inV().outE().toList()) {
                    assertEquals(graphProvider.convertId("test3"), g.label());
                    assertEquals(new Long(0), g.inV().out().count().next());
                    assertEquals(new Long(1), g.inV().in().count().next());
                }
            }
        }

        int totalVertices = 0;
        for (int i = 0; i < 4; i++) {
            totalVertices = totalVertices + (int) Math.pow(branchSize, i);
        }

        tryCommit(graph, assertVertexEdgeCounts(totalVertices, totalVertices - 1));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_PERSISTENCE)
    public void shouldPersistDataOnClose() throws Exception {
        final GraphProvider graphProvider = GraphManager.get();
        final Graph graph = g;

        final Vertex v = graph.addVertex();
        final Vertex u = graph.addVertex();
        if (graph.features().edge().properties().supportsStringValues()) {
            v.property("name", "marko");
            u.property("name", "pavel");
        }

        final Edge e = v.addEdge(graphProvider.convertLabel("collaborator"), u);
        if (graph.features().edge().properties().supportsStringValues())
            e.property("location", "internet");

        tryCommit(graph, assertVertexEdgeCounts(2, 1));
        graph.close();

        final Graph reopenedGraph = graphProvider.standardTestGraph(this.getClass(), name.getMethodName());
        assertVertexEdgeCounts(2, 1).accept(reopenedGraph);

        if (graph.features().vertex().properties().supportsStringValues()) {
            reopenedGraph.iterators().vertexIterator().forEachRemaining(vertex -> {
                assertTrue(vertex.property("name").value().equals("marko") || vertex.property("name").value().equals("pavel"));
            });
        }

        reopenedGraph.iterators().edgeIterator().forEachRemaining(edge -> {
            assertEquals(graphProvider.convertId("collaborator"), edge.label());
            if (graph.features().edge().properties().supportsStringValues())
                assertEquals("internet", edge.property("location").value());
        });

        graphProvider.clear(reopenedGraph, graphProvider.standardGraphConfiguration(this.getClass(), name.getMethodName()));
    }
}
