package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTest extends AbstractBlueprintsTest {

    /**
     * All Blueprints implementations should be constructable through GraphFactory.
     */
    @Test
    public void shouldOpenInMemoryGraphViaApacheConfig() {
        final Graph expectedGraph = g;
        final Graph createdGraph = GraphFactory.open(config);

        assertNotNull(createdGraph);
        assertEquals(expectedGraph.getClass(), createdGraph.getClass());
    }

    /**
     * Blueprints implementations should have private constructor as all graphs.  They should be only instantiated
     * through the GraphFactory or the static open() method on the Graph implementation itself.
     */
    @Test
    public void shouldHavePrivateConstructor() {
        assertTrue(Arrays.asList(g.getClass().getConstructors()).stream().allMatch(c -> {
            final int modifier = c.getModifiers();
            return Modifier.isPrivate(modifier) || Modifier.isPrivate(modifier);
        }));
    }

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
     * Graphs should be empty on creation.
     */
    @Test
    public void shouldConstructAnEmptyGraph() {
        BlueprintsStandardSuite.assertVertexEdgeCounts(g, 0, 0);
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
    public void testRemovingVertices() {
        final int vertexCount = 500;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();

        IntStream.range(0, vertexCount).forEach(i->vertices.add(g.addVertex(Property.Key.ID, i)));
        tryCommit(g, graph-> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, 0));

        for (int i = 0; i < vertexCount; i = i + 2) {
            final Vertex a = vertices.get(i);
            final Vertex b = vertices.get(i + 1);
            edges.add(a.addEdge(AbstractBlueprintsSuite.GraphManager.get().convertLabel("a" + UUID.randomUUID()), b));
        }

        tryCommit(g, graph-> AbstractBlueprintsSuite.assertVertexEdgeCounts(g, vertexCount, vertexCount / 2));

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
}
