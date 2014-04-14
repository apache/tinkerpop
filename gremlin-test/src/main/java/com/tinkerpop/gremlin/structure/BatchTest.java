package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.structure.util.batch.Exists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.fail;

/**
 * BatchGraph tests are also handled by the IOTest battery.  These tests focus mostly on issues with the incremental
 * loading setting turned on since that is not supported by the IO library.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BatchTest extends AbstractGremlinTest {
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadIncrementallyWithSuppliedIdentifier() {
        final BatchGraph graph = new BatchGraph.Builder<>(g)
                .incrementalLoading(true)
                .bufferSize(1).build();
        graph.addVertex(Element.ID, 1, "name", "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, 2, "name", "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, 1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.getProperty("age").get());
        assertTrue(vStephen.outE("knows").has("weight",1.0f).inV().has("name", "marko").hasNext());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.getProperty("age").get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadIncrementallyWithNamedIdentifier() {
        final BatchGraph graph = new BatchGraph.Builder<>(g)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).build();
        graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.getProperty("age").get());
        assertTrue(vStephen.outE("knows").has("weight",1.0f).inV().has("name", "marko").hasNext());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.getProperty("age").get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadIncrementallyWithSuppliedIdentifierThrowOnExistingVertex() {
        final BatchGraph graph = new BatchGraph.Builder<>(g)
                .incrementalLoading(true, Exists.THROW, Exists.IGNORE)
                .bufferSize(1).build();
        graph.addVertex(Element.ID, 1, "name", "marko", "age", 29);
        try {
            graph.addVertex(Element.ID, 1, "name", "marko", "age", 34);
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadIncrementallyWithNamedIdentifierThrowOnExistingVertex() {
        final BatchGraph graph = new BatchGraph.Builder<>(g)
                .incrementalLoading(true, Exists.THROW, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).build();
        graph.addVertex(Element.ID, "marko", "age", 29);
        try {
            graph.addVertex(Element.ID, "marko", "age", 34);
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadIncrementallyWithSuppliedIdentifierOverwriteExistingVertex() {
        final BatchGraph graph = new BatchGraph.Builder<>(g)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .bufferSize(1).build();
        graph.addVertex(Element.ID, 1, "name", "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, 2, "name", "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, 1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.getProperty("age").get());
        assertTrue(vStephen.outE("knows").has("weight",1.0f).inV().has("name", "marko").hasNext());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(34, vMarko.getProperty("age").get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadIncrementallyWithNamedIdentifierOverwriteExistingVertex() {
        final BatchGraph graph = new BatchGraph.Builder<>(g)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).build();
        graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.getProperty("age").get());
        assertTrue(vStephen.outE("knows").has("weight",1.0f).inV().has("name", "marko").hasNext());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(34, vMarko.getProperty("age").get());
    }
}
