package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
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
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifier() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true)
                .bufferSize(1).create();
        graph.addVertex(Element.ID, 1, "name", "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, 2, "name", "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, 1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadVerticesIncrementallyWithNamedIdentifier() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).create();
        graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifierThrowOnExistingVertex() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.THROW, Exists.IGNORE)
                .bufferSize(1).create();
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
    public void shouldLoadVerticesIncrementallyWithNamedIdentifierThrowOnExistingVertex() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.THROW, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).create();
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
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifierOverwriteExistingVertex() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .bufferSize(1).create();
        graph.addVertex(Element.ID, 1, "name", "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, 2, "name", "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, 1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(34, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadVerticesIncrementallyWithNamedIdentifierOverwriteExistingVertex() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(34, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifier() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f, Element.ID, 1);
        v1.addEdge("knows", v2, "weight", 0.5f, Element.ID, 1); // second edge is ignored as it already exists
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());
        assertEquals(new Long(0), vStephen.outE("knows").has("weight", 0.5f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifier() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f, Element.ID, "abcde");
        v1.addEdge("knows", v2, "weight", 0.5f, Element.ID, "abcde"); // second edge is ignored as it already exists
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("uid", "abcde").has("weight", 1.0f).inV().has("name", "marko").count().next());
        assertEquals(new Long(0), vStephen.outE("knows").has("weight", 0.5f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifierOverwriteExistingEdge() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.IGNORE, Exists.OVERWRITE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f, Element.ID, 1);
        v1.addEdge("knows", v2, "weight", 0.5f, Element.ID, 1); // second edge is overwrites properties of the first
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(0), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 0.5f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierOverwriteExistingEdge() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.IGNORE, Exists.OVERWRITE)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f, Element.ID, "abcde");
        v1.addEdge("knows", v2, "weight", 0.5f, Element.ID, "abcde"); // second edge overwrites properties of the first
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(0), vStephen.outE("knows").has("uid", "abcde").has("weight", 1.0f).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 0.5f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifierThrowOnExistingEdge() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.IGNORE, Exists.THROW)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f, Element.ID, 1);
        try {
            v1.addEdge("knows", v2, "weight", 0.5f, Element.ID, 1); // second edge is overwrites properties of the first
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierThrowOnExistingEdge() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true, Exists.IGNORE, Exists.THROW)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f, Element.ID, "abcde");
        try {
            v1.addEdge("knows", v2, "weight", 0.5f, Element.ID, "abcde"); // second edge is overwrites properties of the first
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifierAndNoIdSpecified() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f);
        v1.addEdge("knows", v2, "weight", 0.5f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 0.5f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierAndNoIdSpecified() {
        final BatchGraph graph = BatchGraph.build(g)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = graph.addVertex(Element.ID, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(Element.ID, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0f);
        v1.addEdge("knows", v2, "weight", 0.5f);
        tryCommit(graph);

        final Vertex vStephen = g.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0f).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 0.5f).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().<Vertex>has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }
}
