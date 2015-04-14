/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import org.apache.tinkerpop.gremlin.structure.util.batch.Exists;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.List;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES;
import static org.junit.Assert.*;

/**
 * BatchGraph tests are also handled by the IOTest battery.  These tests focus mostly on issues with the incremental
 * loading setting turned on since that is not supported by the IO library.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BatchTest extends AbstractGremlinTest {
    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifier() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true)
                .bufferSize(1).create();
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Vertex.class);
        final Object id2 = GraphManager.getGraphProvider().convertId("2", Vertex.class);
        batchGraph.addVertex(T.id, id1, "name", "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, id2, "name", "stephen", "age", 37);
        final Vertex v2 = batchGraph.addVertex(T.id, id1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadVerticesIncrementallyWithNamedIdentifier() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).create();
        batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifierThrowOnExistingVertex() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.THROW, Exists.IGNORE)
                .bufferSize(1).create();
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Vertex.class);
        batchGraph.addVertex(T.id, id1, "name", "marko", "age", 29);
        try {
            batchGraph.addVertex(T.id, id1, "name", "marko", "age", 34);
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    public void shouldLoadVerticesIncrementallyWithNamedIdentifierThrowOnExistingVertex() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.THROW, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        batchGraph.addVertex(T.id, "marko", "age", 29);
        try {
            batchGraph.addVertex(T.id, "marko", "age", 34);
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifierOverwriteSingleExistingVertex() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.OVERWRITE_SINGLE, Exists.IGNORE)
                .bufferSize(1).create();
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Vertex.class);
        final Object id2 = GraphManager.getGraphProvider().convertId("2", Vertex.class);
        batchGraph.addVertex(T.id, id1, "name", "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, id2, "name", "stephen", "age", 37);
        final Vertex v2 = batchGraph.addVertex(T.id, id1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(34, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadVerticesIncrementallyWithNamedIdentifierOverwriteSingleExistingVertex() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.OVERWRITE_SINGLE, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(34, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadVerticesIncrementallyWithSuppliedIdentifierOverwriteExistingVertex() {
        final BatchGraph<?> batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .bufferSize(1).create();
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Vertex.class);
        final Object id2 = GraphManager.getGraphProvider().convertId("2", Vertex.class);
        batchGraph.addVertex(T.id, id1, "name", "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, id2, "name", "stephen", "age", 37);
        final Vertex v2 = batchGraph.addVertex(T.id, id1, "name", "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(2, IteratorUtils.count(vMarko.properties("age")));
        assertEquals(2, IteratorUtils.count(vMarko.properties("name")));
        assertTrue(((List) g.V(vMarko).valueMap().next().get("age")).contains(29));
        assertTrue(((List) g.V(vMarko).valueMap().next().get("age")).contains(34));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void shouldLoadVerticesIncrementallyWithNamedIdentifierAddMultiPropertyExistingVertex() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(2, IteratorUtils.count(vMarko.properties("age")));
        assertEquals(2, IteratorUtils.count(vMarko.properties("name")));
        assertTrue(((List) g.V(vMarko).valueMap().next().get("age")).contains(29));
        assertTrue(((List) g.V(vMarko).valueMap().next().get("age")).contains(34));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifier() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);

        final Object id1 = GraphManager.getGraphProvider().convertId("1", Edge.class);

        v1.addEdge("knows", v2, "weight", 1.0d, T.id, id1);
        v1.addEdge("knows", v2, "weight", 0.5d, T.id, id1); // second edge is ignored as it already exists
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(0), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifier() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, "abcde");
        v1.addEdge("knows", v2, "weight", 0.5d, T.id, "abcde"); // second edge is ignored as it already exists
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("uid", "abcde").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(0), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifierOverwriteSingleExistingEdge() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.IGNORE, Exists.OVERWRITE_SINGLE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Edge.class);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, id1);
        v1.addEdge("knows", v2, "weight", 0.5d, T.id, id1); // second edge is overwrites properties of the first
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(0), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierOverwriteSingleExistingEdge() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.IGNORE, Exists.OVERWRITE_SINGLE)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, "abcde");
        v1.addEdge("knows", v2, "weight", 0.5d, T.id, "abcde"); // second edge overwrites properties of the first
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(0), g.V(vStephen).outE("knows").has("uid", "abcde").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifierOverwriteExistingEdge() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.IGNORE, Exists.OVERWRITE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Edge.class);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, id1);
        v1.addEdge("knows", v2, "weight", 0.5d, T.id, id1); // second edge is overwrites properties of the first
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(0), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierOverwriteExistingEdge() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.IGNORE, Exists.OVERWRITE)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, "abcde");
        v1.addEdge("knows", v2, "weight", 0.5d, T.id, "abcde"); // second edge overwrites properties of the first
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(0), g.V(vStephen).outE("knows").has("uid", "abcde").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldLoadEdgesIncrementallyWithSuppliedIdentifierThrowOnExistingEdge() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.IGNORE, Exists.THROW)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        final Object id1 = GraphManager.getGraphProvider().convertId("1", Edge.class);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, id1);
        try {
            v1.addEdge("knows", v2, "weight", 0.5d, T.id, id1); // second edge is overwrites properties of the first
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierThrowOnExistingEdge() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true, Exists.IGNORE, Exists.THROW)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0d, T.id, "abcde");
        try {
            v1.addEdge("knows", v2, "weight", 0.5d, T.id, "abcde"); // second edge is overwrites properties of the first
            fail("Should have thrown an error because the vertex already exists");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadEdgesIncrementallyNoIdSpecified() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0d);
        v1.addEdge("knows", v2, "weight", 0.5d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldLoadEdgesIncrementallyWithNamedIdentifierAndNoIdSpecified() {
        final BatchGraph batchGraph = BatchGraph.build(graph)
                .incrementalLoading(true)
                .vertexIdKey("name")
                .edgeIdKey("uid")
                .bufferSize(1).create();
        final Vertex v2 = batchGraph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = batchGraph.addVertex(T.id, "stephen", "age", 37);
        v1.addEdge("knows", v2, "weight", 1.0d);
        v1.addEdge("knows", v2, "weight", 0.5d);
        tryCommit(batchGraph);

        final Vertex vStephen = g.V().has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());
        assertEquals(new Long(1), g.V(vStephen).outE("knows").has("weight", 0.5d).inV().has("name", "marko").count().next());

        final Vertex vMarko = g.V().has("name", "marko").next();
        assertEquals(29, vMarko.property("age").value());
    }
}
