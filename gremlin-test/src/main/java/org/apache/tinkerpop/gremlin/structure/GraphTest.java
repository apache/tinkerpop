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
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.structure.io.util.CustomId;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
        "vertexWithIdAlreadyExists",
        "elementNotFound",
        "idArgsMustBeEitherIdOrElement"
})
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class GraphTest extends AbstractGremlinTest {

    /**
     * Ensure compliance with Features by checking that all Features are exposed by the implementation.
     */
    @Test
    public void shouldImplementAndExposeFeatures() {
        final Graph.Features features = graph.features();
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
    public void shouldHaveExceptionConsistencyWhenFindVertexByIdThatIsNonExistentViaIterator() {
        try {
            graph.vertices(graphProvider.convertId(10000l, Vertex.class)).next();
            fail("Call to g.vertices(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.elementNotFound(Vertex.class, graphProvider.convertId(10000l, Vertex.class)).getClass()));
        }
    }

    @Test
    public void shouldHaveExceptionConsistencyWhenFindEdgeByIdThatIsNonExistentViaIterator() {
        try {
            graph.edges(graphProvider.convertId(10000l, Edge.class)).next();
            fail("Call to g.edges(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(Graph.Exceptions.elementNotFound(Edge.class, graphProvider.convertId(10000l, Edge.class)).getClass()));
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    public void shouldHaveExceptionConsistencyWhenAssigningSameIdOnVertex() {
        final Object o = graphProvider.convertId("1", Vertex.class);
        graph.addVertex(T.id, o);
        try {
            graph.addVertex(T.id, o);
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
        graph.addVertex(T.id, 1000l);
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(1000l).next();
            assertEquals(1000l, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldAddVertexWithUserSuppliedStringId() {
        graph.addVertex(T.id, "1000");
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices("1000").next();
            assertEquals("1000", v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldAddVertexWithUserSuppliedUuidId() {
        final UUID uuid = UUID.randomUUID();
        graph.addVertex(T.id, uuid);
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(uuid).next();
            assertEquals(uuid, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ANY_IDS)
    public void shouldAddVertexWithUserSuppliedAnyIdUsingUuid() {
        final UUID uuid = UUID.randomUUID();
        graph.addVertex(T.id, uuid);
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(uuid).next();
            assertEquals(uuid, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ANY_IDS)
    public void shouldAddVertexWithUserSuppliedAnyIdUsingString() {
        final UUID uuid = UUID.randomUUID();
        graph.addVertex(T.id, uuid.toString());
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(uuid.toString()).next();
            assertEquals(uuid.toString(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ANY_IDS)
    public void shouldAddVertexWithUserSuppliedAnyIdUsingAnyObject() {
        final UUID uuid = UUID.randomUUID();

        // this is different from "FEATURE_CUSTOM_IDS" as TinkerGraph does not define a specific id class
        // (i.e. TinkerId) for the identifier.
        final CustomId customId = new CustomId("test", uuid);
        graph.addVertex(T.id, customId);
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(customId).next();
            assertEquals(customId, v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingDetachedVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(DetachedFactory.detach(v1, true)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(DetachedVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingReferenceVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(ReferenceFactory.detach(v1)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(ReferenceVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingStarVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(StarGraph.of(v1).getStarVertex()).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(StarGraph.StarVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingVertexId() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingStringRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id().toString()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingVertices() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1, v2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingVertexIds() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id(), v2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_UUID_IDS)
    public void shouldIterateVerticesWithUuidIdSupportUsingStringRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, UUID.randomUUID()) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id().toString(), v2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingVertex() {
        final Vertex v1 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingDetachedVertex() {
        final Vertex v1 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(DetachedFactory.detach(v1, true)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(DetachedVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingReferenceVertex() {
        final Vertex v1 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(ReferenceFactory.detach(v1)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(ReferenceVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingStarVertex() {
        final Vertex v1 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(StarGraph.of(v1).getStarVertex()).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(StarGraph.StarVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingVertexId() {
        final Vertex v1 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingStringRepresentation() {
        final Vertex v1 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id().toString()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingVertices() {
        final Vertex v1 = graph.addVertex();
        final Vertex v2 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1, v2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingVertexIds() {
        final Vertex v1 = graph.addVertex();
        final Vertex v2 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id(), v2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateVerticesWithCustomIdSupportUsingStringRepresentations() {
        final Vertex v1 = graph.addVertex();
        final Vertex v2 = graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id().toString(), v2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericSupportUsingDetachedVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(DetachedFactory.detach(v1, true)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(DetachedVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericSupportUsingReferenceVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(ReferenceFactory.detach(v1)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(ReferenceVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericSupportUsingStarVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(StarGraph.of(v1).getStarVertex()).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(StarGraph.StarVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingVertexId() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingLongRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(Long.parseLong(v1.id().toString())).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(Integer.parseInt(v1.id().toString())).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(Float.parseFloat(v1.id().toString())).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(Double.parseDouble(v1.id().toString())).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingStringRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id().toString()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingVertices() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1, v2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingVertexIds() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id(), v2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingLongRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(Long.parseLong(v1.id().toString()), Long.parseLong(v2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(Integer.parseInt(v1.id().toString()), Integer.parseInt(v2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(Float.parseFloat(v1.id().toString()), Float.parseFloat(v2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(Double.parseDouble(v1.id().toString()), Double.parseDouble(v2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateVerticesWithNumericIdSupportUsingStringRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 1l) : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, 2l) : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id().toString(), v2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldNotMixTypesForGettingSpecificVerticesWithVertexFirst() {
        final Vertex v1 = graph.addVertex();
        try {
            graph.vertices(v1, graphProvider.convertId("1", Vertex.class));
            fail("Should have thrown an exception because id arguments were mixed.");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldNotMixTypesForGettingSpecificVerticesWithStringFirst() {
        final Vertex v1 = graph.addVertex();
        try {
            graph.vertices(graphProvider.convertId("1", Vertex.class), v1);
            fail("Should have thrown an exception because id arguments were mixed.");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringIdSupportUsingVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringSupportUsingDetachedVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(DetachedFactory.detach(v1, true)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(DetachedVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringSupportUsingReferenceVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(ReferenceFactory.detach(v1)).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(ReferenceVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringSupportUsingStarVertex() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(StarGraph.of(v1).getStarVertex()).next();
            assertEquals(v1.id(), v.id());
            assertThat(v, is(not(instanceOf(StarGraph.StarVertex.class))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringIdSupportUsingVertexId() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringIdSupportUsingStringRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            final Vertex v = graph.vertices(v1.id().toString()).next();
            assertEquals(v1.id(), v.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringIdSupportUsingVertices() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "2") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1, v2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringIdSupportUsingVertexIds() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "2") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id(), v2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldIterateVerticesWithStringIdSupportUsingStringRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v1 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "1") : graph.addVertex();
        final Vertex v2 = graph.features().vertex().supportsUserSuppliedIds() ? graph.addVertex(T.id, "2") : graph.addVertex();
        graph.addVertex();
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.vertices(v1.id().toString(), v2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES, supported = false)
    public void shouldOverwriteEarlierKeyValuesWithLaterKeyValuesOnAddVertexIfNoMultiProperty() {
        final Vertex v = graph.addVertex("test", "A", "test", "B", "test", "C");
        tryCommit(graph, graph -> {
            assertEquals(1, IteratorUtils.count(v.properties("test")));
            assertTrue(IteratorUtils.stream(v.values("test")).anyMatch(t -> t.equals("C")));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void shouldOverwriteEarlierKeyValuesWithLaterKeyValuesOnAddVertexIfMultiProperty() {
        final Vertex v = graph.addVertex("test", "A", "test", "B", "test", "C");
        tryCommit(graph, graph -> {
            assertEquals(3, IteratorUtils.count(v.properties("test")));
            assertTrue(IteratorUtils.stream(v.values("test")).anyMatch(t -> t.equals("A")));
            assertTrue(IteratorUtils.stream(v.values("test")).anyMatch(t -> t.equals("B")));
            assertTrue(IteratorUtils.stream(v.values("test")).anyMatch(t -> t.equals("C")));
        });
    }

    /**
     * Graphs should have a standard toString representation where the value is generated by
     * {@link org.apache.tinkerpop.gremlin.structure.util.StringFactory#graphString(Graph, String)}.
     */
    @Test
    public void shouldHaveStandardStringRepresentation() throws Exception {
        assertTrue(graph.toString().matches(".*\\[.*\\]"));
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

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(graph.addVertex()));
        tryCommit(graph, assertVertexEdgeCounts(vertexCount, 0));

        IntStream.range(0, edgeCount).forEach(i -> {
            boolean created = false;
            while (!created) {
                final Vertex a = vertices.get(random.nextInt(vertices.size()));
                final Vertex b = vertices.get(random.nextInt(vertices.size()));
                if (a != b) {
                    edges.add(a.addEdge(GraphManager.getGraphProvider().convertLabel("a" + UUID.randomUUID()), b));
                    created = true;
                }
            }
        });

        tryCommit(graph, assertVertexEdgeCounts(vertexCount, edgeCount));

        int counter = 0;
        for (Edge e : edges) {
            counter = counter + 1;
            e.remove();

            final int currentCounter = counter;
            tryCommit(graph, assertVertexEdgeCounts(vertexCount, edgeCount - currentCounter));
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

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(graph.addVertex()));
        tryCommit(graph, assertVertexEdgeCounts(vertexCount, 0));

        for (int i = 0; i < vertexCount; i = i + 2) {
            final Vertex a = vertices.get(i);
            final Vertex b = vertices.get(i + 1);
            edges.add(a.addEdge(GraphManager.getGraphProvider().convertLabel("a" + UUID.randomUUID()), b));
        }

        tryCommit(graph, assertVertexEdgeCounts(vertexCount, vertexCount / 2));

        int counter = 0;
        for (Vertex v : vertices) {
            counter = counter + 1;
            v.remove();

            if ((counter + 1) % 2 == 0) {
                final int currentCounter = counter;
                tryCommit(graph, assertVertexEdgeCounts(
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
            graph.addVertex();
        }
        final Iterator<Vertex> vertexIterator = graph.vertices();
        assertTrue(vertexIterator.hasNext());
        while (vertexIterator.hasNext()) {
            vertexIterator.next().remove();
        }
        assertFalse(vertexIterator.hasNext());
        tryCommit(graph, graph -> assertFalse(graph.vertices().hasNext()));
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
            graph.addVertex().addEdge("link", graph.addVertex());
        }

        final Iterator<Edge> edgeIterator = graph.edges();
        assertTrue(edgeIterator.hasNext());
        while (edgeIterator.hasNext()) {
            edgeIterator.next().remove();
        }
        assertFalse(edgeIterator.hasNext());
        tryCommit(graph, g -> assertFalse(g.edges().hasNext()));
    }

    /**
     * Create a small {@link org.apache.tinkerpop.gremlin.structure.Graph} and ensure that counts of edges per vertex are correct.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldEvaluateConnectivityPatterns() {
        final GraphProvider graphProvider = GraphManager.getGraphProvider();

        final Vertex a;
        final Vertex b;
        final Vertex c;
        final Vertex d;
        if (graph.features().vertex().supportsUserSuppliedIds()) {
            a = graph.addVertex(T.id, graphProvider.convertId("1", Vertex.class));
            b = graph.addVertex(T.id, graphProvider.convertId("2", Vertex.class));
            c = graph.addVertex(T.id, graphProvider.convertId("3", Vertex.class));
            d = graph.addVertex(T.id, graphProvider.convertId("4", Vertex.class));
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

        graph.vertices().forEachRemaining(v -> {
            assertEquals(1l, IteratorUtils.count(v.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(v.edges(Direction.IN)));
        });

        graph.edges().forEachRemaining(x -> {
            assertEquals(graphProvider.convertLabel("knows"), x.label());
        });

        if (graph.features().vertex().supportsUserSuppliedIds()) {
            final Vertex va = graph.vertices(graphProvider.convertId("1", Vertex.class)).next();
            final Vertex vb = graph.vertices(graphProvider.convertId("2", Vertex.class)).next();
            final Vertex vc = graph.vertices(graphProvider.convertId("3", Vertex.class)).next();
            final Vertex vd = graph.vertices(graphProvider.convertId("4", Vertex.class)).next();

            assertEquals(a, va);
            assertEquals(b, vb);
            assertEquals(c, vc);
            assertEquals(d, vd);

            assertEquals(1l, IteratorUtils.count(va.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(va.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vb.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vb.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.OUT)));

            final Edge i = a.addEdge(graphProvider.convertLabel("hates"), b);

            assertEquals(1l, IteratorUtils.count(va.edges(Direction.IN)));
            assertEquals(2l, IteratorUtils.count(va.edges(Direction.OUT)));
            assertEquals(2l, IteratorUtils.count(vb.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vb.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.OUT)));

            for (Edge x : IteratorUtils.list(a.edges(Direction.OUT))) {
                assertTrue(x.label().equals(graphProvider.convertLabel("knows")) || x.label().equals(graphProvider.convertLabel("hates")));
            }

            assertEquals(graphProvider.convertLabel("hates"), i.label());
            assertEquals(graphProvider.convertId("2", Vertex.class).toString(), i.inVertex().id().toString());
            assertEquals(graphProvider.convertId("1", Vertex.class).toString(), i.outVertex().id().toString());
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
        final GraphProvider graphProvider = GraphManager.getGraphProvider();

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

        List<Edge> results = IteratorUtils.list(a.edges(Direction.OUT));
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = IteratorUtils.list(a.edges(Direction.OUT, labelFriend));
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));

        results = IteratorUtils.list(a.edges(Direction.OUT, labelHate));
        assertEquals(1, results.size());
        assertTrue(results.contains(aHateC));

        results = IteratorUtils.list(a.edges(Direction.IN, labelHate));
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = IteratorUtils.list(a.edges(Direction.IN, labelFriend));
        assertEquals(0, results.size());

        results = IteratorUtils.list(b.edges(Direction.IN, labelHate));
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateB));

        results = IteratorUtils.list(b.edges(Direction.IN, labelFriend));
        assertEquals(1, results.size());
        assertTrue(results.contains(aFriendB));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter() {
        final GraphProvider graphProvider = GraphManager.getGraphProvider();
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

        List<Edge> results = IteratorUtils.list(a.edges(Direction.OUT, labelFriend, labelHate));
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = IteratorUtils.list(a.edges(Direction.IN, labelFriend, labelHate));
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = IteratorUtils.list(b.edges(Direction.IN, labelFriend, labelHate));
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(cHateB));

        results = IteratorUtils.list(b.edges(Direction.IN, graphProvider.convertLabel("blah1"), graphProvider.convertLabel("blah2")));
        assertEquals(0, results.size());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldTestTreeConnectivity() {
        final GraphProvider graphProvider = GraphManager.getGraphProvider();

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

        assertEquals(0l, IteratorUtils.count(start.edges(Direction.IN)));
        assertEquals(branchSize, IteratorUtils.count(start.edges(Direction.OUT)));
        for (Edge a : IteratorUtils.list(start.edges(Direction.OUT))) {
            assertEquals(graphProvider.convertLabel("test1"), a.label());
            assertEquals(branchSize, IteratorUtils.count(a.inVertex().vertices(Direction.OUT)));
            assertEquals(1, IteratorUtils.count(a.inVertex().vertices(Direction.IN)));
            for (Edge b : IteratorUtils.list(a.inVertex().edges(Direction.OUT))) {
                assertEquals(graphProvider.convertLabel("test2"), b.label());
                assertEquals(branchSize, IteratorUtils.count(b.inVertex().vertices(Direction.OUT)));
                assertEquals(1, IteratorUtils.count(b.inVertex().vertices(Direction.IN)));
                for (Edge c : IteratorUtils.list(b.inVertex().edges(Direction.OUT))) {
                    assertEquals(graphProvider.convertLabel("test3"), c.label());
                    assertEquals(0, IteratorUtils.count(c.inVertex().vertices(Direction.OUT)));
                    assertEquals(1, IteratorUtils.count(c.inVertex().vertices(Direction.IN)));
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
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_PERSISTENCE)
    public void shouldPersistDataOnClose() throws Exception {
        final GraphProvider graphProvider = GraphManager.getGraphProvider();

        final Vertex v = graph.addVertex();
        final Vertex u = graph.addVertex();
        if (graph.features().vertex().properties().supportsStringValues()) {
            v.property(VertexProperty.Cardinality.single, "name", "marko");
            u.property(VertexProperty.Cardinality.single, "name", "pavel");
        }

        final Edge e = v.addEdge(graphProvider.convertLabel("collaborator"), u);
        if (graph.features().edge().properties().supportsStringValues())
            e.property("location", "internet");

        tryCommit(graph, assertVertexEdgeCounts(2, 1));
        graph.close();

        final Graph reopenedGraph = graphProvider.standardTestGraph(this.getClass(), name.getMethodName(), null);
        assertVertexEdgeCounts(2, 1).accept(reopenedGraph);

        if (graph.features().vertex().properties().supportsStringValues()) {
            reopenedGraph.vertices().forEachRemaining(vertex -> {
                assertTrue(vertex.property("name").value().equals("marko") || vertex.property("name").value().equals("pavel"));
            });
        }

        reopenedGraph.edges().forEachRemaining(edge -> {
            assertEquals(graphProvider.convertLabel("collaborator"), edge.label());
            if (graph.features().edge().properties().supportsStringValues())
                assertEquals("internet", edge.property("location").value());
        });

        graphProvider.clear(reopenedGraph, graphProvider.standardGraphConfiguration(this.getClass(), name.getMethodName(), null));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
    public void shouldIterateEdgesWithUuidIdSupportUsingEdge() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
    public void shouldIterateEdgesWithUuidIdSupportUsingEdgeId() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
    public void shouldIterateEdgesWithUuidIdSupportUsingStringRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id().toString()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
    public void shouldIterateEdgesWithUuidIdSupportUsingEdges() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1, e2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
    public void shouldIterateEdgesWithUuidIdSupportUsingEdgeIds() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id(), e2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
    public void shouldIterateEdgesWithUuidIdSupportUsingStringRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, UUID.randomUUID()) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id().toString(), e2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateEdgesWithCustomIdSupportUsingEdge() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateEdgesWithCustomIdSupportUsingEdgeId() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateEdgesWithCustomIdSupportUsingStringRepresentation() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id().toString()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateEdgesWithCustomIdSupportUsingEdges() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        final Edge e2 = v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1, e2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateEdgesWithCustomIdSupportUsingEdgeIds() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        final Edge e2 = v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id(), e2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_CUSTOM_IDS)
    public void shouldIterateEdgesWithCustomIdSupportUsingStringRepresentations() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        final Edge e2 = v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id().toString(), e2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingEdge() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingEdgeId() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingLongRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(Long.parseLong(e1.id().toString())).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(Integer.parseInt(e1.id().toString())).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(Float.parseFloat(e1.id().toString())).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(Double.parseDouble(e1.id().toString())).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingStringRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id().toString()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingVertices() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1, e2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingEdgeIds() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id(), e2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingLongRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(Long.parseLong(e1.id().toString()), Long.parseLong(e2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(Integer.parseInt(e1.id().toString()), Integer.parseInt(e2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(Float.parseFloat(e1.id().toString()), Float.parseFloat(e2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(Double.parseDouble(e1.id().toString()), Double.parseDouble(e2.id().toString()))));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
    public void shouldIterateEdgesWithNumericIdSupportUsingStringRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 1l) : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, 2l) : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id().toString(), e2.id().toString())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotMixTypesForGettingSpecificEdgesWithEdgeFirst() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        try {
            graph.edges(e1, graphProvider.convertId("1", Edge.class));
            fail("Should have thrown an exception because id arguments were mixed.");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotMixTypesForGettingSpecificEdgesWithStringFirst() {
        final Vertex v = graph.addVertex();
        final Edge e1 = v.addEdge("self", v);
        try {
            graph.edges(graphProvider.convertId("1", Edge.class), e1);
            fail("Should have thrown an exception because id arguments were mixed.");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
    public void shouldIterateEdgesWithStringIdSupportUsingEdge() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "1") : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
    public void shouldIterateEdgesWithStringIdSupportUsingEdgeId() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "1") : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
    public void shouldIterateEdgesWithStringIdSupportUsingStringRepresentation() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "1") : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            final Edge e = graph.edges(e1.id().toString()).next();
            assertEquals(e1.id(), e.id());
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
    public void shouldIterateEdgesWithStringIdSupportUsingEdges() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "1") : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "2") : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1, e2)));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
    public void shouldIterateEdgesWithStringIdSupportUsingEdgeIds() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "1") : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "2") : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id(), e2.id())));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
    public void shouldIterateEdgesWithStringIdSupportUsingStringRepresentations() {
        // if the graph supports id assigned, it should allow it.  if the graph does not, it will generate one
        final Vertex v = graph.addVertex();
        final Edge e1 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "1") : v.addEdge("self", v);
        final Edge e2 = graph.features().edge().supportsUserSuppliedIds() ? v.addEdge("self", v, T.id, "2") : v.addEdge("self", v);
        v.addEdge("self", v);
        tryCommit(graph, graph -> {
            assertEquals(2, IteratorUtils.count(graph.edges(e1.id().toString(), e2.id().toString())));
        });
    }
}
