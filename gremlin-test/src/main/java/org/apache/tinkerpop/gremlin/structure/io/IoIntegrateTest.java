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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.javatuples.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoIntegrateTest extends AbstractGremlinTest {
    private static final Logger logger = LoggerFactory.getLogger(IoIntegrateTest.class);

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shouldHaveSizeOfStarGraphLessThanDetached() throws Exception {
        final Random random = TestHelper.RANDOM;
        final Vertex vertex = graph.addVertex("person");
        for (int i = 0; i < 100; i++) { // vertex properties and meta properties
            vertex.property(VertexProperty.Cardinality.list, UUID.randomUUID().toString(), random.nextDouble(),
                    "acl", random.nextBoolean() ? "public" : "private",
                    "created", random.nextLong());
        }
        for (int i = 0; i < 50000; i++) {  // edges and edge properties
            vertex.addEdge("knows", graph.addVertex("person"), "since", random.nextLong(), "acl", random.nextBoolean() ? "public" : "private");
            graph.addVertex("software").addEdge("createdBy", vertex, "date", random.nextLong());
            graph.addVertex("group").addEdge("hasMember", vertex);
        }
        ///////////////
        Pair<StarGraph, Integer> pair = serializeDeserialize(StarGraph.of(vertex));
        int starGraphSize = pair.getValue1();
        TestHelper.validateEquality(vertex, pair.getValue0().getStarVertex());
        ///
        pair = serializeDeserialize(pair.getValue0());
        assertEquals(starGraphSize, pair.getValue1().intValue());
        starGraphSize = pair.getValue1();
        TestHelper.validateEquality(vertex, pair.getValue0().getStarVertex());
        ///
        pair = serializeDeserialize(pair.getValue0());
        assertEquals(starGraphSize, pair.getValue1().intValue());
        starGraphSize = pair.getValue1();
        TestHelper.validateEquality(vertex, pair.getValue0().getStarVertex());
        ///
        // this is a rough approximation of "detached" serialization of all vertices and edges.
        // now that writeVertex in gryo writes StarGraph that approach can't be used anymore to test
        // serialization size of detached.
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final GryoWriter writer = graph.io(IoCore.gryo()).writer().create();
        writer.writeObject(outputStream, DetachedFactory.detach(vertex, true));
        vertex.edges(Direction.BOTH).forEachRemaining(e -> writer.writeObject(outputStream, DetachedFactory.detach(e, true)));
        final int detachedVertexSize = outputStream.size();
        assertTrue(starGraphSize < detachedVertexSize);

        logger.info("Size of star graph:        {}", starGraphSize);
        logger.info("Size of detached vertex:   {}", detachedVertexSize);
        logger.info("Size reduction:            {}", (float) detachedVertexSize / (float) starGraphSize);
    }

    private Pair<StarGraph, Integer> serializeDeserialize(final StarGraph starGraph) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            graph.io(IoCore.gryo()).writer().create().writeObject(outputStream, starGraph);
            return Pair.with(graph.io(IoCore.gryo()).reader().create().readObject(new ByteArrayInputStream(outputStream.toByteArray()), StarGraph.class), outputStream.size());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

}
