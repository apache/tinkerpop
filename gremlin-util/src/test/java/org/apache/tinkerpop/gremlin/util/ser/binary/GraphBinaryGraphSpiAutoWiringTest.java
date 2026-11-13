/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphDefaultProvider;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validates that {@code tinkergraph-gremlin} on the (class or module) path auto-wires
 * {@link org.apache.tinkerpop.gremlin.structure.io.binary.types.GraphSerializer} with a {@link DefaultGraphProvider}
 * and that a {@code Graph} result round-trips through {@link GraphBinaryWriter}/{@link GraphBinaryReader}
 * without naming {@link TinkerGraph} directly. The companion "no provider" case is covered in
 * {@code gremlin-core}, which has no dependency on {@code tinkergraph-gremlin} and therefore sees no provider.
 */
public class GraphBinaryGraphSpiAutoWiringTest {

    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @Test
    public void shouldFindTinkerGraphDefaultProviderOnThePath() {
        final Optional<DefaultGraphProvider> provider = DefaultGraphProvider.find();
        assertTrue(provider.isPresent());
        assertTrue(provider.get() instanceof TinkerGraphDefaultProvider);
    }

    @Test
    public void shouldDeserializeGraphResultThroughSpiDiscoveredProvider() throws Exception {
        final Graph source = TinkerGraph.open();
        final Vertex v = source.addVertex(T.id, 1, T.label, "person");
        v.property("name", "marko");

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        try {
            writer.write(source, buffer);
            buffer.readerIndex(0);

            final Graph result = (Graph) reader.read(buffer);
            final Vertex deserialized = result.vertices(1).next();
            assertEquals("person", deserialized.label());
            assertEquals("marko", deserialized.value("name"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldOpenGraphWithProvidedConfiguration() {
        final DefaultGraphProvider provider = DefaultGraphProvider.find()
                .orElseThrow(() -> new IllegalStateException(DefaultGraphProvider.NO_PROVIDER_MESSAGE));
        final Graph graph = provider.open(new BaseConfiguration());
        assertTrue(graph instanceof TinkerGraph);

        // recorded as GraphFactory would, which keeps subgraph()'s default graph configured as it was before the SPI
        assertEquals(TinkerGraph.class.getName(), graph.configuration().getString(Graph.GRAPH));
    }
}
