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
package org.apache.tinkerpop.gremlin.server;

import io.netty.channel.ChannelPipeline;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReadLimits;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GraphBinaryReadLimitsChannelizerTest {

    private static AbstractChannelizer initChannelizer(final Settings settings) {
        final ServerGremlinExecutor executor = Mockito.mock(ServerGremlinExecutor.class);
        Mockito.when(executor.getSettings()).thenReturn(settings);

        final AbstractChannelizer channelizer = new AbstractChannelizer() {
            @Override
            public void configure(final ChannelPipeline pipeline) {
            }
        };
        channelizer.init(executor);
        return channelizer;
    }

    private static GraphBinaryReadLimits limitsOf(final AbstractChannelizer channelizer) {
        final GraphBinaryMessageSerializerV4 serializer =
                (GraphBinaryMessageSerializerV4) channelizer.serializers.get(SerTokens.MIME_GRAPHBINARY_V4);
        return serializer.getMapper().getReader().getLimits();
    }

    @Test
    public void shouldApplyDefaultLimitsWhenNotConfigured() {
        assertEquals(GraphBinaryReadLimits.DEFAULT, limitsOf(initChannelizer(new Settings())));
    }

    @Test
    public void shouldApplyConfiguredLimits() {
        final Settings settings = new Settings();
        settings.maxGraphBinaryValueLength = 100;
        settings.maxGraphBinaryArrayLength = 200;
        settings.maxGraphBinaryInitialCapacity = 300;

        assertEquals(GraphBinaryReadLimits.of(100, 200, 300), limitsOf(initChannelizer(settings)));
    }

    @Test
    public void shouldApplyConfiguredLimitsToSerializerDefinedInSettings() {
        final Settings settings = new Settings();
        settings.maxGraphBinaryValueLength = 100;
        settings.serializers = Collections.singletonList(new Settings.SerializerSettings(
                GraphBinaryMessageSerializerV4.class.getName(), Collections.emptyMap()));

        assertEquals(GraphBinaryReadLimits.of(100, GraphBinaryReadLimits.DEFAULT_MAX_ARRAY_LENGTH,
                GraphBinaryReadLimits.DEFAULT_MAX_INITIAL_CAPACITY), limitsOf(initChannelizer(settings)));
    }

    @Test
    public void shouldFailStartupOnInvalidLimit() {
        final Settings settings = new Settings();
        settings.maxGraphBinaryValueLength = 0;

        try {
            initChannelizer(settings);
            fail("An invalid limit should fail the startup rather than leave the serializer unavailable");
        } catch (IllegalArgumentException ex) {
            assertEquals("maxGraphBinaryValueLength must be greater than zero", ex.getMessage());
        }
    }
}
