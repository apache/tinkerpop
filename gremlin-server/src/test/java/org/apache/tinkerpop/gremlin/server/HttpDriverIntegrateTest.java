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
package org.apache.tinkerpop.gremlin.server;

import io.netty.handler.codec.EncoderException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        return settings;
    }

    @Test
    public void shouldSubmitScriptWithGraphSON() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHSON_V3)
                .create();
        try {
            final Client client = cluster.connect();
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitScriptWithGraphBinary() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHBINARY_V1)
                .create();
        try {
            final Client client = cluster.connect();
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitBytecodeWithGraphSON() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHSON_V3)
                .create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            final String result = g.inject("2").toList().get(0);
            assertEquals("2", result);
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetErrorForBytecodeWithUntypedGraphSON() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHSON_V2_UNTYPED)
                .create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            g.inject("2").toList();
            fail("Exception expected");
        } catch (EncoderException ex) {
            assertThat(ex.getMessage(), allOf(containsString("An error occurred during serialization of this request"),
                    containsString("it could not be sent to the server - Reason: only GraphSON3 and GraphBinary recommended for serialization of Bytecode requests, but used org.apache.tinkerpop.gremlin.")));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitBytecodeWithGraphBinary() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHBINARY_V1)
                .create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            final String result = g.inject("2").toList().get(0);
            assertEquals("2", result);
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitMultipleRequestsOverSingleConnection() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .minConnectionPoolSize(1).maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHBINARY_V1)
                .create();
        try {
            for (int ix = 0; ix < 100; ix++) {
                final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
                assertEquals(ix, g.inject(ix).toList().get(0).intValue());
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitMultipleRequestsOverMultiConnection() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .minConnectionPoolSize(1).maxConnectionPoolSize(8)
                .serializer(Serializers.GRAPHBINARY_V1)
                .create();
        try {
            for (int ix = 0; ix < 100; ix++) {
                final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
                assertEquals(ix, g.inject(ix).toList().get(0).intValue());
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailToUseSession() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHBINARY_V1)
                .create();
        try {
            final Client client = cluster.connect("shouldFailToUseSession");
            client.submit("1+1").all().get();
            fail("Can't use session with HTTP");
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            assertEquals("Cannot use sessions or tx() with HttpChannelizer", t.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailToUseTx() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .channelizer(Channelizer.HttpChannelizer.class)
                .serializer(Serializers.GRAPHBINARY_V1)
                .create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            final Transaction tx = g.tx();
            final GraphTraversalSource gtx = tx.begin();
            gtx.inject("1").toList();
            fail("Can't use tx() with HTTP");
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            assertEquals("Cannot use sessions or tx() with HttpChannelizer", t.getMessage());
        } finally {
            cluster.close();
        }
    }
}
