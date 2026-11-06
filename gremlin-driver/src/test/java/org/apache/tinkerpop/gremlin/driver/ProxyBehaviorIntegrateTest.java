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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.socket.server.RecordingProxyServer;
import org.apache.tinkerpop.gremlin.socket.server.SimpleTestServer;
import org.apache.tinkerpop.gremlin.socket.server.SocketServerConstants;
import org.apache.tinkerpop.gremlin.socket.server.TestHttpServerInitializer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Proves that {@link Cluster.Builder#proxy(ProxyOptions)} routes driver traffic through an HTTP proxy.
 * <p>
 * A {@link SimpleTestServer} (the real Gremlin socket server) runs on {@link SocketServerConstants#PORT}
 * and an in-process {@link RecordingProxyServer} runs on {@link SocketServerConstants#PROXY_PORT}. The
 * proxy records the {@code host:port} target of every tunnel it establishes and exposes a small control
 * API ({@code GET /__recorded}, {@code POST /__reset}) that the tests query with the JDK11
 * {@link java.net.http.HttpClient}.
 */
public class ProxyBehaviorIntegrateTest {

    private static final int PORT = SocketServerConstants.PORT;
    private static final int PROXY_PORT = SocketServerConstants.PROXY_PORT;

    private static SimpleTestServer server;
    private static RecordingProxyServer proxyServer;
    private static HttpClient httpClient;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        server = new SimpleTestServer(PORT);
        server.start(new TestHttpServerInitializer());

        proxyServer = new RecordingProxyServer(PROXY_PORT);
        proxyServer.start();

        httpClient = HttpClient.newHttpClient();
    }

    @AfterClass
    public static void tearDown() {
        if (server != null) server.stop();
        if (proxyServer != null) proxyServer.stop();
    }

    private static Cluster.Builder buildCluster() {
        return Cluster.build("localhost")
                .validationRequest(SocketServerConstants.GREMLIN_SINGLE_VERTEX)
                .port(PORT)
                .responseSerializer(new GraphBinaryMessageSerializerV4());
    }

    private static String getRecorded() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PROXY_PORT + "/__recorded"))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private static void resetRecorded() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PROXY_PORT + "/__reset"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    public void shouldRouteTrafficThroughProxy() throws Exception {
        resetRecorded();

        final Cluster cluster = buildCluster()
                .proxy(ProxyOptions.create("localhost", PROXY_PORT))
                .create();
        try {
            final Client client = cluster.connect();
            final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
            assertEquals(1, results.size());
            assertThat(results.get(0).getObject(), instanceOf(Vertex.class));

            // the driver issues an HTTP CONNECT, so the proxy should have recorded the tunnel to the
            // socket server. Match on the ":45943" substring to stay tolerant of the host form.
            final String recorded = getRecorded();
            assertTrue("Expected the proxy to have recorded the tunnel to :" + PORT + " but was: " + recorded,
                    recorded.contains(":" + PORT));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotRecordWithoutProxy() throws Exception {
        resetRecorded();

        final Cluster cluster = buildCluster().create();
        try {
            final Client client = cluster.connect();
            final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
            assertEquals(1, results.size());
            assertThat(results.get(0).getObject(), instanceOf(Vertex.class));

            // without a proxy configured the driver connects directly, so nothing should be recorded.
            final String recorded = getRecorded();
            assertFalse("Expected no tunnel to :" + PORT + " to be recorded but was: " + recorded,
                    recorded.contains(":" + PORT));
        } finally {
            cluster.close();
        }
    }
}
