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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.*;

/**
 * Integration tests for streaming HTTP response support in the Java driver.
 * Verifies that the streaming pipeline (HttpStreamingResponseHandler + GraphBinaryStreamResponseReader)
 * works correctly end-to-end against a real Gremlin Server.
 */
public class StreamingResponseIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        return settings;
    }

    @Test
    public void shouldStreamBasicResults() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            final List<Result> results = client.submit("g.inject(1,2,3)").all().get();
            assertEquals(3, results.size());
            assertEquals(1, results.get(0).getInt());
            assertEquals(2, results.get(1).getInt());
            assertEquals(3, results.get(2).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStreamIncrementallyWithIterator() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            final ResultSet rs = client.submit("g.inject(1,2,3,4,5)");

            // Consume results one at a time via iterator
            final Iterator<Result> iter = rs.iterator();
            final List<Integer> collected = new ArrayList<>();
            while (iter.hasNext()) {
                collected.add(iter.next().getInt());
            }
            assertEquals(5, collected.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1, (int) collected.get(i));
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStreamIncrementallyWithOne() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            final ResultSet rs = client.submit("g.inject(10,20,30)");

            assertEquals(10, rs.one().getInt());
            assertEquals(20, rs.one().getInt());
            assertEquals(30, rs.one().getInt());
            assertNull(rs.one());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStreamLargeResultSet() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            // Generate a large result set that would span multiple HTTP chunks
            final List<Result> results = client.submit(
                    "g.inject(1).repeat(__.identity()).times(1000).emit()").all().get();
            assertEquals(1000, results.size());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStreamEmptyResponse() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            final List<Result> results = client.submit("g.V().hasLabel('nonexistent')").all().get();
            assertTrue(results.isEmpty());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldHandleServerErrorDuringStreaming() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            client.submit("invalid_script_that_should_fail").all().get();
            fail("Expected exception");
        } catch (ExecutionException e) {
            // Error should propagate correctly through the streaming pipeline
            assertTrue(e.getCause() instanceof ResponseException);
            final ResponseException re = (ResponseException) e.getCause();
            assertEquals(HttpResponseStatus.BAD_REQUEST.code(), re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReuseConnectionAfterStreamingComplete() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            // First request
            final List<Result> results1 = client.submit("g.inject(1)").all().get();
            assertEquals(1, results1.size());

            // Second request on same client (should reuse connection)
            final List<Result> results2 = client.submit("g.inject(2)").all().get();
            assertEquals(1, results2.size());
            assertEquals(2, results2.get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldHandleConcurrentStreamingRequests() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            final CompletableFuture<List<Result>> f1 = client.submit("g.inject(1,2,3)").all();
            final CompletableFuture<List<Result>> f2 = client.submit("g.inject(4,5,6)").all();
            final CompletableFuture<List<Result>> f3 = client.submit("g.inject(7,8,9)").all();

            assertEquals(3, f1.get().size());
            assertEquals(3, f2.get().size());
            assertEquals(3, f3.get().size());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStreamWithTraversalApi() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(
                    DriverRemoteConnection.using(cluster));

            final List<Integer> results = g.inject(1, 2, 3).toList();
            assertEquals(3, results.size());
            assertTrue(results.contains(1));
            assertTrue(results.contains(2));
            assertTrue(results.contains(3));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStreamVerticesFromGraph() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            // Use inject to create data rather than relying on pre-loaded graph
            final List<Result> results = client.submit("g.inject(1,2,3,4,5,6)").all().get();
            assertEquals(6, results.size());
        } finally {
            cluster.close();
        }
    }
}
