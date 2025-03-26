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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/**
 * Tests for options sent by the client using with().
 *
 * @author Ryan Tan
 */
public class ClientWithOptionsIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Test
    public void shouldTimeOutnonAliasedClientSendingByteCode() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client, "ggrateful"));
        assertThrows(CompletionException.class, () -> {
            final List<Vertex> res = g.with("evaluationTimeout", 1).V().both().both().both().toList();
            fail("Failed to time out. Result: " + res);
        });
    }

    @Test
    public void shouldTimeOutAliasedClientSubmittingScript() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect().alias("ggrateful");
        assertThrows(ExecutionException.class, () -> {
            final List<Result> res = client.submit("g.with(\"evaluationTimeout\", 1).V().both().both().both();").all().get();
            fail("Failed to time out. Result: " + res);
        });
    }

    @Test
    public void shouldTimeOutNonAliasedClientSubmittingScript() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();
        assertThrows(ExecutionException.class, () -> {
            final List<Result> res = client.submit("ggrateful.with(\"evaluationTimeout\", 1).V().both().both().both();").all().get();
            fail("Failed to time out. Result: " + res);
        });
    }
}
