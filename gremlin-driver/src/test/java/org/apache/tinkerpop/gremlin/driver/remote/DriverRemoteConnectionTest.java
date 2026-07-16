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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.tinkerpop.gremlin.driver.RequestOptions.getRequestOptions;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteConnectionTest {
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    @Test
    public void shouldBuildRequestOptions() {
        final RequestOptions options = getRequestOptions(
                g.with("x").
                        with("y", 100).
                        with(Tokens.ARGS_BATCH_SIZE, 1000).
                        with(Tokens.TIMEOUT_MILLIS, 100000L).
                        with(Tokens.ARGS_USER_AGENT, "test").
                        V().asAdmin().getGremlinLang());
        assertEquals(1000, options.getBatchSize().get().intValue());
        assertEquals(100000L, options.getTimeoutMillis().get().longValue());
    }

    @Test
    public void shouldBuildRequestOptionsWithNumerics() {
        final RequestOptions options = getRequestOptions(
                g.with(Tokens.ARGS_BATCH_SIZE, 100).
                  with(Tokens.TIMEOUT_MILLIS, 1000).
                  V().asAdmin().getGremlinLang());
        assertEquals(Integer.valueOf(100), options.getBatchSize().get());
        assertEquals(Long.valueOf(1000), options.getTimeoutMillis().get());
    }

    @Test
    public void shouldSubmitTraversalWithoutTraversalSourceAsParameter() throws Exception {
        final Client client = mockClient();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(client, "mySource");

        connection.submitAsync(g.V().asAdmin().getGremlinLang()).get();

        final RequestOptions options = captureRequestOptions(client, "g.V()");
        assertFalse(options.getParameters().orElse("").contains("\"g\":"));
    }

    @Test
    public void shouldSubmitTraversalWithOnlyExplicitParameters() throws Exception {
        final Client client = mockClient();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(client, "mySource");
        final GremlinLang gremlinLang = g.V(GValue.of("x", 42)).asAdmin().getGremlinLang();

        connection.submitAsync(gremlinLang).get();

        final RequestOptions options = captureRequestOptions(client, "g.V(x)");
        assertTrue(options.getParameters().isPresent());
        assertTrue(options.getParameters().get().contains("\"x\":42"));
        assertFalse(options.getParameters().get().contains("\"g\":"));
    }

    private static Client mockClient() {
        final Client client = mock(Client.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(Collections.emptyIterator());
        when(client.alias("mySource")).thenReturn(client);
        when(client.submitAsync(anyString(), any(RequestOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(resultSet));
        return client;
    }

    private static RequestOptions captureRequestOptions(final Client client, final String gremlin) {
        final ArgumentCaptor<RequestOptions> optionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);
        verify(client).submitAsync(eq(gremlin), optionsCaptor.capture());
        return optionsCaptor.getValue();
    }
}
