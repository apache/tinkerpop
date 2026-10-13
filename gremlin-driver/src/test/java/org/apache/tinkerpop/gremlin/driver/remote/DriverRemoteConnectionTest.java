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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
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

    /**
     * Builds a {@link Cluster} that never connects to a live server. Creation and {@code connect()}/{@code alias()}
     * are lazy in the driver, so no I/O happens until a traversal is submitted.
     */
    private static Cluster newCluster() {
        return Cluster.build("localhost").port(45940).create();
    }

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
    public void shouldConnectUsingClusterWithDefaultTraversalSource() throws Exception {
        final Cluster cluster = newCluster();
        try {
            final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
            assertThat(connection.toString(), containsString("[graph=g]"));
            // safe to close; leaves the caller-supplied cluster open (tryCloseCluster == false)
            connection.close();
            assertFalse(cluster.isClosed());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldConnectUsingClusterWithCustomTraversalSource() throws Exception {
        final Cluster cluster = newCluster();
        try {
            final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster, "gods");
            assertThat(connection.toString(), containsString("[graph=gods]"));
            connection.close();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldConnectUsingClientWithDefaultTraversalSource() throws Exception {
        final Cluster cluster = newCluster();
        try {
            final Client client = cluster.connect();
            final DriverRemoteConnection connection = DriverRemoteConnection.using(client);
            assertThat(connection.toString(), containsString("[graph=g]"));
            // close() is a no-op for the using(Client) path so the supplied client remains usable
            connection.close();
        } finally {
            cluster.close();
        }
    }


    @Test
    public void shouldConnectUsingHostAndPort() throws Exception {
        final DriverRemoteConnection connection = DriverRemoteConnection.using("localhost", 45940);
        try {
            assertThat(connection.toString(), containsString("[graph=g]"));
        } finally {
            // the underlying cluster is created internally; close it to avoid leaking resources
            connection.client.getCluster().close();
        }
    }


    @Test
    public void shouldConstructFromConfigurationWithDefaults() throws Exception {
        // No clusterFile and no clusterConfiguration => defaults to Cluster.open() (localhost) and source "g"
        final Configuration conf = new BaseConfiguration();
        final DriverRemoteConnection connection = new DriverRemoteConnection(conf);
        try {
            assertThat(connection.toString(), containsString("[graph=g]"));
        } finally {
            connection.close();
        }
    }

    @Test
    public void shouldConstructFromConfigurationWithSourceName() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "gods");
        final DriverRemoteConnection connection = new DriverRemoteConnection(conf);
        try {
            assertThat(connection.toString(), containsString("[graph=gods]"));
        } finally {
            connection.close();
        }
    }

    @Test
    public void shouldConstructFromConfigurationWithClusterConfiguration() throws Exception {
        // 'clusterConfiguration' subset is passed to Cluster.open(Configuration); hosts is required
        final Configuration conf = new BaseConfiguration();
        conf.setProperty("clusterConfiguration.hosts", "localhost");
        conf.setProperty("clusterConfiguration.port", 45940);
        conf.setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "gods");
        final DriverRemoteConnection connection = new DriverRemoteConnection(conf);
        try {
            assertThat(connection.toString(), containsString("[graph=gods]"));
        } finally {
            connection.close();
        }
    }

    @Test
    public void shouldThrowOnConfigurationWithBothClusterFileAndClusterConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_CLUSTERFILE, "conf/remote-objects.yaml");
        conf.setProperty("clusterConfiguration.hosts", "localhost");
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new DriverRemoteConnection(conf));
        assertThat(ex.getMessage(), containsString("should not contain both"));
    }

    @Test
    public void shouldConstructFromClusterAndConfiguration() throws Exception {
        // package-private constructor used for testing: reads sourceName and 'attachment' from the configuration
        final Cluster cluster = newCluster();
        try {
            final Configuration conf = new BaseConfiguration();
            conf.setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "gods");
            conf.setProperty(RemoteConnection.GREMLIN_REMOTE + "attachment", true);
            final DriverRemoteConnection connection = new DriverRemoteConnection(cluster, conf);
            assertThat(connection.toString(), containsString("[graph=gods]"));
            // tryCloseCluster is false for this constructor, so the supplied cluster stays open
            connection.close();
            assertFalse(cluster.isClosed());
        } finally {
            cluster.close();
        }
    }


    @Test
    public void shouldThrowOnUsingConfigurationWithBothClusterFileAndClusterConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty("clusterConfigurationFile", "conf/remote-objects.yaml");
        conf.setProperty("clusterConfiguration", "localhost");
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DriverRemoteConnection.using(conf));
        assertEquals("A configuration should not contain both 'clusterConfigurationFile' and 'clusterConfiguration'",
                ex.getMessage());
    }

    @Test
    public void shouldThrowOnUsingConfigurationWithNeitherClusterFileNorClusterConfiguration() {
        final Configuration conf = new BaseConfiguration();
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DriverRemoteConnection.using(conf));
        assertEquals("A configuration must contain either 'clusterConfigurationFile' and 'clusterConfiguration'",
                ex.getMessage());
    }

    @Test
    public void shouldThrowOnUsingNonExistentClusterConfigurationFile() {
        // Cluster.open(file) finds no file on disk, then tries the classpath; getResource returns null for a truly
        // missing file, so resource.getFile() throws NPE. using(String, String) wraps that as IllegalStateException.
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DriverRemoteConnection.using("this-file-does-not-exist.yaml", "g"));
        assertTrue("expected the underlying NullPointerException as the wrapped cause",
                ex.getCause() instanceof NullPointerException);
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
