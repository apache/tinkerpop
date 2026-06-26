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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the standardized GLV connection options on the {@link Cluster.Builder}: canonical names and defaults.
 */
public class ConnectionOptionsTest {

    @Test
    public void shouldUseCanonicalDefaults() {
        final Cluster cluster = Cluster.build("localhost").create();
        assertEquals(128, cluster.maxConnections());
        assertEquals(64, cluster.getBatchSize());
        assertEquals(5000, cluster.getConnectTimeout());
        assertEquals(180000L, cluster.getIdleTimeout());
        assertEquals(30000L, cluster.getKeepAliveTime());
        assertEquals(0L, cluster.getReadTimeout());
        assertEquals(8192, cluster.getMaxResponseHeaderBytes());
        assertEquals(Compression.DEFLATE, cluster.getCompression());
        assertNull(cluster.getProxy());
        cluster.close();
    }

    @Test
    public void shouldSetCanonicalConnectionOptions() {
        final Cluster cluster = Cluster.build("localhost")
                .maxConnections(8)
                .batchSize(100)
                .connectTimeoutMillis(2500)
                .idleTimeoutMillis(120000)
                .readTimeoutMillis(30000)
                .maxResponseHeaderBytes(16384)
                .create();
        assertEquals(8, cluster.maxConnections());
        assertEquals(100, cluster.getBatchSize());
        assertEquals(2500, cluster.getConnectTimeout());
        assertEquals(120000L, cluster.getIdleTimeout());
        assertEquals(30000L, cluster.getReadTimeout());
        assertEquals(16384, cluster.getMaxResponseHeaderBytes());
        cluster.close();
    }

    @Test
    public void shouldSetTimeoutsViaDuration() {
        final Cluster cluster = Cluster.build("localhost")
                .connectTimeout(Duration.ofSeconds(2))
                .idleTimeout(Duration.ofMinutes(2))
                .readTimeout(Duration.ofSeconds(30))
                .keepAliveTime(Duration.ofSeconds(45))
                .create();
        // Duration overloads convert to the same millisecond values as the *Millis setters.
        assertEquals(2000, cluster.getConnectTimeout());
        assertEquals(120000L, cluster.getIdleTimeout());
        assertEquals(30000L, cluster.getReadTimeout());
        assertEquals(45000L, cluster.getKeepAliveTime());
        cluster.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectConnectTimeoutDurationExceedingIntMillis() {
        Cluster.build("localhost").connectTimeout(Duration.ofMillis((long) Integer.MAX_VALUE + 1));
    }

    @Test
    public void shouldSetCompressionViaEnum() {
        final Cluster cluster = Cluster.build("localhost").compression(Compression.DEFLATE).create();
        assertEquals(Compression.DEFLATE, cluster.getCompression());
        cluster.close();
    }

    @Test
    public void shouldDefaultCompressionToNoneWhenNullSupplied() {
        final Cluster cluster = Cluster.build("localhost").compression((Compression) null).create();
        assertEquals(Compression.NONE, cluster.getCompression());
        cluster.close();
    }

    @Test
    public void shouldConfigureProxyWithoutCredentials() {
        final ProxyOptions proxy = ProxyOptions.create("proxy.example.com", 3128);
        final Cluster cluster = Cluster.build("localhost").proxy(proxy).create();
        assertSame(proxy, cluster.getProxy());
        assertFalse(proxy.hasCredentials());
        cluster.close();
    }

    @Test
    public void shouldConfigureProxyWithCredentials() {
        final ProxyOptions proxy = ProxyOptions.create("proxy.example.com", 3128, "user", "pass");
        final Cluster cluster = Cluster.build("localhost").proxy(proxy).create();
        assertSame(proxy, cluster.getProxy());
        assertTrue(proxy.hasCredentials());
        assertEquals("user", proxy.getUsername());
        assertEquals("pass", proxy.getPassword());
        cluster.close();
    }

    @Test
    public void shouldEnableSslWhenSslContextSupplied() throws Exception {
        final SslContext sslContext = SslContextBuilder.forClient().build();
        final Cluster cluster = Cluster.build("localhost").ssl(sslContext).create();
        assertTrue(cluster.isSslEnabled());
        assertNotNull(cluster.createSSLContext());
        assertSame(sslContext, cluster.createSSLContext());
        cluster.close();
    }

    @Test
    public void shouldDefaultValidationRequestToInject() {
        final Cluster cluster = Cluster.build("localhost").create();
        assertEquals("g.inject(0)", cluster.validationRequest().create().getGremlin());
        cluster.close();
    }

    @Test
    public void shouldSetTraversalSource() {
        assertEquals("gmodern", RequestOptions.build().traversalSource("gmodern").create().getG().get());
    }

    @Test
    public void shouldSetKeepAliveTime() {
        final Cluster cluster = Cluster.build("localhost").keepAliveTimeMillis(45000).create();
        assertEquals(45000L, cluster.getKeepAliveTime());
        cluster.close();
    }

    @Test
    public void shouldDisableKeepAliveTimeWithZero() {
        final Cluster cluster = Cluster.build("localhost").keepAliveTimeMillis(0).create();
        assertEquals(0L, cluster.getKeepAliveTime());
        cluster.close();
    }

    @Test
    public void shouldConfigureSoKeepAliveOnBootstrapWhenKeepAliveTimePositive() {
        final Bootstrap b = new Bootstrap();
        Connection.configureKeepAlive(b, 30000);
        assertEquals(Boolean.TRUE, b.config().options().get(ChannelOption.SO_KEEPALIVE));
    }

    @Test
    public void shouldNotConfigureSoKeepAliveOnBootstrapWhenKeepAliveTimeZero() {
        final Bootstrap b = new Bootstrap();
        Connection.configureKeepAlive(b, 0);
        assertNull(b.config().options().get(ChannelOption.SO_KEEPALIVE));
    }

    @Test
    public void shouldConfigureConnectTimeoutOnBootstrap() {
        final Bootstrap b = new Bootstrap();
        Connection.configureConnectTimeout(b, 2500);
        assertEquals(Integer.valueOf(2500), b.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS));
    }

    @Test
    public void shouldConfigureEndpointFromHttpUrl() {
        final Cluster cluster = Cluster.build().url("http://localhost:8183/gremlin").create();
        assertFalse(cluster.isSslEnabled());
        assertEquals(8183, cluster.getPort());
        assertEquals("/gremlin", cluster.getPath());
        assertTrue(cluster.toString().contains("localhost"));
        cluster.close();
    }

    @Test
    public void shouldConfigureEndpointFromHttpsUrl() {
        final Cluster cluster = Cluster.build().url("https://localhost:8182/mygremlin").create();
        assertTrue(cluster.isSslEnabled());
        assertEquals(8182, cluster.getPort());
        assertEquals("/mygremlin", cluster.getPath());
        cluster.close();
    }

    @Test
    public void shouldConfigureContactPointAndDefaultsFromUrlWithoutPortOrPath() {
        final Cluster cluster = Cluster.build().url("http://localhost").create();
        assertFalse(cluster.isSslEnabled());
        // port and path fall back to their defaults when not present in the url
        assertEquals(8182, cluster.getPort());
        assertEquals("/gremlin", cluster.getPath());
        assertTrue(cluster.toString().contains("localhost"));
        cluster.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUrlWithUnsupportedScheme() {
        Cluster.build().url("ws://localhost:8182/gremlin");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUrlWhenContactPointAlreadyAdded() {
        Cluster.build().addContactPoint("localhost").url("http://localhost:8182/gremlin");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUrlWhenContactPointsAlreadyAdded() {
        Cluster.build().addContactPoints("host1", "host2").url("http://localhost:8182/gremlin");
    }
}
