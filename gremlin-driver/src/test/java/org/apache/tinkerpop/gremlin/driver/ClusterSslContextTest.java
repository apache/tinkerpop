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

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import org.junit.Test;

import javax.net.ssl.SSLEngine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClusterSslContextTest {

    /**
     * Enabling hostname verification must configure the SSL engine with the {@code HTTPS} endpoint identification
     * algorithm, which is what makes the JDK fail the handshake on a certificate/host mismatch.
     */
    @Test
    public void shouldEnableHostnameVerificationWhenConfigured() throws Exception {
        final Cluster cluster = Cluster.build("localhost").enableSsl(true).sslHostnameVerification(true).create();
        try {
            final SslContext sslContext = cluster.createSSLContext();
            final SSLEngine engine = sslContext.newEngine(ByteBufAllocator.DEFAULT, "localhost", 8182);
            assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
        } finally {
            cluster.close();
        }
    }

    /**
     * By default the SSL engine is configured without an endpoint identification algorithm, so the server
     * certificate's hostname is not checked.
     */
    @Test
    public void shouldNotEnableHostnameVerificationByDefault() throws Exception {
        final Cluster cluster = Cluster.build("localhost").enableSsl(true).create();
        try {
            final SslContext sslContext = cluster.createSSLContext();
            final SSLEngine engine = sslContext.newEngine(ByteBufAllocator.DEFAULT, "localhost", 8182);
            assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
        } finally {
            cluster.close();
        }
    }
}
