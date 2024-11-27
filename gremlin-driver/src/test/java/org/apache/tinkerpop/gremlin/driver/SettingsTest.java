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
package org.apache.tinkerpop.gremlin.driver;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SettingsTest {

    @Test
    public void shouldCreateFromConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty("port", 8000);
        conf.setProperty("nioPoolSize", 16);
        conf.setProperty("workerPoolSize", 32);
        conf.setProperty("auth.type", "basic");
        conf.setProperty("auth.username", "user1");
        conf.setProperty("auth.password", "password1");
        conf.setProperty("hosts", Arrays.asList("255.0.0.1", "255.0.0.2", "255.0.0.3"));
        conf.setProperty("serializer.className", "my.serializers.MySerializer");
        conf.setProperty("serializer.config.any", "thing");
        conf.setProperty("enableUserAgentOnConnect", false);
        conf.setProperty("bulkResults", true);
        conf.setProperty("connectionPool.enableSsl", true);
        conf.setProperty("connectionPool.keyStore", "server.jks");
        conf.setProperty("connectionPool.keyStorePassword", "password2");
        conf.setProperty("connectionPool.keyStoreType", "pkcs12");
        conf.setProperty("connectionPool.trustStore", "trust.jks");
        conf.setProperty("connectionPool.trustStorePassword", "password3");
        conf.setProperty("connectionPool.trustStoreType", "jks");
        conf.setProperty("connectionPool.sslEnabledProtocols", Arrays.asList("TLSv1.1","TLSv1.2"));
        conf.setProperty("connectionPool.sslCipherSuites", Arrays.asList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"));
        conf.setProperty("connectionPool.sslSkipCertValidation", true);
        conf.setProperty("connectionPool.maxSize", 200);
        conf.setProperty("connectionPool.minSimultaneousUsagePerConnection", 300);
        conf.setProperty("connectionPool.maxSimultaneousUsagePerConnection", 400);
        conf.setProperty("connectionPool.maxInProcessPerConnection", 500);
        conf.setProperty("connectionPool.minInProcessPerConnection", 600);
        conf.setProperty("connectionPool.maxWaitForConnection", 700);
        conf.setProperty("connectionPool.maxResponseContentLength", 800);
        conf.setProperty("connectionPool.reconnectInterval", 900);
        conf.setProperty("connectionPool.resultIterationBatchSize", 1100);
        conf.setProperty("connectionPool.channelizer", "channelizer0");
        conf.setProperty("connectionPool.validationRequest", "g.inject()");
        conf.setProperty("connectionPool.connectionSetupTimeoutMillis", 15000);
        conf.setProperty("connectionPool.idleConnectionTimeout", 160000);

        final Settings settings = Settings.from(conf);

        assertEquals(8000, settings.port);
        assertEquals(16, settings.nioPoolSize);
        assertEquals(32, settings.workerPoolSize);
        assertEquals("basic", settings.auth.type);
        assertEquals("user1", settings.auth.username);
        assertEquals("password1", settings.auth.password);
        assertEquals(Arrays.asList("255.0.0.1", "255.0.0.2", "255.0.0.3"), settings.hosts);
        assertEquals("my.serializers.MySerializer", settings.serializer.className);
        assertEquals("thing", settings.serializer.config.get("any"));
        assertEquals(false, settings.enableUserAgentOnConnect);
        assertTrue(settings.bulkResults);
        assertThat(settings.connectionPool.enableSsl, is(true));
        assertEquals("server.jks", settings.connectionPool.keyStore);
        assertEquals("password2", settings.connectionPool.keyStorePassword);
        assertEquals("pkcs12", settings.connectionPool.keyStoreType);
        assertEquals("trust.jks", settings.connectionPool.trustStore);
        assertEquals("password3", settings.connectionPool.trustStorePassword);
        assertEquals("jks", settings.connectionPool.trustStoreType);
        assertEquals(Arrays.asList("TLSv1.1","TLSv1.2"), settings.connectionPool.sslEnabledProtocols);
        assertEquals(Arrays.asList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"), settings.connectionPool.sslCipherSuites);
        assertThat(settings.connectionPool.sslSkipCertValidation, is(true));
        assertEquals(200, settings.connectionPool.maxSize);
        assertEquals(700, settings.connectionPool.maxWaitForConnection);
        assertEquals(800, settings.connectionPool.maxResponseContentLength);
        assertEquals(900, settings.connectionPool.reconnectInterval);
        assertEquals(15000, settings.connectionPool.connectionSetupTimeoutMillis);
        assertEquals(160000, settings.connectionPool.idleConnectionTimeout);
        assertEquals(1100, settings.connectionPool.resultIterationBatchSize);
        assertEquals("g.inject()", settings.connectionPool.validationRequest);
    }
}
