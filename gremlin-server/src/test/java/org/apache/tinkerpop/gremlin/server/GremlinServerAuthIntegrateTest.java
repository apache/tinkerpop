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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerAuthIntegrateTest extends AbstractGremlinServerIntegrationTest {

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.className = SimpleAuthenticator.class.getName();

        // use a credentials graph with one user in it: stephen/password
        final Map<String,Object> authConfig = new HashMap<>();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-empty.properties");
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_LOCATION, "data/credentials.kryo");

        authSettings.config = authConfig;
        settings.authentication = authSettings;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuthenticateOverSslWithPlainText":
                final Settings.SslSettings sslConfig = new Settings.SslSettings();
                sslConfig.enabled = true;
                settings.ssl = sslConfig;
                break;
        }

        return settings;
    }

    @Test
    public void shouldAuthenticateWithPlainText() throws Exception {
        final Cluster cluster = Cluster.build().credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthenticateOverSslWithPlainText() throws Exception {
        final Cluster cluster = Cluster.build()
                .enableSsl(true)
                .credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextNoCredentials() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all();
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadPassword() throws Exception {
        final Cluster cluster = Cluster.build().credentials("stephen", "bad").create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all();
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadUsername() throws Exception {
        final Cluster cluster = Cluster.build().credentials("marko", "password").create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all();
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }
    
    @Test
    public void shouldAuthenticateWithPlainTextOverJSONSerialization() throws Exception {
        final Cluster cluster = Cluster.build().serializer(Serializers.GRAPHSON).credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }
}
