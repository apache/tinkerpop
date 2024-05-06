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
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.ser.SerializersV4;
import org.ietf.jgss.GSSException;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.driver.Auth.basic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        authSettings.authenticator = SimpleAuthenticator.class.getName();

        // use a credentials graph with two users in it: stephen/password and marko/rainbow-dash
        final Map<String, Object> authConfig = new HashMap<>();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");

        authSettings.config = authConfig;
        settings.authentication = authSettings;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuthenticateOverSslWithPlainText":
            case "shouldFailIfSslEnabledOnServerButNotClient":
                final Settings.SslSettings sslConfig = new Settings.SslSettings();
                sslConfig.enabled = true;
                sslConfig.keyStore = JKS_SERVER_KEY;
                sslConfig.keyStorePassword = KEY_PASS;
                settings.ssl = sslConfig;
                break;
        }

        return settings;
    }

    @Test
    public void shouldFailIfSslEnabledOnServerButNotClient() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client did not enable SSL");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(RuntimeException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthenticateWithPlainText() throws Exception {
        final Cluster cluster = TestClientFactory.build().auth(basic("stephen", "password")).create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateOverSslWithPlainText() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .enableSsl(true).sslSkipCertValidation(true)
                .auth(basic("stephen", "password")).create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextNoCredentials() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client did not provide credentials");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);

            // depending on the configuration of the system environment you might get either of these
            assertThat(root, anyOf(instanceOf(GSSException.class), instanceOf(ResponseException.class)));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadPassword() {
        final Cluster cluster = TestClientFactory.build().auth(basic("stephen", "bad")).create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client did not provide valid credentials");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadUsername() throws Exception {
        final Cluster cluster = TestClientFactory.build().auth(basic("marko", "password")).create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthenticateWithPlainTextOverDefaultJSONSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(SerializersV4.GRAPHSON)
                .auth(basic("stephen", "password")).create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateWithPlainTextOverGraphSONV4Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(SerializersV4.GRAPHSON_V4_UNTYPED)
                .auth(basic("stephen", "password")).create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateAndWorkWithVariablesOverDefaultJsonSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(SerializersV4.GRAPHSON)
                .auth(basic("stephen", "password")).create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            final Vertex vertex = (Vertex) client.submit("v=graph.addVertex(\"name\", \"stephen\")").all().get().get(0).getObject();
            assertEquals("stephen", vertex.value("name"));

            final Property vpName = (Property) client.submit("v.property('name')").all().get().get(0).getObject();
            assertEquals("stephen", vpName.value());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthenticateAndWorkWithVariablesOverGraphSONV4Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(SerializersV4.GRAPHSON_V4_UNTYPED)
                .auth(basic("stephen", "password")).create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            final Map vertex = (Map) client.submit("v=graph.addVertex('name', 'stephen')").all().get().get(0).getObject();
            final Map<String, List<Map>> properties = (Map) vertex.get("properties");
            assertEquals("stephen", properties.get("name").get(0).get("value"));

            final Map vpName = (Map) client.submit("v.property('name')").all().get().get(0).getObject();
            assertEquals("stephen", vpName.get("value"));
        } finally {
            cluster.close();
        }
    }

    private static void assertConnection(final Cluster cluster, final Client client) throws InterruptedException, ExecutionException {
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }
}
