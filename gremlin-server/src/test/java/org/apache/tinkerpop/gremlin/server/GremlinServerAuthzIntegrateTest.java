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

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.log4j.Level.INFO;
import static org.apache.tinkerpop.gremlin.server.GremlinServer.AUDIT_LOGGER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Run with:
 * mvn verify -Dit.test=GremlinServerAuthzIntegrateTest -pl gremlin-server -DskipTests -DskipIntegrationTests=false -Dmaven.javadoc.skip=true -Dorg.slf4j.simpleLogger.defaultLogLevel=info
 *
 * @author Marc de Lignie
 */
public class GremlinServerAuthzIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Log4jRecordingAppender recordingAppender;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Base64.Encoder encoder = Base64.getUrlEncoder();

    @Override
    public void setUp() throws Exception {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.addAppender(recordingAppender);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.removeAppender(recordingAppender);
        super.tearDown();
    }

    /**
     * Configure Gremlin Server settings per test.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final Settings.SslSettings sslConfig = new Settings.SslSettings();
        sslConfig.enabled = false;

        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.config = new HashMap<>();
        authSettings.authenticator = SimpleAuthenticator.class.getName();
        authSettings.config.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");

        final Settings.AuthorizationSettings authzSettings = new Settings.AuthorizationSettings();
        authzSettings.authorizer = AllowListAuthorizer.class.getName();
        authzSettings.config = new HashMap<>();
        final String yamlName = "org/apache/tinkerpop/gremlin/server/allow-list.yaml";
        final String yamlHttpName = "org/apache/tinkerpop/gremlin/server/allow-list-http-anonymous.yaml";
        final String file = Objects.requireNonNull(getClass().getClassLoader().getResource(yamlName)).getFile();
        authzSettings.config.put(AllowListAuthorizer.KEY_AUTHORIZATION_ALLOWLIST, file);

        settings.ssl = sslConfig;
        settings.authentication = authSettings;
        settings.authorization = authzSettings;
        settings.enableAuditLog = true;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldFailBytecodeRequestWithAllowAllAuthenticator":
            case "shouldFailStringRequestWithAllowAllAuthenticator":
                authSettings.authenticator = AllowAllAuthenticator.class.getName();
                break;
            case "shouldAuthorizeWithHttpTransport":
            case "shouldFailAuthorizeWithHttpTransport":
            case "shouldKeepAuthorizingWithHttpTransport":
                settings.channelizer = HttpChannelizer.class.getName();
                break;
            case "shouldAuthorizeWithAllowAllAuthenticatorAndHttpTransport":
                settings.channelizer = HttpChannelizer.class.getName();
                authSettings.authenticator = AllowAllAuthenticator.class.getName();
                authSettings.config = null;
                final String fileHttp = Objects.requireNonNull(getClass().getClassLoader().getResource(yamlHttpName)).getFile();
                authzSettings.config.put(AllowListAuthorizer.KEY_AUTHORIZATION_ALLOWLIST, fileHttp);
                break;
        }
        return settings;
    }

    @Test
    public void shouldAuthorizeBytecodeRequest() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gmodern"));

        try {
            assertEquals(6, (long) g.V().count().next());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeBytecodeRequestWithLambda() {
        final Cluster cluster = TestClientFactory.build().credentials("marko", "rainbow-dash").create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gclassic"));

        try {
            assertEquals(6, (long) g.V().count().next());
            assertEquals(6, (long) g.V().map(Lambda.function("it.get().value('name')")).count().next());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailBytecodeRequestWithLambda() throws Exception{
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gmodern"));

        try {
            g.V().map(Lambda.function("it.get().value('name')")).count().next();
            fail("Authorization for bytecode request with lambda should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for bytecode requests on [gmodern] using lambdas.", re.getMessage());

            // wait for logger to flush - (don't think there is a way to detect this)
            stopServer();
            Thread.sleep(1000);

            assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO,
                    "User stephen with address .+? attempted an unauthorized request for bytecode operation: " +
                            "\\[\\[], \\[V\\(\\), map\\(lambda\\[it.get\\(\\).value\\('name'\\)]\\), count\\(\\)]]"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldKeepAuthorizingBytecodeRequests() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gmodern"));

        try {
            assertEquals(6, (long) g.V().count().next());
            try {
                g.V().map(Lambda.function("it.get().value('name')")).count().next();
                fail("Authorization for bytecode request with lambda should fail");
            } catch (Exception ex) {
                final ResponseException re = (ResponseException) ex.getCause();
                assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
                assertEquals("Failed to authorize: User not authorized for bytecode requests on [gmodern] using lambdas.", re.getMessage());
            }
            assertEquals(6, (long) g.V().count().next());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeStringRequest() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("marko", "rainbow-dash").create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(6, client.submit("gclassic.V().count()").all().get().get(0).getInt());
            assertEquals(6, client.submit("gmodern.V().map{it.get().value('name')}.count()").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailStringRequestWithGroovyScript() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for string-based requests.", re.getMessage());

            // wait for logger to flush - (don't think there is a way to detect this)
            stopServer();
            Thread.sleep(1000);

            assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO,
                    "User stephen with address .+? attempted an unauthorized request for eval operation: 1\\+1"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailStringRequestWithGremlinTraversal() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            client.submit("gmodern.V().count()").all().get();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for string-based requests.", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeSessionedStringRequest() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("marko", "rainbow-dash").create();
        final Client client = cluster.connect("session1");

        try {
            assertEquals(2, client.submit("a = 4; 1+1").all().get().get(0).getInt());
            assertEquals(10, client.submit("gclassic.V().count().next() + a").all().get().get(0).getInt());
            assertEquals(6, client.submit("gmodern.V().map{it.get().value('name')}.count()").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailBytecodeRequestWithAllowAllAuthenticator() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                    DriverRemoteConnection.using(cluster, "gclassic"));
            g.V().count().next();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for bytecode requests on [gclassic].", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailStringRequestWithAllowAllAuthenticator() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for string-based requests.", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeWithHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("marko:rainbow-dash".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void shouldFailAuthorizeWithHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=3-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO,
                "User stephen with address .+? attempted an unauthorized http request: 3-1"));
    }

    @Test
    public void shouldKeepAuthorizingWithHttpTransport() throws Exception {
        HttpGet httpget;
        final CloseableHttpClient httpclient = HttpClients.createDefault();

        httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=4-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("marko:rainbow-dash".getBytes()));
        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(3, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }

        httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=5-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));
        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }

        httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=6-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("marko:rainbow-dash".getBytes()));
        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(5, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void shouldAuthorizeWithAllowAllAuthenticatorAndHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=7-1"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }
}
