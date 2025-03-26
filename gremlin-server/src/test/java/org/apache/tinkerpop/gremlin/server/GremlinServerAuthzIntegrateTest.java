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

import nl.altindag.log.LogCaptor;
import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthenticationHandler;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.tinkerpop.gremlin.driver.auth.Auth.basic;
import static org.junit.Assert.assertEquals;

/**
 * Run with:
 * mvn verify -Dit.test=GremlinServerAuthzIntegrateTest -pl gremlin-server -DskipTests -DskipIntegrationTests=false -Dmaven.javadoc.skip=true -Dorg.slf4j.simpleLogger.defaultLogLevel=info
 *
 * @author Marc de Lignie
 */
public class GremlinServerAuthzIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static LogCaptor logCaptor;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Base64.Encoder encoder = Base64.getUrlEncoder();

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forRoot();
    }

    @Before
    public void resetLogs() {
        logCaptor.clearLogs();
    }

    @AfterClass
    public static void tearDownClass() {
        logCaptor.close();
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
        if (nameOfTest.equals("shouldAuthorizeWithHttpTransport")) {
            authSettings.authenticationHandler = HttpBasicAuthenticationHandler.class.getName();
        }
        return settings;
    }

    @Test
    public void shouldAuthorizeGremlinLangRequest() {
        final Cluster cluster = TestClientFactory.build().auth(basic("marko", "rainbow-dash")).create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().with(
                DriverRemoteConnection.using(cluster, "gmodern"));
        try {
            assertEquals(6, (long) g.V().count().next());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeStringRequest() throws Exception {
        final Cluster cluster = TestClientFactory.build().auth(basic("marko", "rainbow-dash")).create();
        final Client client = cluster.connect();

        RequestOptions modern = RequestOptions.build().addG("gmodern").create();
        RequestOptions classic = RequestOptions.build().addG("gclassic").create();

        try {
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
            assertEquals(6, client.submit("g.V().count()", classic).all().get().get(0).getInt());
            assertEquals(6, client.submit("g.V().values('name').count()", modern).all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeWithHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httpPost = new HttpPost(TestClientFactory.createURLString());
        httpPost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(1)\"}", Consts.UTF_8));
        httpPost.addHeader("Authorization", "Basic " + encoder.encodeToString("marko:rainbow-dash".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get(SerTokens.TOKEN_DATA).get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }
}
