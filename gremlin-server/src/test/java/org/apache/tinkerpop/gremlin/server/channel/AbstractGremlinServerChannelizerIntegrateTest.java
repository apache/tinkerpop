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
package org.apache.tinkerpop.gremlin.server.channel;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.tinkerpop.gremlin.driver.AuthProperties;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;

import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.tinkerpop.gremlin.driver.AuthProperties.Property;

abstract class AbstractGremlinServerChannelizerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Base64.Encoder encoder = Base64.getUrlEncoder();

    protected static final String HTTP = "http";
    protected static final String WS = "ws";
    protected static final String HTTPS = "https";
    protected static final String WSS = "wss";
    protected static final String WS_AND_HTTP = "wsAndHttp";
    protected static final String WSS_AND_HTTPS = "wssAndHttps";

    public abstract String getProtocol();
    public abstract String getSecureProtocol();
    public abstract String getChannelizer();
    public abstract Settings.AuthenticationSettings getAuthSettings();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = getChannelizer();
        final String nameOfTest = name.getMethodName();
        Settings.AuthenticationSettings authSettings = getAuthSettings();
        switch (nameOfTest) {
            case "shouldReturnResult":
                break;
            case "shouldWorkWithSSL":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                break;
            case "shouldWorkWithAuth":
                if (authSettings != null) {
                    settings.authentication = getAuthSettings();
                }
                break;
            case "shouldWorkWithSSLAndAuth":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                if (authSettings != null) {
                    settings.authentication = getAuthSettings();
                }
                break;
        }
        return settings;
    }

    @Test
    public void shouldReturnResult() throws Exception {
        final CombinedTestClient client =  new CombinedTestClient(getProtocol());
        try {
            client.sendAndAssert("2+2", 4);
        } finally {
            client.close();
        }
    }

    @Test
    public void shouldWorkWithSSL() throws Exception {
        final CombinedTestClient client =  new CombinedTestClient(getSecureProtocol());
        try {
            client.sendAndAssert("2+2", 4);
        } finally {
            client.close();
        }
    }

    @Test
    public void shouldWorkWithAuth() throws Exception {
        CombinedTestClient client =  new CombinedTestClient(getProtocol());
        try {
            client.sendAndAssertUnauthorized("2+2", "stephen", "notpassword");
        } finally {
            client.close();
        }

        client = new CombinedTestClient(getProtocol());
        try {
            client.sendAndAssert("2+2", 4, "stephen", "password");
        } finally {
            client.close();
        }

        client = new CombinedTestClient(getProtocol());
        try {
            // Expect exception when try again if the server pipeline is correct
            client.sendAndAssertUnauthorized("2+2", "stephen", "notpassword");
        } finally {
            client.close();
        }
    }

    @Test
    public void shouldWorkWithSSLAndAuth() throws Exception {
        CombinedTestClient client =  new CombinedTestClient(getSecureProtocol());
        try {
            client.sendAndAssertUnauthorized("2+2", "stephen", "incorrect-password");
        } finally {
            client.close();
        }

        client = new CombinedTestClient(getSecureProtocol());
        try {
            client.sendAndAssert("2+2", 4, "stephen", "password");
        } finally {
            client.close();
        }

        client = new CombinedTestClient(getSecureProtocol());
        try {
            // Expect exception when try again if the server pipeline is correct
            client.sendAndAssertUnauthorized("2+2", "stephen", "incorrect-password");
        } finally {
            client.close();
        }
    }

    public class CombinedTestClient {
        private CloseableHttpClient httpClient = null;
        private Cluster wsCluster = null;
        private Cluster.Builder wsBuilder = null;
        private Client wsClient = null;
        private boolean secure = false;


        public CombinedTestClient(final String protocol) throws Exception {
            switch (protocol) {
                case HTTP:
                    httpClient = HttpClients.createDefault();
                    break;
                case HTTPS:
                    httpClient = createSslHttpClient();
                    secure = true;
                    break;
                case WS:
                    this.wsBuilder = TestClientFactory.build();
                    break;
                case WSS:
                    this.wsBuilder = TestClientFactory.build();
                    secure = true;
                    break;
                case WS_AND_HTTP:
                    httpClient = HttpClients.createDefault();
                    this.wsBuilder = TestClientFactory.build();
                    break;
                case WSS_AND_HTTPS:
                    httpClient = createSslHttpClient();
                    secure = true;
                    this.wsBuilder = TestClientFactory.build();
                    break;
            }
        }

        private CloseableHttpClient createSslHttpClient() throws Exception {
            final SSLContextBuilder wsBuilder = new SSLContextBuilder();
            wsBuilder.loadTrustMaterial(null, (chain, authType) -> true);
            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(wsBuilder.build(),
                new NoopHostnameVerifier());
            //This winds up using a PoolingHttpClientConnectionManager so need to pass the
            //RegistryBuilder
            final Registry<ConnectionSocketFactory> registry = RegistryBuilder
                .<ConnectionSocketFactory> create().register("https", sslsf)
                .build();
            final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
            return HttpClients
                .custom()
                .setConnectionManager(cm)
                .build();

        }

        public void sendAndAssert(final String gremlin, Object result) throws Exception {
            sendAndAssert(gremlin, result, null, null);
        }

        public void close() throws IOException {
            if (httpClient != null) {
                httpClient.close();
            }
            if (wsCluster != null) {
                wsCluster.close();
            }
        }

        public void sendAndAssertUnauthorized(final String gremlin, final String username, final String password) throws Exception {
            if (httpClient != null) {
                final HttpPost httpPost = createPost(gremlin, username, password);
                try (final CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    assertEquals(401, response.getStatusLine().getStatusCode());
                }
            }
            if (wsBuilder != null) {
                setWsClient(username, password);
                try {
                    wsClient.submit(gremlin).all().get();
                    fail("Should not authorize on incorrect auth creds");
                } catch(Exception e) {
                    assertEquals("Username and/or password are incorrect", e.getCause().getMessage());
                }
            }
        }

        public void sendAndAssert(final String gremlin, final Object result, final String username, final String password) throws Exception {
            if (httpClient != null) {
                final HttpPost httpPost = createPost(gremlin, username, password);
                try (final CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    assertEquals(200, response.getStatusLine().getStatusCode());
                    assertEquals("application/json", response.getEntity().getContentType().getValue());
                    final String json = EntityUtils.toString(response.getEntity());
                    final JsonNode node = mapper.readTree(json);
                    assertEquals(result, node.get("result").get("data").get("@value").get(0).get("@value").intValue());
                }
            }
            if (wsBuilder != null) {
                setWsClient(username, password);
                assertEquals(result, wsClient.submit(gremlin).all().get().get(0).getInt());
            }
        }

        private void setWsClient(final String username, final String password) {
            if (username != null && password != null) {
                final AuthProperties authProps = new AuthProperties()
                                                .with(Property.USERNAME, username)
                                                .with(Property.PASSWORD, password);

                wsCluster = wsBuilder.enableSsl(secure).sslSkipCertValidation(true).authProperties(authProps).create();
                wsClient = wsCluster.connect();
            } else {
                wsCluster = wsBuilder.enableSsl(secure).sslSkipCertValidation(true).create();
                wsClient = wsCluster.connect();
            }
        }

        private HttpPost createPost(final String gremlin, final String username, final String password) {
            String urlString = TestClientFactory.createURLString();
            if (secure) {
                urlString = urlString.replace("http", "https");
            }
            final HttpPost httpPost = new HttpPost(urlString);
            httpPost.addHeader("Content-Type", "application/json");
            if (username != null && password != null) {
                final String auth = encoder.encodeToString((username + ":" + password).getBytes());
                httpPost.addHeader("Authorization", "Basic " + auth);
            }
            final String jsonBody = String.format("{\"gremlin\": \"%s\"}", gremlin);
            httpPost.setEntity(new StringEntity(jsonBody, Consts.UTF_8));
            return httpPost;
        }
    }
}

