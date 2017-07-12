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

import org.apache.tinkerpop.gremlin.driver.AuthProperties;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;


import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

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
    protected static final String NIO = "nio";
    protected static final String NIO_SECURE = "nioSecure";

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
                break;
            case "shouldWorkWithAuth":
                if (authSettings != null) {
                    settings.authentication = getAuthSettings();
                }
                break;
            case "shouldWorkWithSSLAndAuth":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
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
            client.close();
            client = new CombinedTestClient(getProtocol());
            client.sendAndAssert("2+2", 4, "stephen", "password");
            client.close();
        } catch (Exception e) {
            client.close();
            throw e;
        }
    }

    @Test
    public void shouldWorkWithSSLAndAuth() throws Exception {
        CombinedTestClient client =  new CombinedTestClient(getSecureProtocol());
        try {
            client.sendAndAssertUnauthorized("2+2", "stephen", "incorrect-password");
            client.close();
            client = new CombinedTestClient(getSecureProtocol());
            client.sendAndAssert("2+2", 4, "stephen", "password");
            client.close();
        } catch (Exception e) {
            client.close();
            throw e;
        }
    }

    public class CombinedTestClient {
        private CloseableHttpClient httpClient = null;
        private Cluster wsCluster = null;
        private Cluster.Builder wsBuilder = null;
        private Cluster nioCluster = null;
        private Cluster.Builder nioBuilder = null;
        private Client wsClient = null;
        private Client.ClusteredClient nioClient = null;
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
                case NIO:
                    this.nioBuilder = TestClientFactory.build();
                    break;
                case NIO_SECURE:
                    this.nioBuilder = TestClientFactory.build();
                    secure = true;
                    break;
            }
        }

        private CloseableHttpClient createSslHttpClient() throws Exception {
            final SSLContextBuilder wsBuilder = new SSLContextBuilder();
            wsBuilder.loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] chain,
                    String authType) throws CertificateException {
                    return true;
                }
            });
            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(wsBuilder.build(),
                new AllowAllHostnameVerifier());
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

        public void close() {
            if (wsCluster != null) {
                wsCluster.close();
            }
            if (nioCluster != null) {
                nioCluster.close();
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
            if (nioBuilder != null) {
                setNioClient(username, password);
                try {
                    nioClient.submit(gremlin);
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
                    assertEquals(result, node.get("result").get("data").get(0).get("@value").intValue());
                }
            }
            if (wsBuilder != null) {
                setWsClient(username, password);
                assertEquals(result, wsClient.submit(gremlin).all().get().get(0).getInt());
            }
            if (nioClient != null) {
                assertEquals(result, nioClient.submit(gremlin).all().get().get(0).getInt());
            }
        }

        private void setNioClient(final String username, final String password) {
            nioBuilder.channelizer(Channelizer.NioChannelizer.class.getName());
            if (username != null && password != null) {
                final AuthProperties authProps = new AuthProperties()
                                                .with(Property.USERNAME, username)
                                                .with(Property.PASSWORD, password);

                nioCluster = nioBuilder.enableSsl(secure).authProperties(authProps).create();
                nioClient = nioCluster.connect();
            } else {
                nioCluster = nioBuilder.enableSsl(secure).create();
                nioClient = nioCluster.connect();
            }
        }

        private void setWsClient(final String username, final String password) {
            if (username != null && password != null) {
                final AuthProperties authProps = new AuthProperties()
                                                .with(Property.USERNAME, username)
                                                .with(Property.PASSWORD, password);

                wsCluster = wsBuilder.enableSsl(secure).authProperties(authProps).create();
                wsClient = wsCluster.connect();
            } else {
                wsCluster = wsBuilder.enableSsl(secure).create();
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

