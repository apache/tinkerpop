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
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.ietf.jgss.GSSException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import static org.apache.tinkerpop.gremlin.driver.auth.Auth.basic;
import static org.apache.tinkerpop.gremlin.driver.auth.Auth.sigv4;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            case "shouldPassSigv4ToServer":
                settings.authentication = new Settings.AuthenticationSettings();
                break;
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
    public void shouldPassSigv4ToServer() throws Exception {
        final AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        final AwsSessionCredentials credentials = AwsSessionCredentials.create("I am AWSAccessKeyId", "I am AWSSecretKey", "I am AWSSessionToken");
        when(credentialsProvider.resolveCredentials()).thenReturn(credentials);

        final AtomicReference<HttpRequest> httpRequest = new AtomicReference<>();
        final Cluster cluster = TestClientFactory.build()
                .auth(sigv4("us-west2", credentialsProvider, "service-name"))
                .addInterceptor("header-checker", r -> {
                    httpRequest.set(r);
                    return r;
                })
                .create();
        final Client client = cluster.connect();
        client.submit("g.inject(2)").all().get();

        Map<String, String> headers = httpRequest.get().headers();
        assertNotNull(headers.get("X-Amz-Date"));
        assertThat(headers.get("Authorization"), startsWith("AWS4-HMAC-SHA256 Credential=I am AWSAccessKeyId"));
        assertThat(headers.get("Authorization"),
                allOf(containsString("/us-west2/service-name/aws4_request"), containsString("Signature=")));

        cluster.close();
    }

    @Test
    public void shouldFailIfSslEnabledOnServerButNotClient() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(2)").all().get();
            fail("This should not succeed as the client did not enable SSL");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(ExecutionException.class));
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("The server may be expecting SSL to be enabled"));
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

        client.submit("g.inject(2)").all().get();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextNoCredentials() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(2)").all().get();
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
            client.submit("g.inject(2)").all().get();
            fail("This should not succeed as the client did not provide valid credentials");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            // server do not send error message now
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadUsername() {
        final Cluster cluster = TestClientFactory.build().auth(basic("marko", "password")).create();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(2)").all().get();
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    private static void assertConnection(final Cluster cluster, final Client client) throws InterruptedException, ExecutionException {
        try {
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
            assertEquals(3, client.submit("g.inject(3)").all().get().get(0).getInt());
            assertEquals(4, client.submit("g.inject(4)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }
}
