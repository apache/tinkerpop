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
package org.apache.tinkerpop.gremlin.server;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GremlinServerSslIntegrateTest extends AbstractGremlinServerIntegrationTest {

    // Holds the server's self-signed certificate (CN=example.com) for the hostname-verification test so the client
    // can trust it while still connecting to "localhost" (a deliberate hostname mismatch).
    private SelfSignedCertificate hostnameMismatchCert;

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldEnableSsl":
            case "shouldEnableWebSocketSsl":
            case "shouldEnableSslButFailIfClientConnectsWithoutIt":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                break;
            case "shouldEnableSslWithSslContextProgrammaticallySpecified":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.overrideSslContext(createServerSslContext());
                break;
            case "shouldFailHandshakeOnHostnameMismatchOnlyWhenVerificationEnabled":
                try {
                    hostnameMismatchCert = new SelfSignedCertificate("example.com");
                } catch (Exception ex) {
                    throw new RuntimeException("Couldn't set up self-signed certificate for test", ex);
                }
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.overrideSslContext(sslContextForCert(hostnameMismatchCert));
                break;
            case "shouldEnableSslAndClientCertificateAuthWithPkcs12":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyStore = P12_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_PKCS12;
                settings.ssl.trustStore = P12_SERVER_TRUST;
                settings.ssl.trustStorePassword = KEY_PASS;
                break;
            case "shouldEnableSslAndClientCertificateAuth":
            case "shouldEnableSslAndClientCertificateAuthAndFailWithoutCert":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                settings.ssl.trustStore = JKS_SERVER_TRUST;
                settings.ssl.trustStorePassword = KEY_PASS;
                break;
            case "shouldEnableSslAndClientCertificateAuthAndFailWithoutTrustedClientCert":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                break;
            case "shouldEnableSslAndFailIfProtocolsDontMatch":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                settings.ssl.sslEnabledProtocols = Collections.singletonList("TLSv1.2");
                break;
            case "shouldEnableSslAndFailIfCiphersDontMatch":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                settings.ssl.sslCipherSuites = Collections.singletonList("TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
                break;
            case "shouldEnableSslAndClientCertificateAuthWithDifferentStoreType":
            case "shouldEnableSslAndClientCertificateAuthAndFailWithIncorrectKeyStoreType":
            case "shouldEnableSslAndClientCertificateAuthAndFailWithIncorrectTrustStoreType":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyStore = JKS_SERVER_KEY;
                settings.ssl.keyStorePassword = KEY_PASS;
                settings.ssl.keyStoreType = KEYSTORE_TYPE_JKS;
                settings.ssl.trustStore = P12_SERVER_TRUST;
                settings.ssl.trustStorePassword = KEY_PASS;
                settings.ssl.trustStoreType = TRUSTSTORE_TYPE_PKCS12;
                break;
        }

        return settings;
    }

    private static SslContext createServerSslContext() {
        final SslProvider provider = SslProvider.JDK;

        try {
            // this is not good for production - just testing
            final SelfSignedCertificate ssc = new SelfSignedCertificate();
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).sslProvider(provider).build();
        } catch (Exception ce) {
            throw new RuntimeException("Couldn't setup self-signed certificate for test");
        }
    }

    private static SslContext sslContextForCert(final SelfSignedCertificate ssc) {
        try {
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).sslProvider(SslProvider.JDK).build();
        } catch (Exception ce) {
            throw new RuntimeException("Couldn't build server SSL context for test", ce);
        }
    }

    // Writes the given certificate into a temporary JKS truststore and returns its path, so a client can trust the
    // cert (chain validation passes) while a hostname mismatch remains the only possible verification failure.
    private static String writeTrustStore(final SelfSignedCertificate ssc) throws Exception {
        final KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(null, null);
        ts.setCertificateEntry("server", ssc.cert());
        final File file = File.createTempFile("gremlin-hostname-test-trust", ".jks");
        file.deleteOnExit();
        try (final FileOutputStream out = new FileOutputStream(file)) {
            ts.store(out, KEY_PASS.toCharArray());
        }
        return file.getAbsolutePath();
    }


    @Test
    public void shouldEnableSsl() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_SERVER_KEY).keyStorePassword(KEY_PASS).sslSkipCertValidation(true).create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailHandshakeOnHostnameMismatchOnlyWhenVerificationEnabled() throws Exception {
        // The server presents a certificate for CN=example.com; the client connects to "localhost" and trusts that
        // cert via a truststore, so chain validation passes and the hostname is the only variable. Running both
        // modes against the identical server/cert/host proves the handshake fails solely because of hostname
        // verification - not some unrelated certificate problem.
        final String trustStorePath = writeTrustStore(hostnameMismatchCert);

        // verification OFF: a trusted but mismatched certificate is accepted, so the request succeeds
        final Cluster permissive = TestClientFactory.build("localhost").enableSsl(true)
                .trustStore(trustStorePath).trustStorePassword(KEY_PASS).trustStoreType(TRUSTSTORE_TYPE_JKS)
                .sslHostnameVerification(false).create();
        try {
            assertEquals("test", permissive.connect().submit("'test'").one().getString());
        } finally {
            permissive.close();
        }

        // verification ON: the same certificate and host now fail the handshake on the hostname mismatch
        final Cluster verifying = TestClientFactory.build("localhost").enableSsl(true)
                .trustStore(trustStorePath).trustStorePassword(KEY_PASS).trustStoreType(TRUSTSTORE_TYPE_JKS)
                .sslHostnameVerification(true).create();
        try {
            verifying.connect().submit("'test'").one();
            fail("Should fail because the certificate hostname (example.com) does not match localhost");
        } catch (Exception x) {
            // The permissive run above connected successfully with the identical server, certificate and host, so
            // enabling hostname verification is the only changed variable - this failure is therefore caused by the
            // hostname mismatch. (The driver collapses the TLS error into a NoHostAvailableException without
            // attaching the SSL cause, so the differential above is the proof rather than the exception type.)
            assertThat(x, instanceOf(NoHostAvailableException.class));
        } finally {
            verifying.close();
        }
    }

    @Test
    public void shouldEnableWebSocketSsl() throws Exception {
        try (SimpleClient client = TestClientFactory.createSSLWebSocketClient()) {
            final Map<Object, Object> bindings = new HashMap<>();
            bindings.put("x", 123);
            bindings.put("y", 123);
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "x+y")
                    .addArg(Tokens.ARGS_BINDINGS, bindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                System.out.println(result.getStatus());
                if (result.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT) {
                    pass.set(ResponseStatusCode.SUCCESS == result.getStatus().getCode() &&
                            (((int) ((List) result.getResult().getData()).get(0) == 246)));
                }
                latch.countDown();
            });

            if (!latch.await(3000, TimeUnit.MILLISECONDS))
                fail("Request should have returned error, but instead timed out");
            assertThat(pass.get(), is(true));
        }
    }

    @Test
    public void shouldEnableSslWithSslContextProgrammaticallySpecified() throws Exception {
        // just for testing - this is not good for production use
        final SslContextBuilder builder = SslContextBuilder.forClient();
        builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        builder.sslProvider(SslProvider.JDK);

        final Cluster cluster = TestClientFactory.build().enableSsl(true).sslContext(builder.build()).create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslButFailIfClientConnectsWithoutIt() {
        final Cluster cluster = TestClientFactory.build().enableSsl(false).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl is enabled on the server but not on client");
        } catch(Exception x) {
            assertThat(x, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(x);
            assertThat(root, instanceOf(RuntimeException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthWithPkcs12() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(P12_CLIENT_KEY).keyStorePassword(KEY_PASS)
                .keyStoreType(KEYSTORE_TYPE_PKCS12).trustStore(P12_CLIENT_TRUST).trustStorePassword(KEY_PASS).create();
        final Client client = cluster.connect();

        try {
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuth() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_CLIENT_KEY).keyStorePassword(KEY_PASS)
                .keyStoreType(KEYSTORE_TYPE_JKS).trustStore(JKS_CLIENT_TRUST).trustStorePassword(KEY_PASS).create();
        final Client client = cluster.connect();

        try {
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithoutCert() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_SERVER_KEY).keyStorePassword(KEY_PASS)
                .keyStoreType(KEYSTORE_TYPE_JKS).sslSkipCertValidation(true).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client auth is enabled on the server but client does not have a cert");
        } catch (Exception x) {
            assertThat(x, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(x);
            assertThat(root, instanceOf(SSLException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithoutTrustedClientCert() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_CLIENT_KEY).keyStorePassword(KEY_PASS)
                .keyStoreType(KEYSTORE_TYPE_JKS).trustStore(JKS_CLIENT_TRUST).trustStorePassword(KEY_PASS).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client auth is enabled on the server but does not trust client's cert");
        } catch (Exception x) {
            assertThat(x, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(x);
            assertThat(root, instanceOf(SSLException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndFailIfProtocolsDontMatch() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_SERVER_KEY).keyStorePassword(KEY_PASS)
                .sslSkipCertValidation(true).sslEnabledProtocols(Arrays.asList("TLSv1.3")).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client requires TLSv1.2 whereas server supports only TLSv1.1");
        } catch (Exception x) {
            assertThat(x, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(x);
            assertThat(root, instanceOf(SSLException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndFailIfCiphersDontMatch() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_SERVER_KEY).keyStorePassword(KEY_PASS)
                .sslSkipCertValidation(true).sslCipherSuites(Arrays.asList("SSL_RSA_WITH_RC4_128_SHA")).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client requires TLSv1.2 whereas server supports only TLSv1.1");
        } catch (Exception x) {
            assertThat(x, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(x);
            assertThat(root, instanceOf(SSLException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthWithDifferentStoreType() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true)
                .keyStore(JKS_CLIENT_KEY).keyStorePassword(KEY_PASS).keyStoreType(KEYSTORE_TYPE_JKS)
                .trustStore(P12_CLIENT_TRUST).trustStorePassword(KEY_PASS).trustStoreType(TRUSTSTORE_TYPE_PKCS12)
                .create();
        final Client client = cluster.connect();

        try {
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }

        final Cluster cluster2 = TestClientFactory.build().enableSsl(true)
                .keyStore(P12_CLIENT_KEY).keyStorePassword(KEY_PASS).keyStoreType(KEYSTORE_TYPE_PKCS12)
                .trustStore(JKS_CLIENT_TRUST).trustStorePassword(KEY_PASS).trustStoreType(TRUSTSTORE_TYPE_JKS)
                .create();
        final Client client2 = cluster2.connect();

        try {
            assertEquals("test", client2.submit("'test'").one().getString());
        } finally {
            cluster2.close();
        }
    }
}
