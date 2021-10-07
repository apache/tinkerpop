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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.server.util.ExceptionTestUtils.assertExceptionChainContainsCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GremlinServerSslIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final String PEM_SERVER_KEY = "src/test/resources/server.key.pk8";
    private static final String PEM_SERVER_CRT = "src/test/resources/server.crt";
    private static final String PEM_CLIENT_KEY = "src/test/resources/client.key.pk8";
    private static final String PEM_CLIENT_CRT = "src/test/resources/client.crt";

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldEnableSsl":
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
            case "shouldEnableSslAndClientCertificateAuthWithLegacyPem":
            case "shouldEnableSslAndClientCertificateAuthAndFailWithoutCertWithLegacyPem":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyCertChainFile = PEM_SERVER_CRT;
                settings.ssl.keyFile = PEM_SERVER_KEY;
                settings.ssl.keyPassword = KEY_PASS;
                // Trust the client
                settings.ssl.trustCertChainFile = PEM_CLIENT_CRT;
                break;
            // Trust the client
            case "shouldEnableSslAndClientCertificateAuthAndFailWithoutTrustedClientCertWithLegacyPem":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyCertChainFile = PEM_SERVER_CRT;
                settings.ssl.keyFile = PEM_SERVER_KEY;
                settings.ssl.keyPassword = KEY_PASS;
                // Trust ONLY the server cert
                settings.ssl.trustCertChainFile = PEM_SERVER_CRT;
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
                // Server TLS version must be enabled by default in used JDK version or tests will fail
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
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthWithLegacyPem() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true)
                .keyCertChainFile(PEM_CLIENT_CRT).keyFile(PEM_CLIENT_KEY)
                .keyPassword(KEY_PASS).trustCertificateChainFile(PEM_SERVER_CRT).create();
        final Client client = cluster.connect();

        try {
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithoutCertWithLegacyPem() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_SERVER_KEY).keyStorePassword(KEY_PASS).sslSkipCertValidation(true).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client auth is enabled on the server but client does not have a cert");
        } catch(Exception x) {
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithoutTrustedClientCertWithLegacyPem() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true)
                .keyCertChainFile(PEM_CLIENT_CRT).keyFile(PEM_CLIENT_KEY)
                .keyPassword(KEY_PASS).trustCertificateChainFile(PEM_SERVER_CRT).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client auth is enabled on the server but does not trust client's cert");
        } catch(Exception x) {
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
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
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
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
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndFailIfProtocolsDontMatch() {
        // Server TLS version must be enabled by default in used JDK version or this test will fail
        final Cluster cluster = TestClientFactory.build().enableSsl(true).keyStore(JKS_SERVER_KEY).keyStorePassword(KEY_PASS)
                .sslSkipCertValidation(true).sslEnabledProtocols(Arrays.asList("TLSv1.3")).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client requires higher TLS version than what the server is configured to support");
        } catch (Exception x) {
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
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
            fail("Should throw exception because ciphers don't match");
        } catch (Exception x) {
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
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

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithIncorrectKeyStoreType() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true)
                .keyStore(JKS_CLIENT_KEY).keyStorePassword(KEY_PASS).keyStoreType(KEYSTORE_TYPE_PKCS12)
                .trustStore(P12_CLIENT_TRUST).trustStorePassword(KEY_PASS).trustStoreType(TRUSTSTORE_TYPE_PKCS12)
                .create();
        final Client client = cluster.connect();

        try {
            String res = client.submit("'test'").one().getString();
            fail("Should throw exception because incorrect keyStoreType is specified");
        } catch (Exception x) {
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithIncorrectTrustStoreType() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true)
                .keyStore(P12_CLIENT_KEY).keyStorePassword(KEY_PASS).keyStoreType(KEYSTORE_TYPE_PKCS12)
                .trustStore(JKS_CLIENT_TRUST).trustStorePassword(KEY_PASS).trustStoreType(TRUSTSTORE_TYPE_PKCS12)
                .create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because incorrect trustStoreType is specified");
        } catch (Exception x) {
            assertExceptionChainContainsCause(x, NoHostAvailableException.class);
        } finally {
            cluster.close();
        }
    }
}
