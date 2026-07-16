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

package org.apache.tinkerpop.gremlin.driver.auth;

import java.net.URI;
import java.util.HashMap;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.AUTHORIZATION;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_CONTENT_SHA256;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_DATE;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_SECURITY_TOKEN;

public class Sigv4Test {
    private static final String REGION = "region-1";
    private static final String SERVICE_NAME = "service-name";
    private static final String HOST = "localhost";
    private static final String URI_WITH_QUERY_PARAMS = "http://" + HOST + ":8182?a=1&b=2";
    private static final String KEY = "foo";
    private static final String SECRET = "bar";
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    private AwsCredentialsProvider credentialsProvider;
    private Sigv4 sigv4;

    @Before
    public void setup() {
        sigv4 = new Sigv4(REGION, credentialsProvider, SERVICE_NAME);
    }

    @Test
    public void shouldAddSignedHeaders() throws Exception {
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create(KEY, SECRET));
        HttpRequest httpRequest = createRequestWithRequestMessage();
        sigv4.intercept(httpRequest);
        validateExpectedHeaders(httpRequest);
    }

    @Test
    public void shouldSignPreSerializedBody() throws Exception {
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create(KEY, SECRET));
        HttpRequest httpRequest = createRequestWithBytes();
        sigv4.intercept(httpRequest);
        validateExpectedHeaders(httpRequest);
    }

    @Test
    public void shouldAddSignedHeadersAndSessionToken() throws Exception {
        String sessionToken = "foobarz";
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsSessionCredentials.create(KEY, SECRET, sessionToken));
        HttpRequest httpRequest = createRequestWithRequestMessage();
        sigv4.intercept(httpRequest);
        validateExpectedHeaders(httpRequest);
        assertEquals(sessionToken, httpRequest.headers().get(X_AMZ_SECURITY_TOKEN));
    }

    @Test
    public void shouldThrowIfBodyIsUnsupportedType() {
        // serializeBody() will throw because body is a String
        Auth.AuthenticationException ex = assertThrows(Auth.AuthenticationException.class,
                () -> sigv4.intercept(new HttpRequest(new HashMap<>(), "not valid", URI.create(URI_WITH_QUERY_PARAMS))));
        assertTrue(ex.getCause().getMessage().contains("Cannot serialize body of type"));
    }

    @Test
    public void shouldThrowIfNoRequestMethod() throws Exception {
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create(KEY, SECRET));
        final byte[] body = "{\"gremlin\":\"g.V()\"}".getBytes();
        Auth.AuthenticationException ex = assertThrows(Auth.AuthenticationException.class,
                () -> sigv4.intercept(new HttpRequest(new HashMap<>(), body, URI.create(URI_WITH_QUERY_PARAMS), null)));
        assertTrue(ex.getCause().getMessage().contains("The request method must not be null"));
    }

    @Test
    public void shouldThrowIfNoRequestURI() {
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create(KEY, SECRET));
        final byte[] body = "{\"gremlin\":\"g.V()\"}".getBytes();
        Auth.AuthenticationException ex = assertThrows(Auth.AuthenticationException.class,
                () -> sigv4.intercept(new HttpRequest(new HashMap<>(), body, null)));
        assertTrue(ex.getCause().getMessage().contains("The request URI must not be null"));
    }

    private HttpRequest createRequestWithRequestMessage() throws Exception {
        final RequestMessage msg = RequestMessage.build("g.V()").addG("g").create();
        HttpRequest httpRequest = new HttpRequest(new HashMap<>(), msg, new URI(URI_WITH_QUERY_PARAMS));
        httpRequest.headers().put("Host", "this-should-be-ignored-for-signed-host-header");
        return httpRequest;
    }

    private HttpRequest createRequestWithBytes() throws Exception {
        final byte[] body = "{\"gremlin\":\"2-1\"}".getBytes();
        HttpRequest httpRequest = new HttpRequest(new HashMap<>(), body, new URI(URI_WITH_QUERY_PARAMS));
        httpRequest.headers().put("Content-Type", "application/json");
        httpRequest.headers().put("Host", "this-should-be-ignored-for-signed-host-header");
        return httpRequest;
    }

    private void validateExpectedHeaders(HttpRequest httpRequest) {
        assertEquals(HOST, httpRequest.headers().get(SignerConstant.HOST));
        assertNotNull(httpRequest.headers().get(X_AMZ_DATE));
        assertNotNull(httpRequest.headers().get(X_AMZ_CONTENT_SHA256));
        assertThat(httpRequest.headers().get(AUTHORIZATION),
                allOf(startsWith("AWS4-HMAC-SHA256 Credential=" + KEY),
                        containsString("/" + REGION + "/service-name/aws4_request"),
                        containsString("Signature=")));
    }

    @Test
    public void shouldSignOnlyMinimalHeaderSetForBasicCredentials() throws Exception {
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create(KEY, SECRET));
        final HttpRequest httpRequest = createRequestWithTransportHeaders();
        sigv4.intercept(httpRequest);

        // Only host and the headers the AWS SDK adds itself are signed. Transport-managed headers
        // (accept-encoding, content-type, ...) are never signed, or the signature would not match
        // what the server reconstructs.
        assertEquals("host;x-amz-content-sha256;x-amz-date", signedHeaders(httpRequest));
    }

    @Test
    public void shouldSignSecurityTokenForSessionCredentials() throws Exception {
        when(credentialsProvider.resolveCredentials())
                .thenReturn(AwsSessionCredentials.create(KEY, SECRET, "session-token"));
        final HttpRequest httpRequest = createRequestWithTransportHeaders();
        sigv4.intercept(httpRequest);

        assertEquals("host;x-amz-content-sha256;x-amz-date;x-amz-security-token", signedHeaders(httpRequest));
    }

    private HttpRequest createRequestWithTransportHeaders() throws Exception {
        final byte[] body = "{\"gremlin\":\"g.V().count()\"}".getBytes();
        final HttpRequest httpRequest = new HttpRequest(new HashMap<>(), body, new URI(URI_WITH_QUERY_PARAMS));
        // Seed transport-managed / content headers that must NOT end up in SignedHeaders.
        httpRequest.headers().put("Accept", "application/vnd.graphbinary-v4.0");
        httpRequest.headers().put("Content-Type", "application/json");
        httpRequest.headers().put("Accept-Encoding", "deflate");
        httpRequest.headers().put("User-Agent", "gremlin-java-test");
        return httpRequest;
    }

    private static String signedHeaders(final HttpRequest httpRequest) {
        final String authorization = httpRequest.headers().get(AUTHORIZATION);
        final int start = authorization.indexOf("SignedHeaders=") + "SignedHeaders=".length();
        final int end = authorization.indexOf(',', start);
        return authorization.substring(start, end < 0 ? authorization.length() : end);
    }
}
