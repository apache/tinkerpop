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

package org.apache.tinkerpop.gremlin.driver.auth;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
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
    private static final String REGION = "us-west-2";
    private static final String SERVICE_NAME = "service-name";
    private static final byte[] REQUEST_BODY = "{\"gremlin\":\"2-1\"}".getBytes(StandardCharsets.UTF_8);
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
        HttpRequest httpRequest = createRequest();
        sigv4.apply(httpRequest);
        validateExpectedHeaders(httpRequest);
    }

    @Test
    public void shouldAddSignedHeadersAndSessionToken() throws Exception {
        String sessionToken = "foobarz";
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsSessionCredentials.create(KEY, SECRET, sessionToken));
        HttpRequest httpRequest = createRequest();
        sigv4.apply(httpRequest);
        validateExpectedHeaders(httpRequest);
        assertEquals(sessionToken, httpRequest.headers().get(X_AMZ_SECURITY_TOKEN));
    }

    @Test
    public void shouldThrowIfRequestNonByteArray() {
        Auth.AuthenticationException ex = assertThrows(Auth.AuthenticationException.class,
                () -> sigv4.apply(new HttpRequest(new HashMap<>(), "not byte array", new URI(URI_WITH_QUERY_PARAMS))));
        assertTrue(ex.getMessage().contains("Expected byte[] in HttpRequest body"));
    }

    @Test
    public void shouldThrowIfNoRequestMethod() {
        Auth.AuthenticationException ex = assertThrows(Auth.AuthenticationException.class,
                () -> sigv4.apply(new HttpRequest(new HashMap<>(), REQUEST_BODY, new URI(URI_WITH_QUERY_PARAMS), null)));
        assertTrue(ex.getMessage().contains("The request method must not be null"));
    }

    @Test
    public void shouldThrowIfNoRequestURI() {
        Auth.AuthenticationException ex = assertThrows(Auth.AuthenticationException.class,
                () -> sigv4.apply(new HttpRequest(new HashMap<>(), REQUEST_BODY, null)));
        assertTrue(ex.getMessage().contains("The request URI must not be null"));
    }

    private HttpRequest createRequest() throws URISyntaxException {
        HttpRequest httpRequest = new HttpRequest(new HashMap<>(), REQUEST_BODY, new URI(URI_WITH_QUERY_PARAMS));
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

}