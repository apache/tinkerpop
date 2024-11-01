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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.AUTHORIZATION;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.HOST;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_CONTENT_SHA256;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_DATE;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_SECURITY_TOKEN;

public class Sigv4Test {
    public static final String US_WEST_2 = "us-west-2";
    public static final String SERVICE_NAME = "service-name";
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    private AwsCredentialsProvider credentialsProvider;
    private Sigv4 sigv4;

    @Before
    public void setup() {
        sigv4 = new Sigv4(US_WEST_2, credentialsProvider, SERVICE_NAME);
    }

    @Test
    public void shouldAddSignedHeaders() throws Exception {
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create("foo", "bar"));
        HttpRequest httpRequest = createRequest();
        sigv4.apply(httpRequest);
        validateExpectedHeaders(httpRequest);
    }

    @Test
    public void shouldAddSignedHeadersAndSessionToken() throws Exception {
        String sessionToken = "foobarz";
        when(credentialsProvider.resolveCredentials()).thenReturn(AwsSessionCredentials.create("foo", "bar", sessionToken));
        HttpRequest httpRequest = createRequest();
        sigv4.apply(httpRequest);
        validateExpectedHeaders(httpRequest);
        assertEquals(sessionToken, httpRequest.headers().get(X_AMZ_SECURITY_TOKEN));
    }

    private HttpRequest createRequest() throws URISyntaxException {
        HttpRequest httpRequest = new HttpRequest(new HashMap<>(), "{\"gremlin\":\"2-1\"}".getBytes(StandardCharsets.UTF_8), new URI("http://localhost:8182?a=1&b=2"));
        httpRequest.headers().put("Content-Type", "application/json");
        return httpRequest;
    }

    private void validateExpectedHeaders(HttpRequest httpRequest) {
        assertEquals("localhost", httpRequest.headers().get(HOST));
        assertNotNull(httpRequest.headers().get(X_AMZ_DATE));
        assertNotNull(httpRequest.headers().get(AUTHORIZATION));
        assertNotNull(httpRequest.headers().get(X_AMZ_CONTENT_SHA256));
    }

}