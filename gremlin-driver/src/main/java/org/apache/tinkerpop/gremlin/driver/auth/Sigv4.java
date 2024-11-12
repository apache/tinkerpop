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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.AUTHORIZATION;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.HOST;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_CONTENT_SHA256;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_DATE;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_SECURITY_TOKEN;

/**
 * A {@link org.apache.tinkerpop.gremlin.driver.RequestInterceptor} that provides headers required for SigV4. Because
 * the signing process requires final header and body data, this interceptor should almost always be last.
 */
public class Sigv4 implements Auth {
    private static final Logger logger = LoggerFactory.getLogger(Sigv4.class);
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsV4HttpSigner aws4Signer;
    private final String serviceName;
    private final String regionName;

    public Sigv4(final String regionName, final String serviceName) {
        this(regionName, DefaultCredentialsProvider.create(), serviceName);
    }

    public Sigv4(final String regionName, final AwsCredentialsProvider awsCredentialsProvider, final String serviceName) {
        this.awsCredentialsProvider = awsCredentialsProvider;

        aws4Signer = AwsV4HttpSigner.create();
        this.regionName = regionName;
        this.serviceName = serviceName;
    }

    @Override
    public HttpRequest apply(final HttpRequest httpRequest) {
        try {
            final ContentStreamProvider content = toContentStream(httpRequest);
            // Convert Http request into an AWS SDK signable request
            final SdkHttpRequest awsSignableRequest = toSignableRequest(httpRequest);
            final AwsCredentials credentials = awsCredentialsProvider.resolveCredentials();

            // Sign the AWS SDK signable request (which internally adds some HTTP headers)
            final SignedRequest signed = aws4Signer.sign(r -> r.identity(credentials)
                    .request(awsSignableRequest)
                    .payload(content)
                    .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, this.serviceName)
                    .putProperty(AwsV4HttpSigner.REGION_NAME, this.regionName));

            final Map<String, String> headers = httpRequest.headers();
            setSignedHeaders(headers, signed);
            setSessionToken(headers, credentials);
        } catch (final Exception ex) {
            logger.error("Error signing HTTP request: {}", ex.getMessage(), ex);
            throw new AuthenticationException(ex);
        }
        return httpRequest;
    }

    private void setSessionToken(final Map<String, String> headers, final AwsCredentials credentials) {
        // extract session token if temporary credentials are provided
        if ((credentials instanceof AwsSessionCredentials)) {
            final String sessionToken = ((AwsSessionCredentials) credentials).sessionToken();
            if (sessionToken != null && !sessionToken.isEmpty()) {
                headers.put(X_AMZ_SECURITY_TOKEN, sessionToken);
            }
        }
    }

    private void setSignedHeaders(final Map<String, String> headers, final SignedRequest signed) {
        headers.remove(HttpRequest.Headers.HOST);
        headers.put(HOST, signed.request().host());
        final Map<String, List<String>> signedHeaders = signed.request().headers();
        headers.put(X_AMZ_DATE, getSingleHeaderValue(signedHeaders, X_AMZ_DATE));
        headers.put(AUTHORIZATION, getSingleHeaderValue(signedHeaders, AUTHORIZATION));
        headers.put(X_AMZ_CONTENT_SHA256, getSingleHeaderValue(signedHeaders, X_AMZ_CONTENT_SHA256));
    }

    private String getSingleHeaderValue(final Map<String, List<String>> headers, final String headerName) {
        final Set<String> headerValues = new HashSet<>(headers.containsKey(headerName) ? headers.get(headerName) : Collections.emptySet());
        if (headerValues.size() != 1) {
            throw new IllegalArgumentException(String.format("Expected 1 header %s but found %d", headerName, headerValues.size()));
        }
        return headerValues.iterator().next();
    }

    private ContentStreamProvider toContentStream(final HttpRequest httpRequest) {
        // carry over the entity (or an empty entity, if no entity is provided)
        if (!(httpRequest.getBody() instanceof byte[])) {
            throw new IllegalArgumentException("Expected byte[] in HttpRequest body but got " + httpRequest.getBody().getClass());
        }
        final byte[] body = (byte[]) httpRequest.getBody();
        return (body.length != 0) ? ContentStreamProvider.fromByteArray(body) : ContentStreamProvider.fromUtf8String("");
    }

    private SdkHttpRequest toSignableRequest(final HttpRequest request) {

        // make sure the request contains the minimal required set of information
        checkNotNull(request.getUri(), "The request URI must not be null");
        checkNotNull(request.getMethod(), "The request method must not be null");

        // convert the headers to the internal API format
        final Map<String, String> headers = request.headers();
        final Map<String, List<String>> headersInternal = new HashMap<>();

        // we don't want to add the Host header as the Signer always adds the host header.
        for (Map.Entry<String, String> header : headers.entrySet()) {
            // Skip adding the Host header as the signing process will add one.
            if (!header.getKey().equalsIgnoreCase(HttpRequest.Headers.HOST)) {
                headersInternal.put(header.getKey(), Collections.singletonList(header.getValue()));
            }
        }

        // convert the parameters to the internal API format
        final URI uri = request.getUri();
        final Map<String, List<String>> parametersInternal = extractParametersFromQueryString(uri.getQuery());

        final URI endpointUri = URI.create(uri.getScheme() + "://" + uri.getHost());

        // create the HTTP AWS SdkHttpRequest and carry over information
        return SdkHttpRequest.builder()
                .uri(endpointUri)
                .encodedPath(uri.getPath())
                .method(SdkHttpMethod.fromValue(request.getMethod()))
                .headers(headersInternal)
                .rawQueryParameters(parametersInternal)
                .build();
    }

    private HashMap<String, List<String>> extractParametersFromQueryString(final String queryStr) {

        final HashMap<String, List<String>> parameters = new HashMap<>();

        if (queryStr == null) {
            return parameters;
        }

        // convert the parameters to the internal API format
        for (final String queryParam : queryStr.split("&")) {

            if (!queryParam.isEmpty()) {
                final String[] keyValuePair = queryParam.split("=", 2);

                // parameters are encoded in the HTTP request, we need to decode them here
                final String key = SdkHttpUtils.urlDecode(keyValuePair[0]);
                final String value;

                if (keyValuePair.length == 2) {
                    value = SdkHttpUtils.urlDecode(keyValuePair[1]);
                } else {
                    value = "";
                }

                // insert the parameter key into the map, if not yet present
                if (!parameters.containsKey(key)) {
                    parameters.put(key, new ArrayList<>());
                }

                // append the parameter value to the list for the given key
                parameters.get(key).add(value);
            }
        }

        return parameters;
    }

    private void checkNotNull(final Object obj, final String errMsg) {
        if (obj == null) {
            throw new IllegalArgumentException(errMsg);
        }
    }
}
