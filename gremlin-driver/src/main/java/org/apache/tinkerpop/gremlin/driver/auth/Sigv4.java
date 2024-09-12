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

import com.amazonaws.DefaultRequest;
import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.SdkHttpUtils;
import com.amazonaws.util.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.http.entity.StringEntity;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.auth.internal.SignerConstants.AUTHORIZATION;
import static com.amazonaws.auth.internal.SignerConstants.HOST;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_DATE;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SECURITY_TOKEN;

/**
 * A {@link org.apache.tinkerpop.gremlin.driver.RequestInterceptor} that provides headers required for SigV4. Because
 * the signing process requires final header and body data, this interceptor should almost always be last.
 */
public class Sigv4 implements Auth {
    private final AWSCredentialsProvider awsCredentialsProvider;
    private final AWS4Signer aws4Signer;

    public Sigv4(final String regionName, final String serviceName) {
        this(regionName, new DefaultAWSCredentialsProviderChain(), serviceName);
    }

    public Sigv4(final String regionName, final AWSCredentialsProvider awsCredentialsProvider, final String serviceName) {
        this.awsCredentialsProvider = awsCredentialsProvider;

        aws4Signer = new AWS4Signer();
        aws4Signer.setRegionName(regionName);
        aws4Signer.setServiceName(serviceName);
    }

    @Override
    public HttpRequest apply(final HttpRequest httpRequest) {
        try {
            // Convert Http request into an AWS SDK signable request
            final SignableRequest<?> awsSignableRequest = toSignableRequest(httpRequest);

            // Sign the AWS SDK signable request (which internally adds some HTTP headers)
            final AWSCredentials credentials = awsCredentialsProvider.getCredentials();
            aws4Signer.sign(awsSignableRequest, credentials);

            // extract session token if temporary credentials are provided
            String sessionToken = "";
            if ((credentials instanceof BasicSessionCredentials)) {
                sessionToken = ((BasicSessionCredentials) credentials).getSessionToken();
            }

            final Map<String, String> headers = httpRequest.headers();
            headers.remove(HttpRequest.Headers.HOST);
            headers.put(HOST, awsSignableRequest.getHeaders().get(HOST));
            headers.put(X_AMZ_DATE, awsSignableRequest.getHeaders().get(X_AMZ_DATE));
            headers.put(AUTHORIZATION, awsSignableRequest.getHeaders().get(AUTHORIZATION));

            if (!sessionToken.isEmpty()) {
                headers.put(X_AMZ_SECURITY_TOKEN, sessionToken);
            }
        } catch (final Exception ex) {
            throw new AuthenticationException(ex);
        }
        return httpRequest;
    }

    private SignableRequest<?> toSignableRequest(final HttpRequest request) throws IOException {

        // make sure the request contains the minimal required set of information
        checkNotNull(request.getUri(), "The request URI must not be null");
        checkNotNull(request.getMethod(), "The request method must not be null");

        // convert the headers to the internal API format
        final Map<String, String> headers = request.headers();
        final Map<String, String> headersInternal = new HashMap<>();

        // we don't want to add the Host header as the Signer always adds the host header.
        for (Map.Entry<String, String> header : headers.entrySet()) {
            // Skip adding the Host header as the signing process will add one.
            if (!header.getKey().equalsIgnoreCase(HttpRequest.Headers.HOST)) {
                headersInternal.put(header.getKey(), header.getValue());
            }
        }

        // convert the parameters to the internal API format
        final URI uri = request.getUri();
        final Map<String, List<String>> parametersInternal = extractParametersFromQueryString(uri.getQuery());

        // carry over the entity (or an empty entity, if no entity is provided)
        if (!(request.getBody() instanceof byte[])) {
            throw new IllegalArgumentException("Expected byte[] in HttpRequest body but got " + request.getBody().getClass());
        }

        final byte[] body = (byte[]) request.getBody();
        final InputStream content = (body.length != 0) ? new ByteArrayInputStream(body) : new StringEntity("").getContent();
        final URI endpointUri = URI.create(uri.getScheme() + "://" + uri.getHost());

        return convertToSignableRequest(
                request.getMethod(),
                endpointUri,
                uri.getPath(),
                headersInternal,
                parametersInternal,
                content);
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

    private SignableRequest<?> convertToSignableRequest(
            final String httpMethodName,
            final URI httpEndpointUri,
            final String resourcePath,
            final Map<String, String> httpHeaders,
            final Map<String, List<String>> httpParameters,
            final InputStream httpContent) {

        // create the HTTP AWS SDK Signable Request and carry over information
        final DefaultRequest<?> awsRequest = new DefaultRequest<>(aws4Signer.getServiceName());
        awsRequest.setHttpMethod(HttpMethodName.fromValue(httpMethodName));
        awsRequest.setEndpoint(httpEndpointUri);
        awsRequest.setResourcePath(resourcePath);
        awsRequest.setHeaders(httpHeaders);
        awsRequest.setParameters(httpParameters);
        awsRequest.setContent(httpContent);

        return awsRequest;
    }

    private void checkNotNull(final Object obj, final String errMsg) {
        if (obj == null) {
            throw new IllegalArgumentException(errMsg);
        }
    }
}
