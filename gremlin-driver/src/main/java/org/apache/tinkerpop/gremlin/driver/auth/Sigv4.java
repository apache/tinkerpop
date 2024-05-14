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
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.SdkHttpUtils;
import com.amazonaws.util.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.http.entity.StringEntity;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.auth.internal.SignerConstants.AUTHORIZATION;
import static com.amazonaws.auth.internal.SignerConstants.HOST;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_DATE;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SECURITY_TOKEN;

public class Sigv4 implements Auth {

    static final String NEPTUNE_SERVICE_NAME = "neptune-db";
    private final AWSCredentialsProvider awsCredentialsProvider;
    private final AWS4Signer aws4Signer;

    public Sigv4(final String regionName, final AWSCredentialsProvider awsCredentialsProvider) {
        this(regionName, awsCredentialsProvider, NEPTUNE_SERVICE_NAME);
    }

    public Sigv4(final String regionName, final AWSCredentialsProvider awsCredentialsProvider, final String serviceName) {
        this.awsCredentialsProvider = awsCredentialsProvider;

        aws4Signer = new AWS4Signer();
        aws4Signer.setRegionName(regionName);
        aws4Signer.setServiceName(serviceName);
    }

    @Override
    public FullHttpRequest apply(final FullHttpRequest fullHttpRequest) {
        try {
            // Convert Http request into an AWS SDK signable request
            final SignableRequest<?> awsSignableRequest = toSignableRequest(fullHttpRequest);

            // Sign the AWS SDK signable request (which internally adds some HTTP headers)
            final AWSCredentials credentials = awsCredentialsProvider.getCredentials();
            aws4Signer.sign(awsSignableRequest, credentials);

            // extract session token if temporary credentials are provided
            String sessionToken = "";
            if ((credentials instanceof BasicSessionCredentials)) {
                sessionToken = ((BasicSessionCredentials) credentials).getSessionToken();
            }

            // todo: confirm is needed to replace header `Host` with `host`
            fullHttpRequest.headers().remove(HttpHeaderNames.HOST);
            fullHttpRequest.headers().add(HOST, awsSignableRequest.getHeaders().get(HOST));
            fullHttpRequest.headers().add(X_AMZ_DATE, awsSignableRequest.getHeaders().get(X_AMZ_DATE));
            fullHttpRequest.headers().add(AUTHORIZATION, awsSignableRequest.getHeaders().get(AUTHORIZATION));

            if (!sessionToken.isEmpty()) {
                fullHttpRequest.headers().add(X_AMZ_SECURITY_TOKEN, sessionToken);
            }
        } catch (final Throwable t) {
            throw new RuntimeException(t.getMessage());
        }
        return fullHttpRequest;
    }

    private SignableRequest<?> toSignableRequest(final FullHttpRequest request)
            throws Exception {

        // make sure the request is not null and contains the minimal required set of information
        checkNotNull(request, "The request must not be null");
        checkNotNull(request.uri(), "The request URI must not be null");
        checkNotNull(request.method(), "The request method must not be null");

        // convert the headers to the internal API format
        final HttpHeaders headers = request.headers();
        final Map<String, String> headersInternal = new HashMap<>();

        String hostName = "";

        // we don't want to add the Host header as the Signer always adds the host header.
        for (String header : headers.names()) {
            // Skip adding the Host header as the signing process will add one.
            if (!header.equalsIgnoreCase(HOST)) {
                headersInternal.put(header, headers.get(header));
            } else {
                hostName = headers.get(header);
            }
        }

        // convert the parameters to the internal API format
        final URI uri = URI.create(request.uri());

        final String queryStr = uri.getQuery();
        final Map<String, List<String>> parametersInternal = new HashMap<>(extractParametersFromQueryString(queryStr));

        // carry over the entity (or an empty entity, if no entity is provided)
        final InputStream content;
        final ByteBuf contentBuffer = request.content();
        boolean hasContent = false;
        try {
            if (contentBuffer != null && contentBuffer.isReadable()) {
                hasContent = true;
                contentBuffer.retain();
                byte[] bytes = new byte[contentBuffer.readableBytes()];
                contentBuffer.getBytes(contentBuffer.readerIndex(), bytes);
                content = new ByteArrayInputStream(bytes);
            } else {
                content = new StringEntity("").getContent();
            }
        } catch (UnsupportedEncodingException e) {
            throw new Exception("Encoding of the input string failed", e);
        } catch (IOException e) {
            throw new Exception("IOException while accessing entity content", e);
        } finally {
            if (hasContent) {
                contentBuffer.release();
            }
        }

        if (StringUtils.isNullOrEmpty(hostName)) {
            // try to extract hostname from the uri since hostname was not provided in the header.
            final String authority = uri.getAuthority();
            if (authority == null) {
                throw new Exception("Unable to identify host information,"
                        + " either hostname should be provided in the uri or should be passed as a header");
            }

            hostName = authority;
        }

        final URI endpointUri = URI.create("http://" + hostName);

        return convertToSignableRequest(
                request.method().name(),
                endpointUri,
                uri.getPath(),
                headersInternal,
                parametersInternal,
                content);
    }

    private Map<String, List<String>> extractParametersFromQueryString(final String queryStr) {

        final Map<String, List<String>> parameters = new HashMap<>();

        // convert the parameters to the internal API format
        if (queryStr != null) {
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
        }

        return parameters;
    }

    private SignableRequest<?> convertToSignableRequest(
            final String httpMethodName,
            final URI httpEndpointUri,
            final String resourcePath,
            final Map<String, String> httpHeaders,
            final Map<String, List<String>> httpParameters,
            final InputStream httpContent) throws Exception {

        checkNotNull(httpMethodName, "Http method name must not be null");
        checkNotNull(httpEndpointUri, "Http endpoint URI must not be null");
        checkNotNull(httpHeaders, "Http headers must not be null");
        checkNotNull(httpParameters, "Http parameters must not be null");
        checkNotNull(httpContent, "Http content name must not be null");

        // create the HTTP AWS SDK Signable Request and carry over information
        final DefaultRequest<?> awsRequest = new DefaultRequest<>(NEPTUNE_SERVICE_NAME);
        awsRequest.setHttpMethod(HttpMethodName.fromValue(httpMethodName));
        awsRequest.setEndpoint(httpEndpointUri);
        awsRequest.setResourcePath(resourcePath);
        awsRequest.setHeaders(httpHeaders);
        awsRequest.setParameters(httpParameters);
        awsRequest.setContent(httpContent);

        return awsRequest;
    }

    private void checkNotNull(final Object obj, final String errMsg) throws Exception {
        if (obj == null) {
            throw new Exception(errMsg);
        }
    }
}
