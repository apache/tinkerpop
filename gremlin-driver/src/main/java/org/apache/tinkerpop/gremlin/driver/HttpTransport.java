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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Transport layer that uses Apache HttpClient 5.x async client to send requests to Gremlin Server.
 * Replaces the Netty-based Connection/Channelizer infrastructure.
 */
final class HttpTransport {
    private static final Logger logger = LoggerFactory.getLogger(HttpTransport.class);

    private final CloseableHttpAsyncClient httpClient;
    private final MessageSerializer<?> serializer;
    private final List<Pair<String, ? extends RequestInterceptor>> interceptors;
    private final ExecutorService executor;
    private final ExecutorService streamingReaderPool;
    private final boolean enableUserAgent;
    private final boolean bulkResults;
    private final long maxResponseContentLength;

    HttpTransport(final CloseableHttpAsyncClient httpClient,
                  final MessageSerializer<?> serializer,
                  final List<Pair<String, ? extends RequestInterceptor>> interceptors,
                  final ExecutorService executor,
                  final ExecutorService streamingReaderPool,
                  final boolean enableUserAgent,
                  final boolean bulkResults,
                  final long maxResponseContentLength) {
        this.httpClient = httpClient;
        this.serializer = serializer;
        this.interceptors = interceptors;
        this.executor = executor;
        this.streamingReaderPool = streamingReaderPool;
        this.enableUserAgent = enableUserAgent;
        this.bulkResults = bulkResults;
        this.maxResponseContentLength = maxResponseContentLength;
    }

    /**
     * Sends a request to the specified host and returns a CompletableFuture that completes with a ResultSet.
     * Uses streaming response handling to deliver results progressively as HTTP response chunks arrive.
     */
    CompletableFuture<ResultSet> sendAsync(final Host host, final RequestMessage requestMessage) {
        final URI hostUri = host.getHostUri();

        // Build the HttpRequest and run it through interceptors
        final Map<String, String> headers = new HashMap<>();
        HttpRequest httpRequest = new HttpRequest(headers, requestMessage, hostUri);

        for (final Pair<String, ? extends RequestInterceptor> interceptorPair : interceptors) {
            httpRequest = interceptorPair.getRight().apply(httpRequest);
        }

        // After interceptors, body should be byte[]
        if (!(httpRequest.getBody() instanceof byte[])) {
            throw new IllegalStateException("Request body must be byte[] after interceptor processing");
        }

        final byte[] body = (byte[]) httpRequest.getBody();
        final String contentType = httpRequest.headers().getOrDefault(
                HttpRequest.Headers.CONTENT_TYPE, serializer.mimeTypesSupported()[0]);

        // Build the Apache HC request
        final SimpleHttpRequest request = SimpleRequestBuilder.post(hostUri)
                .setBody(body, ContentType.parse(contentType))
                .build();

        // Copy headers from our HttpRequest to the Apache HC request
        for (final Map.Entry<String, String> header : httpRequest.headers().entrySet()) {
            if (!HttpRequest.Headers.CONTENT_TYPE.equals(header.getKey())) {
                request.setHeader(header.getKey(), header.getValue());
            }
        }

        // Set accept header for response deserialization
        request.setHeader(HttpRequest.Headers.ACCEPT, serializer.mimeTypesSupported()[0]);

        if (enableUserAgent) {
            request.setHeader(HttpRequest.Headers.USER_AGENT, UserAgent.USER_AGENT);
        }

        final CompletableFuture<ResultSet> future = new CompletableFuture<>();
        final ResultSet resultSet = new ResultSet(executor, requestMessage, host);

        // Use streaming response consumer to deliver results progressively
        final StreamingResponseConsumer consumer = new StreamingResponseConsumer(
                resultSet, future, serializer, streamingReaderPool, maxResponseContentLength);

        // When the ResultSet is done (success, error, or timeout), cancel the consumer
        // to stop buffering any further incoming data and prevent OOM.
        resultSet.getReadCompleted().whenComplete((v, t) -> consumer.cancel());

        httpClient.execute(
                SimpleRequestProducer.create(request),
                consumer,
                new FutureCallback<Void>() {
                    @Override
                    public void completed(final Void result) {
                        // Streaming consumer handles completion via the reader thread
                    }

                    @Override
                    public void failed(final Exception ex) {
                        logger.debug("Request to {} failed", host, ex);
                        // StreamingResponseConsumer.failed() handles error propagation
                    }

                    @Override
                    public void cancelled() {
                        final RuntimeException ex = new RuntimeException("Request to " + host + " was cancelled");
                        resultSet.markError(ex);
                        if (!future.isDone()) {
                            future.completeExceptionally(ex);
                        }
                    }
                });

        return future;
    }

    /**
     * Closes the underlying HTTP client.
     */
    void close() {
        try {
            httpClient.close();
        } catch (final Exception e) {
            logger.warn("Error closing HTTP client", e);
        }
    }
}
