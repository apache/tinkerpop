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

import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.CapacityChannel;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.HeapBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * An Apache HC {@link AsyncResponseConsumer} that delivers response data incrementally to a {@link ResultSet}.
 * As HTTP response chunks arrive, bytes are queued into a {@link QueueInputStream}. A reader thread from the
 * streaming reader pool reads from that stream, deserializes chunks via
 * {@link MessageSerializer#readChunk(Buffer, boolean)}, and pushes results to the {@code ResultSet} progressively.
 */
final class StreamingResponseConsumer implements AsyncResponseConsumer<Void> {

    private static final Logger logger = LoggerFactory.getLogger(StreamingResponseConsumer.class);
    private static final HeapBufferFactory BUFFER_FACTORY = new HeapBufferFactory();

    private final ResultSet resultSet;
    private final CompletableFuture<ResultSet> future;
    private final MessageSerializer<?> serializer;
    private final ExecutorService streamingReaderPool;
    private final long maxResponseContentLength;
    private final QueueInputStream queueInputStream = new QueueInputStream();

    private volatile int statusCode;
    private volatile boolean futureCompleted;
    private volatile boolean cancelled;
    private volatile int trailingStatusCode = -1;
    private volatile String trailingException;
    private long bytesRead;

    StreamingResponseConsumer(final ResultSet resultSet,
                              final CompletableFuture<ResultSet> future,
                              final MessageSerializer<?> serializer,
                              final ExecutorService streamingReaderPool,
                              final long maxResponseContentLength) {
        this.resultSet = resultSet;
        this.future = future;
        this.serializer = serializer;
        this.streamingReaderPool = streamingReaderPool;
        this.maxResponseContentLength = maxResponseContentLength;
    }

    /**
     * Cancels this consumer so that subsequent {@code consume()} calls discard data instead of buffering it.
     * Should be called when the client has given up on the response (e.g., timeout).
     */
    void cancel() {
        cancelled = true;
        queueInputStream.markComplete();
    }

    @Override
    public void consumeResponse(final HttpResponse response, final EntityDetails entityDetails,
                                final HttpContext context, final FutureCallback<Void> resultCallback) throws HttpException, IOException {
        statusCode = response.getCode();

        if (statusCode >= 200 && statusCode < 300) {
            // Complete the future immediately so the caller can start consuming results
            futureCompleted = true;
            future.complete(resultSet);

            if (entityDetails != null) {
                // Start the reader thread that will deserialize chunks as they arrive
                streamingReaderPool.execute(() -> readStream(resultCallback));
            } else {
                resultSet.markComplete();
                if (resultCallback != null) resultCallback.completed(null);
            }
        } else {
            // For error responses, complete the future with the ResultSet so the caller can
            // access the error through ResultSet.all() rather than the future itself.
            futureCompleted = true;
            future.complete(resultSet);

            if (entityDetails != null) {
                streamingReaderPool.execute(() -> readErrorStream(resultCallback));
            } else {
                final ResponseException ex = new ResponseException(statusCode, "Server returned status " + statusCode);
                resultSet.markError(ex);
                if (resultCallback != null) resultCallback.completed(null);
            }
        }
    }

    @Override
    public void informationResponse(final HttpResponse response, final HttpContext context) throws HttpException, IOException {
        // No action needed for 1xx responses
    }

    @Override
    public void updateCapacity(final CapacityChannel capacityChannel) throws IOException {
        if (cancelled) {
            capacityChannel.update(0);
            return;
        }
        // Allow unlimited buffering — backpressure is handled by the blocking queue
        capacityChannel.update(Integer.MAX_VALUE);
    }

    @Override
    public void consume(final ByteBuffer src) throws IOException {
        if (cancelled || !src.hasRemaining()) return;

        bytesRead += src.remaining();
        if (maxResponseContentLength > 0 && bytesRead > maxResponseContentLength) {
            cancelled = true;
            resultSet.markError(new ResponseException(413, "Response entity too large"));
            queueInputStream.markError(new IOException("Response entity too large"));
            return;
        }
        final byte[] data = new byte[src.remaining()];
        src.get(data);
        queueInputStream.enqueue(data);
    }

    @Override
    public void streamEnd(final List<? extends Header> trailers) throws HttpException, IOException {
        // Check trailing headers for error status before marking the stream complete.
        // The server sends error information in trailing headers when the response terminates with an error.
        if (trailers != null && !trailers.isEmpty()) {
            int trailingCode = -1;
            String trailingExc = null;
            for (final Header header : trailers) {
                if ("code".equalsIgnoreCase(header.getName())) {
                    try {
                        trailingCode = Integer.parseInt(header.getValue());
                    } catch (final NumberFormatException ignored) {}
                } else if ("exception".equalsIgnoreCase(header.getName())) {
                    trailingExc = header.getValue();
                }
            }
            if (trailingCode >= 400) {
                this.trailingStatusCode = trailingCode;
                this.trailingException = trailingExc;
            }
        }
        queueInputStream.markComplete();
    }

    @Override
    public void failed(final Exception cause) {
        cancelled = true;
        logger.debug("Streaming response failed", cause);
        queueInputStream.markError(new IOException(cause));
        if (!futureCompleted) {
            futureCompleted = true;
            resultSet.markError(cause);
            future.completeExceptionally(cause);
        }
    }

    @Override
    public void releaseResources() {
        cancelled = true;
        queueInputStream.markComplete();
    }

    /**
     * Reader thread: reads from the QueueInputStream, deserializes chunks, and pushes results to the ResultSet.
     * For streaming serializers (GraphBinary), chunks are deserialized progressively via readChunk().
     * For non-streaming serializers (GraphSON), the full response is buffered then deserialized via
     * deserializeBinaryResponse().
     */
    private void readStream(final FutureCallback<Void> resultCallback) {
        if (serializer instanceof GraphBinaryMessageSerializerV4) {
            readStreamingResponse(resultCallback);
        } else {
            readBufferedResponse(resultCallback);
        }
    }

    private void readStreamingResponse(final FutureCallback<Void> resultCallback) {
        boolean isFirstChunk = true;
        try {
            final InputStreamBuffer buffer = new InputStreamBuffer(queueInputStream);
            while (true) {
                final ResponseMessage msg = serializer.readChunk(buffer, isFirstChunk);
                isFirstChunk = false;

                if (msg.getResult() != null) {
                    if (msg.getResult().isBulked()) {
                        final List<?> data = msg.getResult().getData();
                        for (int i = 0; i < data.size(); i += 2) {
                            final Object value = data.get(i);
                            final long bulk = ((Number) data.get(i + 1)).longValue();
                            resultSet.add(new Result(new DefaultRemoteTraverser<>(value, bulk)));
                        }
                    } else {
                        for (final Object item : msg.getResult().getData()) {
                            resultSet.add(new Result(item));
                        }
                    }
                }

                // A status in the response indicates the footer has been read — this is the final chunk
                if (msg.getStatus() != null) {
                    if (msg.getStatus().getCode().code() >= 400) {
                        final ResponseException ex = new ResponseException(
                                msg.getStatus().getCode().code(),
                                msg.getStatus().getMessage(),
                                msg.getStatus().getException());
                        resultSet.markError(ex);
                    } else {
                        resultSet.markComplete();
                    }
                    if (resultCallback != null) resultCallback.completed(null);
                    return;
                }
            }
        } catch (final Exception e) {
            // If the stream ended prematurely but trailing headers indicate an error, use that
            if (trailingStatusCode >= 400) {
                final ResponseException ex = new ResponseException(trailingStatusCode,
                        trailingException != null ? trailingException : "Server error",
                        trailingException);
                resultSet.markError(ex);
            } else if (isPrematureEndOfStream(e)) {
                final ResponseException ex = new ResponseException(500,
                        "Server closed the response stream unexpectedly");
                resultSet.markError(ex);
            } else {
                resultSet.markError(e);
            }
            if (resultCallback != null) resultCallback.completed(null);
        }
    }

    private void readBufferedResponse(final FutureCallback<Void> resultCallback) {
        try {
            // Buffer the full response body
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final byte[] buf = new byte[8192];
            int n;
            while ((n = queueInputStream.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, n);
            }

            final byte[] body = baos.toByteArray();

            if (maxResponseContentLength > 0 && body.length > maxResponseContentLength) {
                resultSet.markError(new ResponseException(413, "Response entity too large"));
                if (resultCallback != null) resultCallback.completed(null);
                return;
            }

            final Buffer buffer = BUFFER_FACTORY.create(body);
            final ResponseMessage msg = serializer.deserializeBinaryResponse(buffer);

            // Check for error status
            if (msg.getStatus() != null && msg.getStatus().getCode().code() >= 400) {
                final ResponseException ex = new ResponseException(
                        msg.getStatus().getCode().code(),
                        msg.getStatus().getMessage(),
                        msg.getStatus().getException());
                resultSet.markError(ex);
                if (resultCallback != null) resultCallback.completed(null);
                return;
            }

            // Add all results to the ResultSet
            if (msg.getResult() != null) {
                if (msg.getResult().isBulked()) {
                    final List<?> data = msg.getResult().getData();
                    for (int i = 0; i < data.size(); i += 2) {
                        final Object value = data.get(i);
                        final long bulk = ((Number) data.get(i + 1)).longValue();
                        resultSet.add(new Result(new DefaultRemoteTraverser<>(value, bulk)));
                    }
                } else {
                    for (final Object item : msg.getResult().getData()) {
                        resultSet.add(new Result(item));
                    }
                }
            }
            resultSet.markComplete();
            if (resultCallback != null) resultCallback.completed(null);
        } catch (final Exception e) {
            resultSet.markError(e);
            if (resultCallback != null) resultCallback.completed(null);
        }
    }

    /**
     * Reader thread for error responses: buffers the body and deserializes it to extract the error details.
     * The server sends error responses as serialized GraphBinary/GraphSON messages with status information.
     */
    private void readErrorStream(final FutureCallback<Void> resultCallback) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final byte[] buf = new byte[4096];
            int n;
            while ((n = queueInputStream.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, n);
            }

            final byte[] body = baos.toByteArray();
            if (body.length > 0) {
                try {
                    final Buffer buffer = BUFFER_FACTORY.create(body);
                    final ResponseMessage msg = serializer.deserializeBinaryResponse(buffer);
                    if (msg.getStatus() != null) {
                        final ResponseException ex = new ResponseException(
                                msg.getStatus().getCode().code(),
                                msg.getStatus().getMessage(),
                                msg.getStatus().getException());
                        resultSet.markError(ex);
                        if (resultCallback != null) resultCallback.completed(null);
                        return;
                    }
                } catch (final Exception deserializeEx) {
                    // If deserialization fails, fall through to use the raw body as the error message
                    logger.debug("Could not deserialize error response body", deserializeEx);
                }
            }

            final String bodyStr = body.length > 0 ? new String(body) : "Server returned status " + statusCode;
            final ResponseException ex = new ResponseException(statusCode, bodyStr);
            resultSet.markError(ex);
            if (resultCallback != null) resultCallback.completed(null);
        } catch (final IOException e) {
            resultSet.markError(new ResponseException(statusCode, "Failed to read error response: " + e.getMessage()));
            if (resultCallback != null) resultCallback.completed(null);
        }
    }

    /**
     * Determines if the exception indicates a premature end-of-stream condition from the InputStreamBuffer.
     */
    private static boolean isPrematureEndOfStream(final Throwable e) {
        if (e instanceof IndexOutOfBoundsException && e.getMessage() != null &&
                e.getMessage().contains("End of stream")) {
            return true;
        }
        // SerializationException wrapping IndexOutOfBoundsException from readChunk
        final Throwable cause = e.getCause();
        if (cause instanceof IndexOutOfBoundsException && cause.getMessage() != null &&
                cause.getMessage().contains("End of stream")) {
            return true;
        }
        // UncheckedIOException from InputStreamBuffer when QueueInputStream has an error set
        if (e instanceof java.io.UncheckedIOException) {
            return true;
        }
        // SerializationException wrapping UncheckedIOException
        return cause instanceof java.io.UncheckedIOException;
    }

    private void completeWithError(final Exception ex, final FutureCallback<Void> resultCallback) {
        resultSet.markError(ex);
        if (!futureCompleted) {
            futureCompleted = true;
            future.completeExceptionally(ex);
        }
        if (resultCallback != null) resultCallback.completed(null);
    }


}
