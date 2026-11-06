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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.stream.ByteBufQueueInputStream;
import org.apache.tinkerpop.gremlin.driver.stream.GraphBinaryStreamResponseReader;
import org.apache.tinkerpop.gremlin.driver.stream.InputStreamBuffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.LAST_CONTENT_READ_RESPONSE;

/**
 * Decodes chunked HTTP responses into streaming results without buffering the full response body.
 * <p>
 * For GraphBinary responses, content chunks are passed to a {@link ByteBufQueueInputStream} consumed by a
 * {@link GraphBinaryStreamResponseReader} on a separate thread. That reader deserializes results incrementally,
 * delivers them to the {@code ResultSet}, and handles completion and cleanup. For non-GraphBinary error responses
 * (e.g., JSON 401/500), the error body is accumulated and parsed when the response ends, then
 * {@code LAST_CONTENT_READ_RESPONSE} is fired for {@link GremlinResponseHandler} to process.
 */
public class HttpStreamingResponseHandler extends MessageToMessageDecoder<HttpObject> {

    private static final Logger logger = LoggerFactory.getLogger(HttpStreamingResponseHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Grace period added to {@code readTimeout} when arming the reader thread's backstop timeout, so the pipeline
     * {@link ReadTimeoutHandler} always fires first on a stalled connection.
     */
    private static final long BACKSTOP_GRACE_MILLIS = 5000L;

    private final GraphBinaryReader graphBinaryReader;
    private final AtomicReference<ResultSet> pendingResultSet;
    private final ExecutorService readerPool;
    private final long backstopTimeoutMillis;

    // Mutable state below is accessed exclusively from the channel's event loop thread.
    private HttpResponseStatus responseStatus;
    private String contentType;
    private long bytesRead;
    private ByteBufQueueInputStream queueInputStream;
    private CompositeByteBuf errorBody;

    public HttpStreamingResponseHandler(final GraphBinaryReader graphBinaryReader,
                                        final AtomicReference<ResultSet> pendingResultSet,
                                        final ExecutorService readerPool) {
        this(graphBinaryReader, pendingResultSet, readerPool, 0L);
    }

    /**
     * @param readTimeoutMillis the connection's {@code readTimeout}. When positive, the reader thread's own timeout is
     *                          armed as a backstop {@value #BACKSTOP_GRACE_MILLIS}ms longer, so the pipeline
     *                          {@link ReadTimeoutHandler} fires first on a stalled connection and terminates the
     *                          response with a properly typed exception. A value {@code <= 0} (the default, meaning
     *                          "no timeout") leaves the reader blocking indefinitely.
     *                          <p>
     *                          The translation to the backstop bound lives here rather than in
     *                          {@link ByteBufQueueInputStream}'s constructor so that the stream can be constructed with
     *                          the effective bound directly (small values in tests, without paying the full grace).
     */
    public HttpStreamingResponseHandler(final GraphBinaryReader graphBinaryReader,
                                        final AtomicReference<ResultSet> pendingResultSet,
                                        final ExecutorService readerPool,
                                        final long readTimeoutMillis) {
        this.graphBinaryReader = graphBinaryReader;
        this.pendingResultSet = pendingResultSet;
        this.readerPool = readerPool;
        this.backstopTimeoutMillis = readTimeoutMillis > 0 ? readTimeoutMillis + BACKSTOP_GRACE_MILLIS : 0L;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final HttpObject msg,
                          final List<Object> out) throws Exception {
        if (msg instanceof HttpResponse) {
            final HttpResponse resp = (HttpResponse) msg;

            // Reset mutable state for the new response cycle to prevent stale state from a previous
            // response bleeding into this one when the handler is reused on the same connection.
            resetState();

            responseStatus = resp.status();
            contentType = resp.headers().get(HttpHeaderNames.CONTENT_TYPE);
            queueInputStream = new ByteBufQueueInputStream(backstopTimeoutMillis);

            // Signal that the server's response headers have arrived, before any body chunk is processed. The full
            // round trip to the server has completed, so a caller blocked on headersReceivedAsync() (e.g. a remote
            // transaction's submit) can now proceed knowing the server has ordered this request ahead of any later one
            // on the same transaction, without waiting for the body to stream back. An error response still trips this
            // via markComplete()/markError() downstream.
            {
                final ResultSet rsForHeaders = pendingResultSet.get();
                if (rsForHeaders != null) rsForHeaders.markHeadersReceived();
            }

            // Spawn reader thread for GraphBinary responses
            if (isGraphBinaryResponse()) {
                final ResultSet rs = pendingResultSet.get();
                if (rs != null) {
                    final InputStreamBuffer buffer = new InputStreamBuffer(queueInputStream);
                    final GraphBinaryStreamResponseReader streamReader =
                            new GraphBinaryStreamResponseReader(buffer, graphBinaryReader, rs, pendingResultSet);
                    try {
                        readerPool.submit(streamReader::run);
                    } catch (RejectedExecutionException e) {
                        logger.warn("Failed to schedule streaming response reader for channel {} with status {} " +
                                        "and content type {}",
                                ctx.channel().id().asShortText(), responseStatus, contentType, e);
                        queueInputStream.signalEndOfStream();
                        rs.markError(e);
                        pendingResultSet.compareAndSet(rs, null);
                        out.add(LAST_CONTENT_READ_RESPONSE);
                    }
                } else {
                    // No pending ResultSet — close the stream and fire sentinel immediately
                    queueInputStream.signalEndOfStream();
                    queueInputStream = null;
                    out.add(LAST_CONTENT_READ_RESPONSE);
                }
            }
        }

        if (msg instanceof HttpContent) {
            final ByteBuf content = ((HttpContent) msg).content();
            bytesRead += content.readableBytes();

            if (bytesRead > 0 && ctx.channel().attr(InactiveChannelHandler.BYTES_READ).get() == null) {
                ctx.channel().attr(InactiveChannelHandler.BYTES_READ).set(0);
            }

            if (!isGraphBinaryResponse()) {
                // Accumulate non-GraphBinary error body across chunks
                if (content.readableBytes() > 0) {
                    if (errorBody == null) {
                        errorBody = ctx.alloc().compositeBuffer();
                    }
                    // retain() because Netty releases the content ByteBuf after decode() returns
                    errorBody.addComponent(true, content.retain());
                }
            } else if (content.readableBytes() > 0 && queueInputStream != null) {
                // Feed bytes to the reader thread
                // retain() because Netty releases the content ByteBuf after decode() returns
                queueInputStream.offer(content.retain());
            }

            if (msg instanceof LastHttpContent) {
                if (isGraphBinaryResponse()) {
                    if (queueInputStream != null) {
                        queueInputStream.signalEndOfStream();
                        // Null out so any spurious content arriving between responses is dropped
                        // rather than offered to the already-closed stream.
                        queueInputStream = null;
                    }
                    out.add(LAST_CONTENT_READ_RESPONSE);
                } else {
                    // Non-GraphBinary error — parse accumulated body and fire sentinel
                    handleNonGraphBinaryError();
                    out.add(LAST_CONTENT_READ_RESPONSE);
                }
            }
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        // Signal end-of-stream only AFTER super.channelInactive so the pending request is marked errored (by the
        // downstream GremlinResponseHandler) before the reader thread is unblocked. Otherwise the reader can win the
        // race with an EOFException from the closed stream. This change matches how errors are handled in
        // exceptionCaught() which is to mark the ResultSet before signaling the stream.
        releaseErrorBody();
        super.channelInactive(ctx);
        if (queueInputStream != null) {
            queueInputStream.signalEndOfStream();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        // Mark error before signaling end-of-stream so the reader thread can't race
        // with an EOFException from the closed stream.
        final ResultSet rs = pendingResultSet.getAndSet(null);
        if (rs != null) {
            rs.markError(cause);
        }
        if (queueInputStream != null) {
            queueInputStream.signalEndOfStream();
        }
        releaseErrorBody();
        super.exceptionCaught(ctx, cause);
    }

    private void handleNonGraphBinaryError() {
        final ResultSet rs = pendingResultSet.get();
        if (rs == null) return;

        try {
            if (errorBody != null && errorBody.readableBytes() > 0) {
                final JsonNode node = mapper.readTree(errorBody.toString(CharsetUtil.UTF_8));
                final String message = node.has("message") ? node.get("message").asText() : responseStatus.reasonPhrase();
                rs.markError(new ResponseException(responseStatus, message));
            } else {
                rs.markError(new ResponseException(responseStatus, responseStatus.reasonPhrase()));
            }
        } catch (Exception e) {
            logger.debug("Failed to parse error response body as JSON", e);
            rs.markError(new ResponseException(responseStatus, responseStatus.reasonPhrase()));
        } finally {
            pendingResultSet.compareAndSet(rs, null);
            releaseErrorBody();
        }
    }

    private void resetState() {
        // Clean up any leftover resources from a previous response on this connection
        if (queueInputStream != null) {
            queueInputStream.signalEndOfStream();
            queueInputStream = null;
        }
        releaseErrorBody();
        bytesRead = 0;
        responseStatus = null;
        contentType = null;
    }

    private void releaseErrorBody() {
        if (errorBody != null) {
            errorBody.release();
            errorBody = null;
        }
    }

    private boolean isGraphBinaryResponse() {
        return !isError(responseStatus) || SerTokens.MIME_GRAPHBINARY_V4.equals(contentType);
    }

    private static boolean isError(final HttpResponseStatus status) {
        return status != HttpResponseStatus.OK;
    }
}
