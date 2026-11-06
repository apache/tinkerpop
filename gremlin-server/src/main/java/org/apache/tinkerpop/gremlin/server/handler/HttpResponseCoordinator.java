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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.util.GremlinError;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
import static io.netty.handler.codec.http.HttpHeaderNames.TRANSFER_ENCODING;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Owns every response write for a single HTTP request and guarantees, by construction, that a request produces
 * exactly one well-formed, terminated chunked response. This is a plain per-request helper, not a Netty pipeline
 * handler.
 * <p>
 * All mutating methods are guarded by this object's monitor, so the eval-worker thread and the timeout-scheduler
 * thread cannot interleave their writes. The {@link State#COMPLETED} short-circuit makes the terminal-write methods
 * idempotent: whichever thread terminates the response first wins, and the others become no-ops. The eval task calls
 * {@link #complete} from a {@code finally} block, so the terminal {@code LastHttpContent} (which clears the channel's
 * in-use flag and ends the chunked stream) is written even if the body-producing code threw.
 * <p>
 * Locking discipline (IMPORTANT for maintainers): the {@code state} and {@code headerSent} fields are guarded by
 * this object's monitor. The {@code public}/package methods are {@code synchronized} and are the only valid entry
 * points. The private helpers ({@code ensureHeaderSent}, {@code writeTerminal}) mutate or rely on that guarded state
 * but are deliberately NOT {@code synchronized} — they assume the caller already holds the monitor. Only ever call
 * them from a {@code synchronized} method, and only ever read/write {@code state}/{@code headerSent} while holding
 * the monitor. Do not add an entry point that touches this state without {@code synchronized}.
 */
final class HttpResponseCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(HttpResponseCoordinator.class);

    private enum State {
        /** No response body has been written yet. */
        NOT_STARTED,
        /** The opening framing (header chunk) has been written; more chunks or a footer are expected. */
        STREAMING,
        /** The response has been fully terminated (terminal {@code LastHttpContent} written); no further writes. */
        COMPLETED
    }

    private final Context context;
    private final MessageSerializer<?> serializer;
    private final String contentType;

    private State state = State.NOT_STARTED;
    private boolean headerSent = false;

    HttpResponseCoordinator(final Context context, final String contentType, final MessageSerializer<?> serializer) {
        this.context = context;
        this.contentType = contentType;
        this.serializer = serializer;
    }

    /**
     * Sends the {@code 200 OK} chunked response header exactly once. The header pairs are supplied by the caller
     * because, for a begin-transaction request, the transaction id header is only known once the transaction has been
     * opened. No-op if the response was already terminated or the header was already sent.
     */
    synchronized void writeHeader(final CharSequence... headers) {
        if (state == State.COMPLETED) return;
        ensureHeaderSent(HttpResponseStatus.OK, headers);
    }

    /**
     * Writes one page of results. On the final page ({@code hasMore == false}) this also writes the body footer and
     * the terminal {@code LastHttpContent}, transitioning to {@link State#COMPLETED}. No-op if already completed.
     * <p>
     * On a serialization failure the error is written through {@link #writeError} (terminating the response) and the
     * exception is re-thrown so the caller can stop iterating.
     */
    synchronized void writeData(final List<Object> aggregate, final boolean hasMore, final boolean bulking) throws Exception {
        if (state == State.COMPLETED) return;

        final ChannelHandlerContext nettyContext = context.getChannelHandlerContext();
        // backstop: the eval task sends the header before iterating, but guarantee it here too.
        ensureHeaderSent(HttpResponseStatus.OK, HttpHeaderNames.CONTENT_TYPE, contentType);

        final boolean firstChunk = state == State.NOT_STARTED;
        final boolean terminal = !hasMore;

        final ByteBuf chunk;
        try {
            // detachment runs inside the try so a failure here is reported to the client as a serialization error
            // (matching the prior makeChunk behavior) rather than escaping uncaught.
            context.handleDetachment(aggregate);

            // An intermediate streaming chunk carries only data (no status, no ResponseMessage). Every other case
            // builds a full ResponseMessage; the terminal page additionally carries the OK status. The four cases map
            // onto distinct serializer framing calls: single-shot (first+terminal), header (first+more),
            // footer (streaming+terminal), and chunk (streaming+more).
            if (!firstChunk && !terminal) {
                chunk = serializer.writeChunk(aggregate, nettyContext.alloc());
            } else {
                final ResponseMessage.Builder builder = ResponseMessage.build().result(aggregate).bulked(bulking);
                if (terminal) builder.code(HttpResponseStatus.OK);
                final ResponseMessage responseMessage = builder.create();

                if (firstChunk && terminal) {
                    chunk = serializer.serializeResponseAsBinary(responseMessage, nettyContext.alloc());
                } else if (firstChunk) {
                    // Serialize the header chunk BEFORE transitioning to STREAMING. If writeHeader throws (the very
                    // first streamed element fails to serialize), state stays NOT_STARTED so the catch below routes to
                    // writeError while still pre-stream — which serializes a full, parseable error message rather than
                    // a footer-only body with no preceding header bytes.
                    chunk = serializer.writeHeader(responseMessage, nettyContext.alloc());
                    state = State.STREAMING;
                } else {
                    chunk = serializer.writeFooter(responseMessage, nettyContext.alloc());
                }
            }
        } catch (Exception ex) {
            final UUID requestId = nettyContext.attr(StateKey.REQUEST_ID).get();
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", aggregate, requestId, ex);
            writeError(GremlinError.serialization(ex));
            throw ex;
        }

        nettyContext.writeAndFlush(new DefaultHttpContent(chunk));

        // The final page closes the body framing above; now end the chunked stream.
        if (!hasMore) {
            writeTerminal(HttpResponseStatus.OK, "");
            state = State.COMPLETED;
        }
    }

    /**
     * Writes an error response and terminates the stream. Serializes an error footer when mid-stream or a complete
     * message otherwise. No-op if the response was already terminated. Never throws on serialization failure (logs
     * instead) so the terminal {@code LastHttpContent} is still written.
     */
    synchronized void writeError(final ResponseMessage responseMessage) {
        if (state == State.COMPLETED) return;

        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        try {
            // Send the header before serializing the body. An error response that has not yet emitted a header carries
            // the error status code on the header line, matching the prior behavior of HttpHandlerUtil.writeError. Doing
            // this first means that if serialization then throws, the finally below still terminates with a well-formed
            // (body-less) error rather than a bare LastHttpContent with no header line. Mid-stream this is a no-op since
            // the header was already sent during the data phase.
            ensureHeaderSent(responseMessage.getStatus().getCode(), HttpHeaderNames.CONTENT_TYPE, contentType);

            final ByteBuf byteBuf = state == State.STREAMING
                    ? serializer.writeErrorFooter(responseMessage, ctx.alloc())
                    : serializer.serializeResponseAsBinary(responseMessage, ctx.alloc());
            ctx.writeAndFlush(new DefaultHttpContent(byteBuf));
        } catch (Throwable t) {
            // Catch Throwable (not just SerializationException) and swallow it: a custom TypeSerializer throwing an
            // unchecked RuntimeException/Error must not propagate out of writeError (it would be uncaught on the
            // timeout/scheduler thread) nor skip the terminal write in the finally. The error body could not be
            // serialized, but the stream is still terminated so the client does not hang and the keep-alive channel's
            // in-use flag clears.
            logger.warn("Unable to serialize ResponseMessage: {} ", responseMessage, t);
        } finally {
            // Idempotent terminal write: runs whether or not the body serialized, so the chunked stream is always
            // ended. Mirrors complete()'s finally-based backstop in the eval task.
            writeTerminal(responseMessage.getStatus().getCode(), responseMessage.getStatus().getException());
            state = State.COMPLETED;
        }
    }

    /**
     * Writes an error response built from a {@link GremlinError}.
     */
    synchronized void writeError(final GremlinError error) {
        writeError(ResponseMessage.build()
                .code(error.getCode())
                .statusMessage(error.getMessage())
                .exception(error.getException())
                .create());
    }

    /**
     * Terminal call from the eval task's {@code finally}. Idempotent: writes the terminal {@code LastHttpContent}
     * only if the response was not already completed (by the final data page or an error). This guarantees the
     * chunked stream is always ended even when the body-producing code threw an unchecked exception.
     */
    synchronized void complete(final HttpResponseStatus status, final String exceptionType) {
        if (state == State.COMPLETED) return;
        ensureHeaderSent(status, HttpHeaderNames.CONTENT_TYPE, contentType);
        writeTerminal(status, exceptionType);
        state = State.COMPLETED;
    }

    // Caller must hold this object's monitor: reads and mutates the guarded {@code headerSent} field. Only ever
    // invoked from the synchronized public methods; intentionally not synchronized itself (see class javadoc).
    private void ensureHeaderSent(final HttpResponseStatus status, final CharSequence... headers) {
        if (headerSent) return;
        if ((headers.length % 2) != 0) throw new IllegalArgumentException("Headers should come in pairs.");

        final HttpResponse responseHeader = new DefaultHttpResponse(HTTP_1_1, status);
        responseHeader.headers().set(TRANSFER_ENCODING, CHUNKED);
        for (int i = 0; i < headers.length; i += 2) {
            responseHeader.headers().set(headers[i], headers[i + 1]);
        }
        context.getChannelHandlerContext().writeAndFlush(responseHeader);
        headerSent = true;
    }

    // Caller must hold this object's monitor: invoked only from the synchronized terminal-write methods as part of
    // their guarded state transition to COMPLETED. Intentionally not synchronized itself (see class javadoc).
    private void writeTerminal(final HttpResponseStatus statusCode, final String exceptionType) {
        final DefaultLastHttpContent last = new DefaultLastHttpContent();
        last.trailingHeaders().add(SerTokens.TOKEN_CODE, statusCode.code());
        if (exceptionType != null && !exceptionType.isEmpty()) {
            last.trailingHeaders().add(SerTokens.TOKEN_EXCEPTION, exceptionType);
        }
        context.getChannelHandlerContext().writeAndFlush(last);
    }
}
