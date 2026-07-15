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
package org.apache.tinkerpop.gremlin.driver.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * An {@link InputStream} backed by a {@link BlockingQueue} of {@link ByteBuf} objects. The Netty event loop
 * offers ByteBufs to the queue as HTTP content chunks arrive, and a reader thread consumes them via
 * standard InputStream reads.
 */
public class ByteBufQueueInputStream extends InputStream {

    private static final ByteBuf END_OF_STREAM = Unpooled.buffer(0);

    private final BlockingQueue<ByteBuf> queue;
    private final long timeoutMillis;
    private ByteBuf current;
    private volatile boolean eof;

    public ByteBufQueueInputStream() {
        this(0L);
    }

    /**
     * @param timeoutMillis the effective maximum time a read blocks waiting for the next chunk. A value {@code <= 0}
     *                      makes the read block indefinitely, deferring the liveness bound to the connection's
     *                      {@code readTimeout} (see {@link org.apache.tinkerpop.gremlin.driver.handler.ReadTimeoutHandler}).
     *                      A positive value acts only as a backstop and is expected to be set longer than
     *                      {@code readTimeout} so the pipeline read-timeout fires first on a stalled connection.
     *                      <p>
     *                      This takes the already-resolved bound rather than a {@code readTimeout} to translate: the
     *                      translation (adding the backstop grace) lives with its caller in
     *                      {@link org.apache.tinkerpop.gremlin.driver.handler.HttpStreamingResponseHandler}. Keeping
     *                      the constructor parameter as the effective bound also lets tests exercise the timeout with
     *                      small values instead of paying the full backstop grace.
     */
    public ByteBufQueueInputStream(final long timeoutMillis) {
        this.queue = new LinkedBlockingQueue<>();
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Offer a ByteBuf to the queue. The caller must have already retained the ByteBuf if needed.
     * The ByteBuf will be released after it is fully read. If the stream is already closed,
     * the buffer is released immediately.
     */
    public void offer(final ByteBuf buf) {
        if (eof) {
            if (buf != END_OF_STREAM && buf.refCnt() > 0) {
                buf.release();
            }
            return;
        }
        queue.add(buf);
    }

    /**
     * Signal that no more ByteBufs will be offered.
     */
    public void signalEndOfStream() {
        queue.offer(END_OF_STREAM);
    }

    @Override
    public int read() throws IOException {
        if (eof) return -1;

        while (current == null || !current.isReadable()) {
            releaseCurrent();
            current = awaitNext();
            if (current == END_OF_STREAM) {
                eof = true;
                current = null;
                return -1;
            }
        }
        return current.readByte() & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (eof) return -1;
        if (len == 0) return 0;

        // Block until at least one byte is available, then return what we have (short read).
        while (current == null || !current.isReadable()) {
            releaseCurrent();
            current = awaitNext();
            if (current == END_OF_STREAM) {
                eof = true;
                current = null;
                return -1;
            }
        }
        final int readable = Math.min(current.readableBytes(), len);
        current.readBytes(b, off, readable);
        return readable;
    }

    /**
     * Waits for the next buffer. When {@code timeoutMillis <= 0} this blocks indefinitely so the wait is bounded only
     * by the connection's read-timeout (which, when it fires, closes the channel and delivers {@code END_OF_STREAM}
     * here). A positive {@code timeoutMillis} is a backstop: if it elapses without a buffer, the connection's
     * read-timeout should already have fired, so reaching this point signals that the normal termination path failed.
     */
    private ByteBuf awaitNext() throws IOException {
        try {
            final ByteBuf next = timeoutMillis <= 0
                    ? queue.take()
                    : queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (next == null) throw new IOException("Timed out waiting for streaming response data");
            return next;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for data", e);
        }
    }

    @Override
    public void close() throws IOException {
        eof = true;
        releaseCurrent();
        // drain and release any remaining buffers
        ByteBuf buf;
        while ((buf = queue.poll()) != null) {
            if (buf != END_OF_STREAM && buf.refCnt() > 0) {
                buf.release();
            }
        }
    }

    private void releaseCurrent() {
        if (current != null && current != END_OF_STREAM && current.refCnt() > 0) {
            current.release();
        }
        current = null;
    }

}
