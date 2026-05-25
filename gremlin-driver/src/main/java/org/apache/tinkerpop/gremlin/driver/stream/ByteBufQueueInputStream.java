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
    private final int capacity;
    private final Runnable onSpaceAvailable;
    private ByteBuf current;
    private volatile boolean eof;
    private volatile boolean readsPaused;

    public ByteBufQueueInputStream() {
        this(Integer.MAX_VALUE, () -> {});
    }

    public ByteBufQueueInputStream(final int capacity, final Runnable onSpaceAvailable) {
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.capacity = capacity;
        this.onSpaceAvailable = onSpaceAvailable;
    }

    /**
     * Offer a ByteBuf to the queue. The caller must have already retained the ByteBuf if needed.
     * The ByteBuf will be released after it is fully read. If the stream is already closed,
     * the buffer is released immediately.
     *
     * @return true if the buffer was accepted, false if the queue is full.
     */
    public boolean offer(final ByteBuf buf) {
        if (eof) {
            if (buf != END_OF_STREAM && buf.refCnt() > 0) {
                buf.release();
            }
            return true;
        }
        return queue.offer(buf);
    }

    /**
     * Blocking put for when the queue is full. The caller should pause reads before calling this
     * to avoid blocking the event loop indefinitely.
     */
    public void putBlocking(final ByteBuf buf) throws InterruptedException {
        if (eof) {
            if (buf != END_OF_STREAM && buf.refCnt() > 0) {
                buf.release();
            }
            return;
        }
        queue.put(buf);
    }

    /**
     * Mark that the producer has paused reads due to backpressure.
     */
    public void markPaused() {
        readsPaused = true;
    }

    /**
     * Signal that no more ByteBufs will be offered.
     */
    public void signalEndOfStream() {
        try {
            queue.put(END_OF_STREAM);
        } catch (InterruptedException e) {
            eof = true;
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int read() throws IOException {
        if (eof) return -1;

        while (current == null || !current.isReadable()) {
            releaseCurrent();
            try {
                current = queue.poll(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for data", e);
            }
            if (current == null) {
                throw new IOException("Timed out waiting for streaming response data");
            }
            if (current == END_OF_STREAM) {
                eof = true;
                current = null;
                return -1;
            }
            checkBackpressure();
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
            try {
                current = queue.poll(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for data", e);
            }
            if (current == null) {
                throw new IOException("Timed out waiting for streaming response data");
            }
            if (current == END_OF_STREAM) {
                eof = true;
                current = null;
                return -1;
            }
            checkBackpressure();
        }
        final int readable = Math.min(current.readableBytes(), len);
        current.readBytes(b, off, readable);
        return readable;
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

    private void checkBackpressure() {
        if (readsPaused && queue.size() < Math.max(1, capacity / 2)) {
            readsPaused = false;
            onSpaceAvailable.run();
        }
    }

    private void releaseCurrent() {
        if (current != null && current != END_OF_STREAM && current.refCnt() > 0) {
            current.release();
        }
        current = null;
    }

}
