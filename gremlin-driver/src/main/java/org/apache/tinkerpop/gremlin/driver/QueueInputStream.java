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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An {@link InputStream} backed by a {@link LinkedBlockingQueue} of byte arrays. Blocks on {@code read()} until
 * data arrives or the stream is marked complete. Used to bridge Apache HC's async data callbacks to the blocking
 * streaming deserialization reader thread.
 */
final class QueueInputStream extends InputStream {

    private static final byte[] END_MARKER = new byte[0];

    private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    private byte[] current;
    private int pos;
    private volatile boolean closed;
    private volatile IOException error;

    /**
     * Enqueue a chunk of data to be read.
     */
    void enqueue(final byte[] data) {
        queue.offer(data);
    }

    /**
     * Signal that no more data will arrive.
     */
    void markComplete() {
        queue.offer(END_MARKER);
    }

    /**
     * Signal an error condition. Subsequent reads will throw the given exception.
     */
    void markError(final IOException ex) {
        this.error = ex;
        queue.offer(END_MARKER);
    }

    @Override
    public int read() throws IOException {
        final byte[] buf = new byte[1];
        final int n = read(buf, 0, 1);
        return n == -1 ? -1 : buf[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (closed) return -1;

        while (current == null || pos >= current.length) {
            if (error != null) throw error;
            try {
                current = queue.take();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for data", e);
            }
            if (current == END_MARKER) {
                closed = true;
                if (error != null) throw error;
                return -1;
            }
            pos = 0;
        }

        final int available = current.length - pos;
        final int toRead = Math.min(available, len);
        System.arraycopy(current, pos, b, off, toRead);
        pos += toRead;
        return toRead;
    }

    @Override
    public int available() {
        if (closed) return 0;
        final int currentAvailable = (current != null) ? current.length - pos : 0;
        return currentAvailable;
    }

    @Override
    public void close() {
        closed = true;
    }
}
