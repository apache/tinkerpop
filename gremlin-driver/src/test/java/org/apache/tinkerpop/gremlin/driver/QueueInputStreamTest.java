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

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueInputStreamTest {

    @Test
    public void shouldReadSingleChunk() throws IOException {
        final QueueInputStream stream = new QueueInputStream();
        stream.enqueue(new byte[]{1, 2, 3});
        stream.markComplete();

        final byte[] buf = new byte[3];
        final int read = stream.read(buf, 0, 3);
        assertEquals(3, read);
        assertArrayEquals(new byte[]{1, 2, 3}, buf);
        assertEquals(-1, stream.read());
    }

    @Test
    public void shouldReadMultipleChunks() throws IOException {
        final QueueInputStream stream = new QueueInputStream();
        stream.enqueue(new byte[]{10, 20});
        stream.enqueue(new byte[]{30, 40, 50});
        stream.markComplete();

        final byte[] buf = new byte[5];
        int total = 0;
        int n;
        while ((n = stream.read(buf, total, buf.length - total)) != -1) {
            total += n;
        }
        assertEquals(5, total);
        assertArrayEquals(new byte[]{10, 20, 30, 40, 50}, buf);
    }

    @Test
    public void shouldReturnMinusOneAfterComplete() throws IOException {
        final QueueInputStream stream = new QueueInputStream();
        stream.markComplete();

        assertEquals(-1, stream.read());
        assertEquals(-1, stream.read(new byte[4], 0, 4));
    }

    @Test
    public void shouldReadSingleByteCorrectly() throws IOException {
        final QueueInputStream stream = new QueueInputStream();
        stream.enqueue(new byte[]{(byte) 0xFF, 0x00, 0x7F});
        stream.markComplete();

        assertEquals(255, stream.read());
        assertEquals(0, stream.read());
        assertEquals(127, stream.read());
        assertEquals(-1, stream.read());
    }

    @Test(expected = IOException.class)
    public void shouldThrowOnErrorAfterRead() throws IOException {
        final QueueInputStream stream = new QueueInputStream();
        stream.markError(new IOException("test error"));

        stream.read();
    }

    @Test
    public void shouldBlockUntilDataArrives() throws Exception {
        final QueueInputStream stream = new QueueInputStream();
        final AtomicInteger result = new AtomicInteger(-2);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);

        final Thread reader = new Thread(() -> {
            try {
                started.countDown();
                result.set(stream.read());
            } catch (final IOException e) {
                result.set(-3);
            } finally {
                done.countDown();
            }
        });
        reader.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        // give the reader thread time to block
        Thread.sleep(50);
        stream.enqueue(new byte[]{42});
        stream.markComplete();

        assertTrue(done.await(1, TimeUnit.SECONDS));
        assertEquals(42, result.get());
    }

    @Test
    public void shouldReturnMinusOneAfterClose() throws IOException {
        final QueueInputStream stream = new QueueInputStream();
        stream.enqueue(new byte[]{1, 2, 3});
        stream.close();

        assertEquals(-1, stream.read());
    }
}
