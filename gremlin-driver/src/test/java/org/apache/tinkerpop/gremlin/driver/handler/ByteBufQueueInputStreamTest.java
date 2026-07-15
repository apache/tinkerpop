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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.stream.ByteBufQueueInputStream;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class ByteBufQueueInputStreamTest {

    @Test
    public void shouldReadSingleByteBuf() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(new byte[]{1, 2, 3, 4});
        stream.offer(buf);
        stream.signalEndOfStream();

        assertEquals(1, stream.read());
        assertEquals(2, stream.read());
        assertEquals(3, stream.read());
        assertEquals(4, stream.read());
        assertEquals(-1, stream.read());
    }

    @Test
    public void shouldReadAcrossMultipleByteBufs() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(Unpooled.wrappedBuffer(new byte[]{1, 2}));
        stream.offer(Unpooled.wrappedBuffer(new byte[]{3, 4}));
        stream.signalEndOfStream();

        final byte[] result = new byte[8];
        int totalRead = 0;
        int read;
        while ((read = stream.read(result, totalRead, result.length - totalRead)) != -1) {
            totalRead += read;
        }
        assertEquals(4, totalRead);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, java.util.Arrays.copyOf(result, totalRead));
    }

    @Test
    public void shouldReleaseByteBufsAfterReading() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(4);
        buf.writeBytes(new byte[]{1, 2, 3, 4});
        assertEquals(1, buf.refCnt());

        stream.offer(buf);
        stream.signalEndOfStream();

        final byte[] result = new byte[4];
        stream.read(result, 0, 4);
        stream.read(); // triggers release of buf and reads EOS

        assertEquals(0, buf.refCnt());
    }

    @Test
    public void shouldThrowWhenBoundedReadTimesOut() throws Exception {
        // A positive timeout is a backstop - when it elapses with no buffer offered, the read fails rather than
        // blocking forever.
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream(50L);
        try {
            stream.read();
            fail("Expected a timeout since no buffer was ever offered");
        } catch (IOException ex) {
            assertEquals("Timed out waiting for streaming response data", ex.getMessage());
        }
    }

    @Test(timeout = 10000)
    public void shouldBlockIndefinitelyWhenUnboundedUntilBufferArrives() throws Exception {
        // A timeout <= 0 means "no timeout" - the read blocks until a buffer is offered rather than giving up.
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream(0L);
        final AtomicInteger readValue = new AtomicInteger(-2);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch started = new CountDownLatch(1);

        final Thread reader = new Thread(() -> {
            started.countDown();
            try {
                readValue.set(stream.read());
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        reader.start();

        // Let the reader block, then confirm it is still waiting well past what any old hardcoded bound would allow
        // to elapse in this test, and only unblocks once a buffer is actually offered.
        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(200);
        assertTrue("reader should still be blocked waiting for data", reader.isAlive());

        stream.offer(Unpooled.wrappedBuffer(new byte[]{7}));
        reader.join(5000);

        assertFalse("reader should have unblocked once data arrived", reader.isAlive());
        assertNull(failure.get());
        assertEquals(7, readValue.get());
    }

    @Test
    public void shouldCleanUpOnClose() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        final ByteBuf buf1 = ByteBufAllocator.DEFAULT.buffer(2);
        buf1.writeBytes(new byte[]{1, 2});
        final ByteBuf buf2 = ByteBufAllocator.DEFAULT.buffer(2);
        buf2.writeBytes(new byte[]{3, 4});

        stream.offer(buf1);
        stream.offer(buf2);
        stream.close();

        assertEquals(0, buf1.refCnt());
        assertEquals(0, buf2.refCnt());
    }
}
