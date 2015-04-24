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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoReaderWriterTest {
    @Test
    public void shouldBeAbleToReUseBuilderInDifferentThreads() throws Exception {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final GraphWriter writer = GryoWriter.build().create();
        writer.writeVertex(os, new DetachedVertex(1, "test", new HashMap<>()));
        os.close();

        final byte[] bytes = os.toByteArray();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean assertProcess1 = new AtomicBoolean(false);
        final AtomicBoolean assertProcess2 = new AtomicBoolean(false);
        final AtomicBoolean assertProcess3 = new AtomicBoolean(false);

        // just one builder to re-use among threads
        final GryoReader.Builder builder = GryoReader.build();

        final Thread process1 = new Thread(() -> {
            try {
                latch.await();
                final GryoReader reader = builder.create();
                final Vertex v = reader.readVertex(new ByteArrayInputStream(bytes), Attachable::get);
                assertProcess1.set(v.id().equals(1));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        final Thread process2 = new Thread(() -> {
            try {
                latch.await();
                final GryoReader reader = builder.create();
                final Vertex v = reader.readVertex(new ByteArrayInputStream(bytes), Attachable::get);
                assertProcess2.set(v.id().equals(1));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        final Thread process3 = new Thread(() -> {
            try {
                latch.await();
                final GryoReader reader = builder.create();
                final Vertex v = reader.readVertex(new ByteArrayInputStream(bytes), Attachable::get);
                assertProcess3.set(v.id().equals(1));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        process1.start();
        process2.start();
        process3.start();

        latch.countDown();

        process3.join();
        process2.join();
        process1.join();

        assertTrue(assertProcess1.get());
        assertTrue(assertProcess2.get());
        assertTrue(assertProcess3.get());
    }
}
