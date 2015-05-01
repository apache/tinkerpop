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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.javatuples.Pair;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoRegistryTest {
    @Test
    public void shouldFindRegisteredClassesByIoImplementation() {
        // note that this is a non-standard usage of IoRegistry strictly for testing purposes - refer to javadocs
        // for proper usage
        final FakeIoRegistry registry = new FakeIoRegistry();
        registry.register(GryoIo.class, Long.class, "test");
        registry.register(GryoIo.class, Integer.class, 1);
        registry.register(GryoIo.class, String.class, 1L);
        registry.register(GraphSONIo.class, Short.class, 100);

        final List<Pair<Class, Object>> foundGryo = registry.find(GryoIo.class);
        assertEquals(3, foundGryo.size());
        assertEquals("test", foundGryo.get(0).getValue1());
        assertEquals(String.class, foundGryo.get(2).getValue0());
        assertEquals(1L, foundGryo.get(2).getValue1());
        assertEquals(Long.class, foundGryo.get(0).getValue0());
        assertEquals(1, foundGryo.get(1).getValue1());
        assertEquals(Integer.class, foundGryo.get(1).getValue0());

        final List<Pair<Class, Object>> foundGraphSON = registry.find(GraphSONIo.class);
        assertEquals(1, foundGraphSON.size());
        assertEquals(100, foundGraphSON.get(0).getValue1());
        assertEquals(Short.class, foundGraphSON.get(0).getValue0());
    }

    @Test
    public void shouldFindRegisteredClassesByIoImplementationAndSerializer() {
        // note that this is a non-standard usage of IoRegistry strictly for testing purposes - refer to javadocs
        // for proper usage
        final FakeIoRegistry registry = new FakeIoRegistry();
        registry.register(GryoIo.class, Long.class, "test");
        registry.register(GryoIo.class, Integer.class, 1);
        registry.register(GryoIo.class, String.class, 1L);
        registry.register(GraphSONIo.class, Short.class, 100);

        final List<Pair<Class, Number>> foundGryo = registry.find(GryoIo.class, Number.class);
        assertEquals(2, foundGryo.size());
        assertEquals(String.class, foundGryo.get(1).getValue0());
        assertEquals(1L, foundGryo.get(1).getValue1());
        assertEquals(1, foundGryo.get(0).getValue1());
        assertEquals(Integer.class, foundGryo.get(0).getValue0());

        final List<Pair<Class, Date>> foundGraphSON = registry.find(GraphSONIo.class, Date.class);
        assertEquals(0, foundGraphSON.size());
    }

    @Test
    public void shouldReturnEmptyListIfIoKeyNotPresentOnFindByImplementation() {
        final FakeIoRegistry registry = new FakeIoRegistry();
        assertEquals(0, registry.find(GryoIo.class).size());
    }

    @Test
    public void shouldReturnEmptyListIfIoKeyNotPresentOnFindByImplementationAndSerializer() {
        final FakeIoRegistry registry = new FakeIoRegistry();
        assertEquals(0, registry.find(GryoIo.class, String.class).size());
    }

    @Test
    public void shouldReturnEmptyListIfIoKeyPresentButNoSerializer() {
        final FakeIoRegistry registry = new FakeIoRegistry();
        registry.register(GryoIo.class, Long.class, String.class);
        assertEquals(0, registry.find(GryoIo.class, Number.class).size());
    }

    /**
     * This class is just for simplicity of testing.  This is not a proper implementation of this class.
     * Proper implementations should call {@link #register(Class, Class, Object)} within a zero-arg constructor.
     */
    public static class FakeIoRegistry extends AbstractIoRegistry {
    }
}
