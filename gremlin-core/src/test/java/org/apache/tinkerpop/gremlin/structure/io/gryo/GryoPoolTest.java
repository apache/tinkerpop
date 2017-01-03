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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoX;
import org.apache.tinkerpop.gremlin.structure.io.IoXIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoY;
import org.apache.tinkerpop.gremlin.structure.io.IoYIoRegistry;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoPoolTest {

    @Test
    public void shouldDoWithReaderWriterMethods() throws Exception {
        final Configuration conf = new BaseConfiguration();
        final GryoPool pool = GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            pool.doWithWriter(writer -> writer.writeObject(os, 1));
            os.flush();
            try (final ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray())) {
                assertEquals(1, pool.<Integer>doWithReader(FunctionUtils.wrapFunction(reader -> reader.readObject(is, Integer.class))).intValue());
            }
        }
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), 1, Integer.class);
    }

    @Test
    public void shouldConfigPoolOnConstructionWithDefaults() throws Exception {
        final Configuration conf = new BaseConfiguration();
        final GryoPool pool = GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), 1, Integer.class);
    }

    @Test
    public void shouldConfigPoolOnConstructionWithPoolSizeOneAndNoIoRegistry() throws Exception {
        final Configuration conf = new BaseConfiguration();
        final GryoPool pool = GryoPool.build().poolSize(1).ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        final GryoReader reader = pool.takeReader();
        final GryoWriter writer = pool.takeWriter();

        pool.offerReader(reader);
        pool.offerWriter(writer);

        for (int ix = 0; ix < 100; ix++) {
            final GryoReader r = pool.takeReader();
            final GryoWriter w = pool.takeWriter();
            assertReaderWriter(w, r, 1, Integer.class);

            // should always return the same original instance
            assertEquals(reader, r);
            assertEquals(writer, w);

            pool.offerReader(r);
            pool.offerWriter(w);
        }
    }

    @Test
    public void shouldConfigPoolOnConstructionWithCustomIoRegistryConstructor() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(IoRegistry.IO_REGISTRY, IoXIoRegistry.ConstructorBased.class.getName());
        final GryoPool pool = GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new IoX("test"), IoX.class);
    }

    @Test
    public void shouldConfigPoolOnConstructionWithCustomIoRegistryInstance() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(IoRegistry.IO_REGISTRY, IoXIoRegistry.InstanceBased.class.getName());
        final GryoPool pool = GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new IoX("test"), IoX.class);
    }

    @Test
    public void shouldConfigPoolOnConstructionWithMultipleCustomIoRegistries() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(IoRegistry.IO_REGISTRY,
                IoXIoRegistry.InstanceBased.class.getName() + "," + IoYIoRegistry.InstanceBased.class.getName());
        final GryoPool pool = GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new IoX("test"), IoX.class);
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new IoY(100, 200), IoY.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldConfigPoolOnConstructionWithoutCustomIoRegistryAndFail() throws Exception {
        final Configuration conf = new BaseConfiguration();
        final GryoPool pool = GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new IoX("test"), IoX.class);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldConfigPoolOnConstructionWithoutBadIoRegistryAndFail() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(IoRegistry.IO_REGISTRY, "some.class.that.does.not.exist");
        GryoPool.build().ioRegistries(conf.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).create();
    }

    private static <T> void assertReaderWriter(final GryoWriter writer, final GryoReader reader, final T o,
                                               final Class<T> clazz) throws Exception{
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            writer.writeObject(os, o);
            os.flush();
            try (final ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray())) {
                assertEquals(o, reader.readObject(is, clazz));
            }
        }
    }
}
