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
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoPoolTest {

    @Test
    public void shouldDoWithReaderWriterMethods() throws Exception {
        final Configuration conf = new BaseConfiguration();
        final GryoPool pool = new GryoPool(conf);
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
        final GryoPool pool = new GryoPool(conf);
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), 1, Integer.class);
    }

    @Test
    public void shouldConfigPoolOnConstructionWithPoolSizeOneAndNoIoRegistry() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(GryoPool.CONFIG_IO_GRYO_POOL_SIZE, 1);
        final GryoPool pool = new GryoPool(conf);
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
    public void shouldConfigPoolOnConstructionWithCustomIoRegistry() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(GryoPool.CONFIG_IO_REGISTRY, CustomIoRegistry.class.getName());
        final GryoPool pool = new GryoPool(conf);
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new Junk("test"), Junk.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldConfigPoolOnConstructionWithoutCustomIoRegistryAndFail() throws Exception {
        final Configuration conf = new BaseConfiguration();
        final GryoPool pool = new GryoPool(conf);
        assertReaderWriter(pool.takeWriter(), pool.takeReader(), new Junk("test"), Junk.class);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldConfigPoolOnConstructionWithoutBadIoRegistryAndFail() throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(GryoPool.CONFIG_IO_REGISTRY, "some.class.that.does.not.exist");
        new GryoPool(conf);
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

    public static class Junk {
        private String x;

        private Junk() {}

        public Junk(final String x) {
            this.x = x;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Junk junk = (Junk) o;

            return x.equals(junk.x);

        }

        @Override
        public int hashCode() {
            return x.hashCode();
        }
    }

    public static class CustomIoRegistry extends AbstractIoRegistry {
        public CustomIoRegistry() {
            register(GryoIo.class, Junk.class, null);
        }
    }
}
