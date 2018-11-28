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

import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.util.IoRegistryHelper;
import org.apache.tinkerpop.shaded.kryo.Kryo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Gryo objects are somewhat expensive to construct (given the dependency on Kryo), therefore this pool helps re-use
 * those objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GryoPool {

    public static final String CONFIG_IO_GRYO_POOL_SIZE = "gremlin.io.gryo.poolSize";
    public static final String CONFIG_IO_GRYO_VERSION = "gremlin.io.gryo.version";
    public static final int CONFIG_IO_GRYO_POOL_SIZE_DEFAULT = 256;
    public static final GryoVersion CONFIG_IO_GRYO_POOL_VERSION_DEFAULT = GryoVersion.V3_1;

    public enum Type {READER, WRITER, READER_WRITER}

    private Queue<GryoReader> gryoReaders;
    private Queue<GryoWriter> gryoWriters;
    private Queue<Kryo> kryos;
    private GryoMapper mapper;

    public static GryoPool.Builder build() {
        return new GryoPool.Builder();
    }

    /**
     * Used by {@code GryoPool.Builder}.
     */
    private GryoPool() {
    }

    public GryoMapper getMapper() {
        return mapper;
    }

    public Kryo takeKryo() {
        final Kryo kryo = kryos.poll();
        return null == kryo ? mapper.createMapper() : kryo;
    }

    public GryoReader takeReader() {
        final GryoReader reader = this.gryoReaders.poll();
        return null == reader ? GryoReader.build().mapper(mapper).create() : reader;
    }

    public GryoWriter takeWriter() {
        final GryoWriter writer = this.gryoWriters.poll();
        return null == writer ? GryoWriter.build().mapper(mapper).create() : writer;
    }

    public void offerKryo(final Kryo kryo) {
        kryos.offer(kryo);
    }

    public void offerReader(final GryoReader gryoReader) {
        gryoReaders.offer(gryoReader);
    }

    public void offerWriter(final GryoWriter gryoWriter) {
        gryoWriters.offer(gryoWriter);
    }

    public <A> A readWithKryo(final Function<Kryo, A> kryoFunction) {
        final Kryo kryo = takeKryo();
        final A a = kryoFunction.apply(kryo);
        offerKryo(kryo);
        return a;
    }

    public void writeWithKryo(final Consumer<Kryo> kryoConsumer) {
        final Kryo kryo = takeKryo();
        kryoConsumer.accept(kryo);
        offerKryo(kryo);
    }

    public <A> A doWithReader(final Function<GryoReader, A> readerFunction) {
        final GryoReader gryoReader = takeReader();
        final A a = readerFunction.apply(gryoReader);
        offerReader(gryoReader);
        return a;
    }

    public void doWithWriter(final Consumer<GryoWriter> writerFunction) {
        final GryoWriter gryoWriter = takeWriter();
        writerFunction.accept(gryoWriter);
        offerWriter(gryoWriter);
    }

    private void createPool(final int poolSize, final Type type, final GryoMapper gryoMapper) {
        this.mapper = gryoMapper;
        if (type.equals(Type.READER) || type.equals(Type.READER_WRITER)) {
            gryoReaders = new LinkedBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++) {
                gryoReaders.add(GryoReader.build().mapper(gryoMapper).create());
            }
        }
        if (type.equals(Type.WRITER) || type.equals(Type.READER_WRITER)) {
            gryoWriters = new LinkedBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++) {
                gryoWriters.add(GryoWriter.build().mapper(gryoMapper).create());
            }
        }

        kryos = new LinkedBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            kryos.add(gryoMapper.createMapper());
        }
    }

    ////

    public static class Builder {

        private int poolSize = 256;
        private List<IoRegistry> ioRegistries = new ArrayList<>();
        private Type type = Type.READER_WRITER;
        private Consumer<GryoMapper.Builder> gryoMapperConsumer = null;
        private GryoVersion version = GryoVersion.V1_0;

        /**
         * Set the version of Gryo to use for this pool.
         */
        public Builder version(final GryoVersion version) {
            this.version = version;
            return this;
        }

        /**
         * The {@code IoRegistry} class names to use for the {@code GryoPool}
         *
         * @param ioRegistryClassNames a list of class names
         * @return the update builder
         */
        public Builder ioRegistries(final List<Object> ioRegistryClassNames) {
            this.ioRegistries.addAll(IoRegistryHelper.createRegistries(ioRegistryClassNames));
            return this;
        }

        /**
         * The {@code IoRegistry} class name to use for the {@code GryoPool}
         *
         * @param ioRegistryClassName a class name
         * @return the update builder
         */
        public Builder ioRegistry(final Object ioRegistryClassName) {
            this.ioRegistries.addAll(IoRegistryHelper.createRegistries(Collections.singletonList(ioRegistryClassName)));
            return this;
        }

        /**
         * The size of the {@code GryoPool}. The size can not be changed once created.
         *
         * @param poolSize the pool size
         * @return the updated builder
         */
        public Builder poolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        /**
         * The type of {@code GryoPool} to support -- see {@code Type}
         *
         * @param type the pool type
         * @return the updated builder
         */
        public Builder type(final Type type) {
            this.type = type;
            return this;
        }

        /**
         * A consumer to update the {@code GryoMapper.Builder} once constructed.
         *
         * @param gryoMapperConsumer the {@code GryoMapper.Builder} consumer
         * @return the updated builder
         */
        public Builder initializeMapper(final Consumer<GryoMapper.Builder> gryoMapperConsumer) {
            this.gryoMapperConsumer = gryoMapperConsumer;
            return this;
        }

        /**
         * Create the {@code GryoPool} from this builder.
         *
         * @return the new pool
         */
        public GryoPool create() {
            final GryoMapper.Builder mapper = GryoMapper.build().version(version);
            final GryoPool gryoPool = new GryoPool();
            if (null != this.ioRegistries)
                this.ioRegistries.forEach(mapper::addRegistry);
            if (null != this.gryoMapperConsumer)
                this.gryoMapperConsumer.accept(mapper);
            gryoPool.createPool(this.poolSize, this.type, mapper.create());
            return gryoPool;
        }
    }
}