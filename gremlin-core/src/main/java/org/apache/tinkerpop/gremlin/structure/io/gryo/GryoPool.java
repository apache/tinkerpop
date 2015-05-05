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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

import java.util.Optional;
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
public class GryoPool {
    public static final String CONFIG_IO_REGISTRY = "gremlin.io.registry";
    public static final String CONFIG_IO_GRYO_POOL_SIZE = "gremlin.io.gryo.poolSize";

    public enum Type {READER, WRITER, READER_WRITER}

    private Queue<GryoReader> gryoReaders;
    private Queue<GryoWriter> gryoWriters;
    private final GryoMapper mapper;

    public GryoPool(final Configuration conf) {
        this(conf.getInt(CONFIG_IO_GRYO_POOL_SIZE, 256), Type.READER_WRITER, tryCreateIoRegistry(conf.getString(CONFIG_IO_REGISTRY, "")));
    }

    /**
     * Create a pool of readers and writers of specified size and use the default {@link GryoMapper} (which means
     * that custom serializers from vendors will not be applied.
     *
     * @param poolSize size of the pool.
     */
    public GryoPool(final int poolSize) {
        this(poolSize, Type.READER_WRITER, Optional.empty());
    }

    /**
     * Create a pool of a readers, writers or both of the specified size with an optional {@link IoRegistry} object
     * which would allow custom serializers to be registered to the pool.
     *
     * @param poolSize size of the pool.
     * @param type the type of pool.
     * @param ioRegistry the registry to assign to each {@link GryoReader} and {@link GryoWriter} instances.
     */
    public GryoPool(final int poolSize, final Type type, final Optional<IoRegistry> ioRegistry) {
        final GryoMapper.Builder mapperBuilder = GryoMapper.build();
        ioRegistry.ifPresent(mapperBuilder::addRegistry);

        // should be able to re-use the GryoMapper - it creates fresh kryo instances from its createMapper method
        mapper = mapperBuilder.create();
        if (type.equals(Type.READER) || type.equals(Type.READER_WRITER)) {
            this.gryoReaders = new LinkedBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++) {
                this.gryoReaders.add(GryoReader.build().mapper(mapper).create());
            }
        }
        if (type.equals(Type.WRITER) || type.equals(Type.READER_WRITER)) {
            this.gryoWriters = new LinkedBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++) {
                this.gryoWriters.add(GryoWriter.build().mapper(mapper).create());
            }
        }
    }

    public GryoReader takeReader() {
        final GryoReader reader = this.gryoReaders.poll();
        return null == reader ? GryoReader.build().mapper(mapper).create() : reader;
    }

    public GryoWriter takeWriter() {
        final GryoWriter writer = this.gryoWriters.poll();
        return null == writer ? GryoWriter.build().mapper(mapper).create() : writer;
    }

    public void offerReader(final GryoReader gryoReader) {
        this.gryoReaders.offer(gryoReader);
    }

    public void offerWriter(final GryoWriter gryoWriter) {
        this.gryoWriters.offer(gryoWriter);
    }

    public <A> A doWithReader(final Function<GryoReader, A> readerFunction) {
        final GryoReader gryoReader = this.takeReader();
        final A a = readerFunction.apply(gryoReader);
        this.offerReader(gryoReader);
        return a;
    }

    public void doWithWriter(final Consumer<GryoWriter> writerFunction) {
        final GryoWriter gryoWriter = this.takeWriter();
        writerFunction.accept(gryoWriter);
        this.offerWriter(gryoWriter);
    }

    private static Optional<IoRegistry> tryCreateIoRegistry(final String className) {
        if (className.isEmpty()) return Optional.empty();

        try {
            final Class clazz = Class.forName(className);
            return Optional.of((IoRegistry) clazz.newInstance());
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
