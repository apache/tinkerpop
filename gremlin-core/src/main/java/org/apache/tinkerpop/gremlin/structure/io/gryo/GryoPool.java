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
import org.apache.tinkerpop.shaded.kryo.Kryo;

import java.lang.reflect.Method;
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
    public static final String CONFIG_IO_REGISTRY = "gremlin.io.registry";
    public static final String CONFIG_IO_GRYO_POOL_SIZE = "gremlin.io.gryo.poolSize";

    public enum Type {READER, WRITER, READER_WRITER}

    private Queue<GryoReader> gryoReaders;
    private Queue<GryoWriter> gryoWriters;
    private final GryoMapper mapper;

    public GryoPool(final Configuration conf, final Consumer<GryoMapper.Builder> builderConsumer, final Consumer<Kryo> kryoConsumer) {
        final GryoMapper.Builder mapperBuilder = GryoMapper.build();
        tryCreateIoRegistry(conf.getList(CONFIG_IO_REGISTRY, Collections.<IoRegistry>emptyList())).forEach(mapperBuilder::addRegistry);
        builderConsumer.accept(mapperBuilder);
        // should be able to re-use the GryoMapper - it creates fresh kryo instances from its createMapper method
        this.mapper = mapperBuilder.create();
        this.createPool(conf.getInt(CONFIG_IO_GRYO_POOL_SIZE, 256), Type.READER_WRITER, this.mapper);
        for (final GryoReader reader : this.gryoReaders) {
            kryoConsumer.accept(reader.getKryo());
        }
        for (final GryoWriter writer : this.gryoWriters) {
            kryoConsumer.accept(writer.getKryo());
        }

    }

    /**
     * Create a pool of readers and writers from a {@code Configuration} object.  There are two configuration keys
     * expected: "gremlin.io.registry" which defines comma separated list of the fully qualified class names of
     * {@link IoRegistry} implementations to use and the "gremlin.io.gryo.poolSize" which defines the initial size
     * of the {@code GryoPool}.  As with usage of {@link GryoMapper.Builder#addRegistry(IoRegistry)}, the order in
     * which these items are added matters greatly.  The order used for writing should be the order used for reading.
     */
    public GryoPool(final Configuration conf) {
        this(conf.getInt(CONFIG_IO_GRYO_POOL_SIZE, 256), Type.READER_WRITER,
                tryCreateIoRegistry(conf.getList(CONFIG_IO_REGISTRY, Collections.emptyList())));
    }

    /**
     * Create a pool of readers and writers of specified size and use the default {@link GryoMapper} (which means
     * that custom serializers from vendors will not be applied.
     *
     * @param poolSize initial size of the pool.
     */
    public GryoPool(final int poolSize) {
        this(poolSize, Type.READER_WRITER, Collections.emptyList());
    }

    /**
     * Create a pool of a readers, writers or both of the specified size with an optional {@link IoRegistry} object
     * which would allow custom serializers to be registered to the pool.
     *
     * @param poolSize   initial size of the pool.
     * @param type       the type of pool.
     * @param registries a list of registries to assign to each {@link GryoReader} and {@link GryoWriter} instances.
     */
    public GryoPool(final int poolSize, final Type type, final List<IoRegistry> registries) {
        final GryoMapper.Builder mapperBuilder = GryoMapper.build();
        registries.forEach(mapperBuilder::addRegistry);
        // should be able to re-use the GryoMapper - it creates fresh kryo instances from its createMapper method
        this.mapper = mapperBuilder.create();
        createPool(poolSize, type, mapper);
    }

    private void createPool(final int poolSize, final Type type, final GryoMapper gryoMapper) {
        if (type.equals(Type.READER) || type.equals(Type.READER_WRITER)) {
            this.gryoReaders = new LinkedBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++) {
                this.gryoReaders.add(GryoReader.build().mapper(gryoMapper).create());
            }
        }
        if (type.equals(Type.WRITER) || type.equals(Type.READER_WRITER)) {
            this.gryoWriters = new LinkedBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++) {
                this.gryoWriters.add(GryoWriter.build().mapper(gryoMapper).create());
            }
        }
    }

    public GryoMapper getMapper() {
        return this.mapper;
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

    private static List<IoRegistry> tryCreateIoRegistry(final List<Object> classNames) {
        if (classNames.isEmpty()) return Collections.emptyList();

        final List<IoRegistry> registries = new ArrayList<>();
        classNames.forEach(c -> {
            try {
                final String className = c.toString();
                final Class<?> clazz = Class.forName(className);
                try {
                    final Method instanceMethod = clazz.getDeclaredMethod("getInstance");
                    if (IoRegistry.class.isAssignableFrom(instanceMethod.getReturnType()))
                        registries.add((IoRegistry) instanceMethod.invoke(null));
                    else
                        throw new Exception();
                } catch (Exception methodex) {
                    // tried getInstance() and that failed so try newInstance() no-arg constructor
                    registries.add((IoRegistry) clazz.newInstance());
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
        return registries;
    }
}
