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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoPool {

    private final Queue<GryoReader> gryoReaders;
    private final Queue<GryoWriter> gryoWriters;
    private static final Integer MAX_QUEUE_SIZE = 256;

    public GryoPool() {
        this.gryoReaders = new ConcurrentLinkedQueue<>();
        this.gryoWriters = new ConcurrentLinkedQueue<>();
    }

    public GryoReader takeReader() {
        final GryoReader reader = this.gryoReaders.poll();
        return (null == reader) ? GryoReader.build().create() : reader;
    }

    public GryoWriter takeWriter() {
        final GryoWriter writer = this.gryoWriters.poll();
        return (null == writer) ? GryoWriter.build().create() : writer;
    }

    public void offerReader(final GryoReader gryoReader) {
        if (this.gryoReaders.size() < MAX_QUEUE_SIZE)
            this.gryoReaders.offer(gryoReader);
    }

    public void offerWriter(final GryoWriter gryoWriter) {
        if (this.gryoWriters.size() < MAX_QUEUE_SIZE)
            this.gryoWriters.offer(gryoWriter);
    }

    public <A> A doWithReaderWriter(final BiFunction<GryoReader, GryoWriter, A> readerWriterBiFunction) {
        final GryoReader gryoReader = this.takeReader();
        final GryoWriter gryoWriter = this.takeWriter();
        final A a = readerWriterBiFunction.apply(gryoReader, gryoWriter);
        this.offerReader(gryoReader);
        this.offerWriter(gryoWriter);
        return a;
    }

    public <A> A doWithReader(final Function<GryoReader, A> readerFunction) {
        final GryoReader gryoReader = this.takeReader();
        final A a = readerFunction.apply(gryoReader);
        this.offerReader(gryoReader);
        return a;
    }

    public <A> A doWithWriter(final Function<GryoWriter, A> writerFunction) {
        final GryoWriter gryoWriter = this.takeWriter();
        final A a = writerFunction.apply(gryoWriter);
        this.offerWriter(gryoWriter);
        return a;
    }
}
