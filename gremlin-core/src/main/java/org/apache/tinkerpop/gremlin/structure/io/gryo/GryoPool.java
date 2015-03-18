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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoPool {

    private final Queue<GryoReader> gryoReaders;
    private final Queue<GryoWriter> gryoWriters;

    public GryoPool(final int poolSize) {
        this.gryoReaders = new LinkedBlockingQueue<>(poolSize);
        this.gryoWriters = new LinkedBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            this.gryoReaders.add(GryoReader.build().create());
            this.gryoWriters.add(GryoWriter.build().create());
        }
    }

    public GryoReader takeReader() {
        final GryoReader reader = this.gryoReaders.poll();
        return null == reader ? GryoReader.build().create() : reader;
    }

    public GryoWriter takeWriter() {
        final GryoWriter writer = this.gryoWriters.poll();
        return null == writer ? GryoWriter.build().create() : writer;
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
}
