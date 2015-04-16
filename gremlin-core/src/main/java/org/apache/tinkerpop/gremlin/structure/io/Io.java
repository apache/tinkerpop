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

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Io<R extends GraphReader.ReaderBuilder, W extends GraphWriter.WriterBuilder, M extends Mapper.Builder> {

    /**
     * Creates a {@link GraphReader.ReaderBuilder} implementation . Implementers should call the
     * {@link #mapper()} function to feed its result to the builder.  In this way, custom class serializers
     * registered to the {@link Mapper.Builder} by {@link Graph} implementations will end up being used for
     * the serialization process.
     */
    public R reader();

    /**
     * Creates a {@link GraphWriter.WriterBuilder} implementation . Implementers should call the
     * {@link #mapper()} function to feed its result to the builder.  In this way, custom class serializers
     * registered to the {@link Mapper.Builder} by {@link Graph} implementations will end up being used for
     * the serialization process.
     */
    public W writer();

    /**
     * Constructs a {@link Mapper.Builder} which is responsible for constructing the abstraction over different
     * serialization methods.  Implementations should set defaults as required, but most importantly need to
     * make the appropriate call to {@link Mapper.Builder#addRegistry(IoRegistry)} which will provide the
     * builder with any required custom serializers of the {@link Graph}.
     */
    public M mapper();

    /**
     * Write a {@link Graph} to file using the default configuration of the {@link #writer()} and its supplied
     * {@link #mapper()}.
     */
    public void write(final String file) throws IOException;

    /**
     * Read a {@link Graph} from file using the default configuration of the {@link #reader()} and its supplied
     * {@link #mapper()}.
     */
    public void read(final String file) throws IOException;

    public interface Builder<I extends Io> {
        public Builder<? extends Io> registry(final IoRegistry registry);
        public Builder<? extends Io> graph(final Graph g);
        public I create();
    }
}
