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
import java.util.function.Consumer;

/**
 * Ties together the core interfaces of an IO format: {@link GraphReader}, {@link GraphWriter} and {@link Mapper}.
 * The {@link Builder} of an {@code Io} instance is supplied to {@link Graph#io(Io.Builder)} and the {@link Graph}
 * implementation can then chose to supply an {@link IoRegistry} to it before returning it.  An {@link Io}
 * implementation should use that {@link IoRegistry} to lookup custom serializers to use and register them to the
 * internal {@link Mapper} (if the format has such capability).
 *
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
    public void writeGraph(final String file) throws IOException;

    /**
     * Read a {@link Graph} from file using the default configuration of the {@link #reader()} and its supplied
     * {@link #mapper()}.
     */
    public void readGraph(final String file) throws IOException;

    public static class Exceptions {
        public static UnsupportedOperationException readerFormatIsForFullGraphSerializationOnly(final Class<? extends GraphReader> clazz) {
            return new UnsupportedOperationException(String.format("%s only reads an entire Graph", clazz));
        }

        public static UnsupportedOperationException writerFormatIsForFullGraphSerializationOnly(final Class<? extends GraphWriter> clazz) {
            return new UnsupportedOperationException(String.format("%s only writes an entire Graph", clazz));
        }
    }

    /**
     * Helps to construct an {@link Io} implementation and should be implemented by every such implementation as
     * that class will be passed to {@link Graph#io(Io.Builder)} by the user.
     */
    public interface Builder<I extends Io> {
        /**
         * Allows a {@link Graph} implementation to have full control over the {@link Mapper.Builder} instance.
         * Typically, the implementation will just pass in its {@link IoRegistry} implementation so that the
         * {@link Mapper} that gets built will have knowledge of any custom classes and serializers it may have.
         * <p/>
         * End-users should not use this method directly.  If a user wants to register custom serializers, then such
         * things can be done via calls to {@link Io#mapper()} after the {@link Io} is constructed via
         * {@link Graph#io(Io.Builder)}.
         */
        public Builder<? extends Io> onMapper(final Consumer<Mapper.Builder> onMapper);

        /**
         * Providers use this method to supply the current instance of their {@link Graph} to the builder.  End-users
         * should not call this method directly.
         */
        public Builder<? extends Io> graph(final Graph g);

        /**
         * Determines if the version matches the one configured for this builder. Graph providers can use this in
         * calls to {@link Graph#io(Builder)} to figure out the correct versions of registries to add.
         */
        public <V> boolean requiresVersion(final V version);

        /**
         * Providers call this method in the {@link Graph#io(Io.Builder)} method to construct the {@link Io} instance
         * and return the value.  End-users will typically not call this method.
         */
        public I create();
    }
}
