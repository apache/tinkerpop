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

import java.util.List;

/**
 * Represents a low-level serialization class that can be used to map classes to serializers.  These implementation
 * create instances of serializers from other libraries (e.g. creating a {@code Kryo} instance).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Mapper<T> {
    /**
     * Create a new instance of the internal object mapper that an implementation represents.
     */
    public T createMapper();

    /**
     * Largely a marker interface for builders that construct {@link Mapper} instances.
     */
    public interface Builder<B extends Builder> {

        /**
         * Adds a vendor supplied {@link IoRegistry} to the {@code Mapper.Builder} which enables it to check for
         * vendor custom serializers to add to the {@link Mapper}.  All {@link Io} implementations should expose
         * this method via this {@link Builder} so that it is compatible with {@link Graph#io}. Successive calls
         * to this method will add multiple registries.  Registry order must be respected when doing so.  In
         * other words, data written with {@link IoRegistry} {@code A} added first and {@code B} second must be read
         * by a {@code Mapper} with that same registry ordering.  Attempting to add {@code B} before {@code A} will
         * result in errors.
         */
        public B addRegistry(final IoRegistry registry);

        /**
         * Adds a vendor supplied {@link IoRegistry} to the {@code Mapper.Builder} which enables it to check for
         * vendor custom serializers to add to the {@link Mapper}.  All {@link Io} implementations should expose
         * this method via this {@link Builder} so that it is compatible with {@link Graph#io}. Successive calls
         * to this method will add multiple registries.  Registry order must be respected when doing so.  In
         * other words, data written with {@link IoRegistry} {@code A} added first and {@code B} second must be read
         * by a {@code Mapper} with that same registry ordering.  Attempting to add {@code B} before {@code A} will
         * result in errors.
         */
        public default B addRegistries(final List<IoRegistry> registries) {
            B b = (B) this;
            for (final IoRegistry registry : registries) {
                b = this.addRegistry(registry);
            }
            return b;
        }
    }
}
