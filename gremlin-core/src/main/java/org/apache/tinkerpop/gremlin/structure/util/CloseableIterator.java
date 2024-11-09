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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An extension of {@code Iterator} that implements {@code AutoCloseable} which allows a {@link Graph} implementation
 * that hold open resources to provide the user the option to release those resources.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    /**
     * Wraps an existing {@code Iterator} in a {@code CloseableIterator}. If the {@code Iterator} is already of that
     * type then it will simply be returned as-is.
     */
    static <T> CloseableIterator<T> of(final Iterator<T> iterator) {
        if (iterator instanceof CloseableIterator)
            return (CloseableIterator<T>) iterator;

        return new DefaultCloseableIterator<T>(iterator);
    }

    @Override
    default void close() {
        // do nothing by default
    }

    static <T> void closeIterator(final Iterator<T> iterator) {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static <T> CloseableIterator<T> empty() {
        return EmptyCloseableIterator.instance();
    }

    class EmptyCloseableIterator<T> extends DefaultCloseableIterator<T> {

        private static final EmptyCloseableIterator INSTANCE = new EmptyCloseableIterator();

        public static <T> EmptyCloseableIterator<T> instance() {
            return INSTANCE;
        }

        private EmptyCloseableIterator() {
            super(EmptyIterator.instance());
        }

    }
}
