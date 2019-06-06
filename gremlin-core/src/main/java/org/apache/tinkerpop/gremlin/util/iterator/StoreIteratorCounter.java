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
package org.apache.tinkerpop.gremlin.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class which can be used by providers to keep a count of the number of
 * open iterators to the underlying storage. Please note that, by default, this does
 * not maintain the count unless explicitly plugged-in by the provider implementation.
 * <p>
 * As an example on how to plugin-in the counter in the provider implementation
 * check TinkerGraphIterator.
 *
 * @see Traversal#close()
 */
public class StoreIteratorCounter {
    public static final StoreIteratorCounter INSTANCE = new StoreIteratorCounter();

    private AtomicLong openIteratorCount = new AtomicLong(0);

    public void reset() {
        openIteratorCount.set(0);
    }

    public long getOpenIteratorCount() {
        return openIteratorCount.get();
    }

    public void increment() {
        openIteratorCount.incrementAndGet();
    }

    public void decrement() {
        openIteratorCount.decrementAndGet();
    }
}
