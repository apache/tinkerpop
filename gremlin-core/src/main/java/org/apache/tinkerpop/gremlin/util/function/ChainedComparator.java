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
package org.apache.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChainedComparator<T> implements Comparator<T>, Serializable {

    private final List<Comparator<T>> comparators;
    private transient Comparator<T> chain;

    public ChainedComparator(final List<Comparator<T>> comparators) {
        if (comparators.isEmpty())
            throw new IllegalArgumentException("A chained comparator requires at least one comparator");
        this.comparators = comparators;
    }

    @Override
    public int compare(final T objectA, final T objectB) {
        if (null == this.chain) this.chain = this.comparators.stream().reduce((a, b) -> a.thenComparing(b)).get();
        return this.chain.compare(objectA, objectB);
    }
}
