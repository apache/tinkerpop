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
package org.apache.tinkerpop.machine.processor.rxjava;

import io.reactivex.functions.Function;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class BarrierFlow<C, S, E, B> implements Function<B, Iterable<Traverser<C, E>>> {

    private final BarrierFunction<C, S, E, B> function;
    private final TraverserFactory traverserFactory;

    BarrierFlow(final BarrierFunction<C, S, E, B> function, final TraverserFactory traverserFactory) {
        this.function = function;
        this.traverserFactory = traverserFactory;
    }


    @Override
    public Iterable<Traverser<C, E>> apply(final B barrier) {
        final Iterator<Traverser<C, E>> iterator = this.function.returnsTraversers() ?
                (Iterator<Traverser<C, E>>) this.function.createIterator(barrier) :
                IteratorUtils.map(this.function.createIterator(barrier), e -> this.traverserFactory.create(this.function, e));
        return () -> iterator;
    }
}
