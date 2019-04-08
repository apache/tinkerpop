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
import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class FlatMapFlow<C, S, E> implements Function<Traverser<C, S>, Iterable<Traverser<C, E>>> {

    private ThreadLocal<FlatMapFunction<C, S, E>> flatMapFunction;

    FlatMapFlow(final FlatMapFunction<C, S, E> flatMapFunction) {
        this.flatMapFunction = ThreadLocal.withInitial(flatMapFunction::clone);
    }

    @Override
    public Iterable<Traverser<C, E>> apply(final Traverser<C, S> traverser) {
        return () -> traverser.flatMap(this.flatMapFunction.get());
    }
}
