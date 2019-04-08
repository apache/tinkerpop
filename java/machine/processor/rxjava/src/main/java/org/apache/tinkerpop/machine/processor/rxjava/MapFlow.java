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
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class MapFlow<C, S, E> implements Function<Traverser<C, S>, Traverser<C, E>> {

    private final ThreadLocal<MapFunction<C, S, E>> mapFunction;

    MapFlow(final MapFunction<C, S, E> mapFunction) {
        this.mapFunction = ThreadLocal.withInitial(mapFunction::clone);
    }

    @Override
    public Traverser<C, E> apply(final Traverser<C, S> traverser) {
        return traverser.map(this.mapFunction.get());
    }
}
