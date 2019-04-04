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
package org.apache.tinkerpop.machine.processor.beam;

import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class FlatMapFn<C, S, E> extends AbstractFn<C, S, E> {

    private FlatMapFunction<C, S, E> flatMapFunction;

    FlatMapFn(final FlatMapFunction<C, S, E> flatMapFunction) {
        super(flatMapFunction);
        this.flatMapFunction = flatMapFunction;
    }

    @ProcessElement
    public void processElement(final @Element Traverser<C, S> traverser, final OutputReceiver<Traverser<C, E>> output) {
        final Iterator<Traverser<C, E>> iterator = traverser.flatMap(this.flatMapFunction);
        while (iterator.hasNext())
            output.output(iterator.next());
    }
}