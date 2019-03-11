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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.functions.FlatMapFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapStep<C, S, E> extends AbstractStep<C, S, E> {

    private final FlatMapFunction<C, S, E> flatMapFunction;
    private Iterator<Traverser<C, E>> iterator = Collections.emptyIterator();

    public FlatMapStep(final AbstractStep<C, ?, S> previousStep, final FlatMapFunction<C, S, E> flatMapFunction) {
        super(previousStep, flatMapFunction);
        this.flatMapFunction = flatMapFunction;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext() || super.hasNext();
    }

    @Override
    public Traverser<C, E> next() {
        if (!this.iterator.hasNext()) {
            this.iterator = super.getPreviousTraverser().flatMap(this.flatMapFunction);
        }
        return this.iterator.next();
    }
}