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
package org.apache.tinkerpop.machines.pipes;

import org.apache.tinkerpop.gremlin.machine.Traverser;
import org.apache.tinkerpop.gremlin.machine.functions.GFunction;
import org.apache.tinkerpop.gremlin.machine.traversers.TraverserSet;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Step<S, E> implements Iterator<Traverser<?, E>> {

    private final GFunction<?, S, E> function;
    private TraverserSet<?, S> set = new TraverserSet<>();

    public Step(final GFunction<?, S, E> function) {
        this.function = function;
    }

    public void addStart(final Traverser<?, S> start) {
        this.set.add((Traverser) start);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Traverser<?, E> next() {
        return null;
    }
}
