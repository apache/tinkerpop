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
package org.apache.tinkerpop.machine.traversers;

import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.functions.FlatMapFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.reduce.Reducer;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traverser<C, S> extends Serializable {

    public Coefficient<C> coefficient();

    public S object();

    public Path path();

    public void addLabel(final String label);

    public default void addLabels(final Set<String> labels) {
        for (final String label : labels) {
            this.addLabel(label);
        }
    }

    public default boolean filter(final FilterFunction<C, S> function) {
        if (function.test(this)) {
            this.coefficient().multiply(function.coefficient().value());
            this.addLabels(function.labels());
            return true;
        } else {
            return false;
        }
    }

    public default <E> Traverser<C, E> map(final MapFunction<C, S, E> function) {
        final Coefficient<C> eCoefficient = this.coefficient().clone().multiply(function.coefficient().value());
        final E eObject = function.apply(this);
        final Traverser<C, E> traverser = this.split(eCoefficient, eObject);
        traverser.addLabels(function.labels());
        return traverser;
    }

    public default <E> Iterator<Traverser<C, E>> flatMap(final FlatMapFunction<C, S, E> function) {
        return Collections.emptyIterator();
    }

    //public default void sideEffect(final SideEffectFunction<C,S> function);

    public default <E> Traverser<C, E> reduce(final Reducer<E> reducer) {
        final Traverser<C, E> traverser = this.split(this.coefficient().clone().unity(), reducer.get());
        return traverser;
    }

    public <E> Traverser<C, E> split(final Coefficient<C> coefficient, final E object);
}
