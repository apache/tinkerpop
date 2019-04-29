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
package org.apache.tinkerpop.machine.function.flatmap;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.structure.TTuple;
import org.apache.tinkerpop.machine.structure.util.T2Tuple;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EntriesFlatMap<C, K, V> extends AbstractFunction<C> implements FlatMapFunction<C, TTuple<K, V>, T2Tuple<K, V>> {


    private EntriesFlatMap(final Coefficient<C> coefficient, final String label) {
        super(coefficient, label);
    }

    @Override
    public Iterator<T2Tuple<K, V>> apply(final Traverser<C, TTuple<K, V>> traverser) {
        return traverser.object().entries();
    }

    @Override
    public EntriesFlatMap<C, K, V> clone() {
        return (EntriesFlatMap<C, K, V>) super.clone();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this);
    }

    public static <C, K, V> EntriesFlatMap<C, K, V> compile(final Instruction<C> instruction) {
        return new EntriesFlatMap<>(instruction.coefficient(), instruction.label());
    }
}