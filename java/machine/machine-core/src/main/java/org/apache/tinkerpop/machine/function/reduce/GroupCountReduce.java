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
package org.apache.tinkerpop.machine.function.reduce;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.structure.data.JMap;
import org.apache.tinkerpop.machine.structure.data.TMap;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountReduce<C, S, E> extends AbstractFunction<C> implements ReduceFunction<C, S, TMap<E, Long>> {

    private final Compilation<C, S, E> byCompilation;

    private GroupCountReduce(final Coefficient<C> coefficient, final String label, final Compilation<C, S, E> byCompilation) {
        super(coefficient, label);
        this.byCompilation = byCompilation;
    }

    @Override
    public TMap<E, Long> apply(final Traverser<C, S> traverser, final TMap<E, Long> currentValue) {
        final E object = null == this.byCompilation ? (E) traverser.object() : this.byCompilation.mapObject(traverser.object()).object();
        currentValue.set(object, traverser.coefficient().count() + currentValue.get(object, 0L));
        return currentValue;
    }

    @Override
    public TMap<E, Long> merge(final TMap<E, Long> valueA, final TMap<E, Long> valueB) {
        final Map<E, Long> newMap = new HashMap<>();
        valueA.entries().forEachRemaining(entry -> newMap.put(entry.getA(), entry.getB()));
        valueB.entries().forEachRemaining(entry -> newMap.put(entry.getA(), entry.getB()));
        return new JMap<>(newMap);
    }

    @Override
    public TMap<E, Long> getInitialValue() {
        return new JMap<>(new HashMap<>());
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.byCompilation);
    }

    public static <C, S, E> GroupCountReduce<C, S, E> compile(final Instruction<C> instruction) {
        return new GroupCountReduce<>(instruction.coefficient(), instruction.label(), Compilation.compileOrNull(0, instruction.args()));
    }

}
