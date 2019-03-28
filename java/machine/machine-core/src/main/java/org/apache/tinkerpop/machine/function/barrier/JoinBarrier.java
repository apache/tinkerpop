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
package org.apache.tinkerpop.machine.function.barrier;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Argument;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JoinBarrier<C, K, V> extends AbstractFunction<C> implements BarrierFunction<C, Map<K, V>, Map<K, V>, List<Map<K, V>>> {

    private final CoreCompiler.Symbols.Tokens joinType;
    private final Compilation<C, Map<K, V>, Map<K, V>> joinCompilation;
    private final Argument<K> joinKey;

    private JoinBarrier(final Coefficient<C> coefficient, final String label,
                        CoreCompiler.Symbols.Tokens joinType,
                        final Compilation<C, Map<K, V>, Map<K, V>> joinCompilation,
                        final Argument<K> joinKey) {
        super(coefficient, label);
        this.joinType = joinType;
        this.joinCompilation = joinCompilation;
        this.joinKey = joinKey;
    }

    @Override
    public List<Map<K, V>> getInitialValue() {
        return new ArrayList<>();
    }

    @Override
    public List<Map<K, V>> merge(final List<Map<K, V>> barrierA, final List<Map<K, V>> barrierB) {
        barrierA.addAll(barrierB);
        return barrierA; // TODO: unchecked in distributed Beam .. may be completely off
    }

    @Override
    public Iterator<Map<K, V>> createIterator(final List<Map<K, V>> barrier) {
        return barrier.iterator();
    }

    @Override
    public boolean returnsTraversers() {
        return false;
    }

    @Override
    public List<Map<K, V>> apply(final Traverser<C, Map<K, V>> traverser, final List<Map<K, V>> barrier) {
        this.joinCompilation.flatMapTraverser(traverser).forEachRemaining(other -> {
            final K key = this.joinKey.mapArg(traverser);
            if (traverser.object().get(key).equals(other.object().get(key))) {
                final Map<K, V> join = new HashMap<>();
                for (final Map.Entry<K, V> entry : traverser.object().entrySet()) {
                    join.put(entry.getKey(), entry.getValue());
                }
                for (final Map.Entry<K, V> entry : other.object().entrySet()) {
                    join.put(entry.getKey(), entry.getValue());
                }
                barrier.add(join);
            }
        });
        return barrier;
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.joinType, this.joinCompilation, this.joinKey);
    }

    public static <C, K, V> JoinBarrier<C, K, V> compile(final Instruction<C> instruction) {
        return new JoinBarrier<>(instruction.coefficient(), instruction.label(),
                CoreCompiler.Symbols.Tokens.valueOf((String) instruction.args()[0]),
                Compilation.compile(instruction.args()[1]),
                Argument.create(instruction.args()[2]));
    }

}
