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
package org.apache.tinkerpop.machine.function.map;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Argument;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.structure.TTuple;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ValueMap<C, K, V> extends AbstractFunction<C> implements MapFunction<C, TTuple<K, V>, V> {

    private Argument<K> key;

    private ValueMap(final Coefficient<C> coefficient, final String label, final Argument<K> key) {
        super(coefficient, label);
        this.key = key;
    }

    @Override
    public V apply(final Traverser<C, TTuple<K, V>> traverser) {
        return traverser.object().value(this.key.mapArg(traverser));
    }

    @Override
    public ValueMap<C, K, V> clone() {
        final ValueMap<C, K, V> clone = (ValueMap<C, K, V>) super.clone();
        clone.key = this.key.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.key.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.key);
    }

    public static <C, K, V> ValueMap<C, K, V> compile(final Instruction<C> instruction) {
        return new ValueMap<>(instruction.coefficient(), instruction.label(), Argument.create(instruction.args()[0]));
    }
}