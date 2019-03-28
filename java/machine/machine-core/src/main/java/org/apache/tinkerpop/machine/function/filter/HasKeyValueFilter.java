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
package org.apache.tinkerpop.machine.function.filter;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Argument;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.FilterFunction;
import org.apache.tinkerpop.machine.structure.data.TMap;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasKeyValueFilter<C, K, V> extends AbstractFunction<C> implements FilterFunction<C, TMap<K, V>> {

    private Argument<K> key;
    private Argument<V> value;

    private HasKeyValueFilter(final Coefficient<C> coefficient, final String label, final Argument<K> key, final Argument<V> value) {
        super(coefficient, label);
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean test(final Traverser<C, TMap<K, V>> traverser) {
        final TMap<K, V> object = traverser.object();
        return this.value.mapArg(traverser).equals(object.get(this.key.mapArg(traverser)));
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.key.hashCode() ^ this.value.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.key, this.value);
    }


    @Override
    public HasKeyValueFilter<C, K, V> clone() {
        final HasKeyValueFilter<C, K, V> clone = (HasKeyValueFilter<C, K, V>) super.clone();
        clone.key = this.key.clone();
        clone.value = this.value.clone();
        return clone;
    }

    public static <C, K, V> HasKeyValueFilter<C, K, V> compile(final Instruction<C> instruction) {
        return new HasKeyValueFilter<>(instruction.coefficient(), instruction.label(), Argument.create(instruction.args()[0]), Argument.create(instruction.args()[1]));
    }
}
