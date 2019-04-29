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
import org.apache.tinkerpop.machine.bytecode.compiler.Pred;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.FilterFunction;
import org.apache.tinkerpop.machine.structure.util.T2Tuple;
import org.apache.tinkerpop.machine.structure.TTuple;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasKeyFilter<C, K, V> extends AbstractFunction<C> implements FilterFunction<C, TTuple<K, V>> {

    private final Pred predicate;
    private Argument<K> key;

    private HasKeyFilter(final Coefficient<C> coefficient, final String label, final Pred predicate, final Argument<K> key) {
        super(coefficient, label);
        this.predicate = predicate;
        this.key = key;
    }

    @Override
    public boolean test(final Traverser<C, TTuple<K, V>> traverser) {
        final TTuple<K, V> object = traverser.object();
        if (Pred.eq == this.predicate)
            return object.has(this.key.mapArg(traverser));
        else {
            final K testKey = this.key.mapArg(traverser);
            for (final T2Tuple<K, V> entry : traverser.object().entries()) {
                if (this.predicate.test(entry.key(), testKey))
                    return true;
            }
            return false;
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.predicate.hashCode() ^ this.key.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.key);
    }

    @Override
    public HasKeyFilter<C, K, V> clone() {
        final HasKeyFilter<C, K, V> clone = (HasKeyFilter<C, K, V>) super.clone();
        clone.key = this.key.clone();
        return clone;
    }

    public static <C, K, V> HasKeyFilter<C, K, V> compile(final Instruction<C> instruction) {
        return new HasKeyFilter<>(instruction.coefficient(), instruction.label(), Pred.valueOf(instruction.args()[0]), Argument.create(instruction.args()[1]));
    }
}