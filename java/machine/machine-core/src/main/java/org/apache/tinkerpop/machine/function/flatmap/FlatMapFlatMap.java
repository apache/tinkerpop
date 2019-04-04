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
import org.apache.tinkerpop.machine.bytecode.compiler.Argument;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.ArrayIterator;

import java.lang.reflect.Array;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FlatMapFlatMap<C, S, E> extends AbstractFunction<C> implements FlatMapFunction<C, S, E> {

    private Argument<E> argument;

    private FlatMapFlatMap(final Coefficient<C> coefficient, final String label, final Argument<E> argument) {
        super(coefficient, label);
        this.argument = argument;
    }

    @Override
    public Iterator<E> apply(final Traverser<C, S> traverser) {
        return this.argument.flatMapArg(traverser);
        /*if (object instanceof Iterator)
            return (Iterator<E>) object;
        else if (object instanceof Iterable)
            return ((Iterable<E>) object).iterator();
        else if (object instanceof Map)
            return ((Map) object).entrySet().iterator();
        else if (object.getClass().isArray())
            return handleArrays(object);
        else
            return IteratorUtils.of(object);*/
    }

    private Iterator<E> handleArrays(final Object array) {
        if (array instanceof Object[]) {
            return new ArrayIterator<>((E[]) array);
        } else {
            int len = Array.getLength(array);
            final Object[] objectArray = new Object[len];
            for (int i = 0; i < len; i++)
                objectArray[i] = Array.get(array, i);
            return new ArrayIterator<>((E[]) objectArray);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.argument.hashCode();
    }

    @Override
    public FlatMapFlatMap<C, S, E> clone() {
        final FlatMapFlatMap<C, S, E> clone = (FlatMapFlatMap<C, S, E>) super.clone();
        clone.argument = this.argument.clone();
        return clone;
    }

    public static <C, S, E> FlatMapFlatMap<C, S, E> compile(final Instruction<C> instruction) {
        return new FlatMapFlatMap<>(instruction.coefficient(), instruction.label(), Argument.create(instruction.args()));
    }
}
