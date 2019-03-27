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
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FilterFilter<C, S> extends AbstractFunction<C> implements FilterFunction<C, S> {

    private final Pred predicate;
    private Argument<S> argument;


    private FilterFilter(final Coefficient<C> coefficient, final String label, final Pred predicate, final Argument<S> argument) {
        super(coefficient, label);
        this.predicate = predicate;
        this.argument = argument;
    }

    @Override
    public boolean test(final Traverser<C, S> traverser) {
        return null == this.predicate ?
                this.argument.filterArg(traverser) :
                this.predicate.test(traverser.object(), this.argument.mapArg(traverser));
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.predicate.hashCode() ^ this.argument.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof FilterFilter &&
                this.predicate.equals(((FilterFilter) object).predicate) &&
                this.argument.equals(((FilterFilter) object).argument) &&
                super.equals(object);
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.argument);
    }

    @Override
    public FilterFilter<C, S> clone() {
        final FilterFilter<C, S> clone = (FilterFilter<C, S>) super.clone();
        clone.argument = this.argument.clone();
        return clone;
    }

    public static <C, S> FilterFilter<C, S> compile(final Instruction<C> instruction) {
        final boolean oneArg = instruction.args().length == 1;
        return new FilterFilter<>(instruction.coefficient(), instruction.label(),
                oneArg ? null : Pred.valueOf(instruction.args()[0]),
                Argument.create(oneArg ? instruction.args()[0] : instruction.args()[1]));
    }

}
