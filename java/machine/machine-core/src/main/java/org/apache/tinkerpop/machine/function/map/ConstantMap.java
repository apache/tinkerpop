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
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ConstantMap<C, S, E> extends AbstractFunction<C> implements MapFunction<C, S, E> {

    private final E constant;

    private ConstantMap(final Coefficient<C> coefficient, final Set<String> labels, final E constant) {
        super(coefficient, labels);
        this.constant = constant;
    }

    @Override
    public E apply(final Traverser<C, S> traverser) {
        return this.constant;
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.constant);
    }

    public static <C, S, E> ConstantMap<C, S, E> compile(final Instruction<C> instruction) {
        return new ConstantMap<>(instruction.coefficient(), instruction.labels(), (E) instruction.args()[0]);
    }
}