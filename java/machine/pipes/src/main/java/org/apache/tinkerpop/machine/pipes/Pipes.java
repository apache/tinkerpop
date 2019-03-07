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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.compiler.BytecodeToFunction;
import org.apache.tinkerpop.machine.functions.CFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Pipes<C, S, E> implements Iterator<E> {

    private final List<Step<?, ?, ?>> steps = new ArrayList<>();

    public Pipes(final Bytecode<C> bytecode) throws Exception {
        final List<CFunction<C>> functions = BytecodeToFunction.compile(bytecode);
        Step previousStep = null;
        for (final CFunction<?> function : functions) {
            previousStep = new Step<>(previousStep, function);
            this.steps.add(previousStep);
        }
    }

    @Override
    public boolean hasNext() {
        return this.steps.get(this.steps.size() - 1).hasNext();
    }

    @Override
    public E next() {
        return (E) this.steps.get(this.steps.size() - 1).next().object();
    }

    public List<E> toList() {
        final List<E> list = new ArrayList<>();
        while (this.hasNext()) {
            list.add(this.next());
        }
        return list;
    }
}
