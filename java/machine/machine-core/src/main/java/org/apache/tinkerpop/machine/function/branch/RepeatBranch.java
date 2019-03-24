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
package org.apache.tinkerpop.machine.function.branch;

import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatBranch<C, S> extends AbstractFunction<C> {

    private final Map<Character, Compilation<C, S, S>> compilations; // TODO: remove
    private Compilation<C, S, S> repeatCompilation;
    private Compilation<C, S, ?> untilCompilation;
    private Compilation<C, S, ?> emitCompilation;
    private int untilLocation = 0;
    private int emitLocation = 0;
    private boolean hasStartPredicates = false;
    private boolean hasEndPredicates = false;

    public RepeatBranch(final Coefficient<C> coefficient, final Set<String> labels, final List<Object> arguments) {
        super(coefficient, labels);
        int location = 1;
        this.compilations = new LinkedHashMap<>();
        for (int i = 0; i < arguments.size(); i = i + 2) {
            final Character type = (Character) arguments.get(i);
            if ('e' == type) {
                this.emitCompilation = (Compilation<C, S, ?>) arguments.get(i + 1);
                this.emitLocation = location++;
                if (this.emitLocation < 3)
                    this.hasStartPredicates = true;
                else
                    this.hasEndPredicates = true;
            } else if ('u' == type) {
                this.untilCompilation = (Compilation<C, S, ?>) arguments.get(i + 1);
                this.untilLocation = location++;
                if (this.untilLocation < 3)
                    this.hasStartPredicates = true;
                else
                    this.hasEndPredicates = true;
            } else {
                this.repeatCompilation = (Compilation<C, S, S>) arguments.get(i + 1);
                location = 3;
            }

            this.compilations.put((Character) arguments.get(i), (Compilation<C, S, S>) arguments.get(i + 1));
        }
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.compilations);
    }

    public Compilation<C, S, S> getRepeat() {
        return this.repeatCompilation;
    }

    public Compilation<C, S, ?> getUntil() {
        return this.untilCompilation;
    }

    public Compilation<C, S, ?> getEmit() {
        return this.emitCompilation;
    }

    public int getEmitLocation() {
        return this.emitLocation;
    }

    public int getUntilLocation() {
        return this.untilLocation;
    }

    public boolean hasStartPredicates() {
        return this.hasStartPredicates;
    }

    public boolean hasEndPredicates() {
        return this.hasEndPredicates;
    }
}
