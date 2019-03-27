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

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.processor.FilterProcessor;
import org.apache.tinkerpop.machine.processor.LoopsProcessor;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatBranch<C, S> extends AbstractFunction<C> {

    private Compilation<C, S, S> repeatCompilation;
    private Compilation<C, S, ?> untilCompilation;
    private Compilation<C, S, ?> emitCompilation;
    private int untilLocation = 0;
    private int emitLocation = 0;
    private boolean hasStartPredicates = false;
    private boolean hasEndPredicates = false;

    public RepeatBranch(final Coefficient<C> coefficient, final String label, final List<Object> arguments) {
        super(coefficient, label);
        int location = 1;
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
        }
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, repeatCompilation, untilCompilation, emitCompilation); // todo: this is random
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

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.repeatCompilation.hashCode() ^ this.emitCompilation.hashCode() ^ this.untilCompilation.hashCode() ^
                this.emitLocation ^ this.untilLocation;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof RepeatBranch &&
                this.repeatCompilation.equals(((RepeatBranch) object).repeatCompilation) &&
                this.emitCompilation.equals(((RepeatBranch) object).emitCompilation) &&
                this.untilCompilation.equals(((RepeatBranch) object).untilCompilation) &&
                this.emitLocation == ((RepeatBranch) object).emitLocation &&
                this.untilLocation == ((RepeatBranch) object).untilLocation &&
                super.equals(object);
    }

    @Override
    public RepeatBranch<C, S> clone() {
        final RepeatBranch<C, S> clone = (RepeatBranch<C, S>) super.clone();
        clone.repeatCompilation = this.repeatCompilation.clone();
        clone.emitCompilation = this.emitCompilation.clone();
        clone.untilCompilation = this.untilCompilation.clone();
        return clone;
    }

    public static <C, S> RepeatBranch<C, S> compile(final Instruction<C> instruction) {
        final List<Object> objects = new ArrayList<>();
        for (final Object arg : instruction.args()) {
            if (arg instanceof Bytecode)
                objects.add(new Compilation<>((Bytecode<?>) arg));
            else if (arg instanceof Character)
                objects.add(arg);
            else if (arg instanceof Integer)
                objects.add(new Compilation<>(new LoopsProcessor<>((int) arg)));
            else if (arg instanceof Boolean)
                objects.add(new Compilation<>(new FilterProcessor<>((boolean) arg)));
        }
        return new RepeatBranch<>(instruction.coefficient(), instruction.label(), objects);
    }
}
