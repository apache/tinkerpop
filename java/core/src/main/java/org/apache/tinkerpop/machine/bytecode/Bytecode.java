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
package org.apache.tinkerpop.machine.bytecode;

import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.compiler.Strategy;
import org.apache.tinkerpop.machine.traversers.CompleteTraverserFactory;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Bytecode<C> implements Cloneable {

    public List<Strategy> strategies = new ArrayList<>();
    public List<Instruction<C>> instructions = new ArrayList<>();


    public void addStrategy(final Strategy strategy) {
        this.strategies.add(strategy);
    }

    public List<Strategy> getStrategies() {
        return this.strategies;
    }

    ///

    public void addInstruction(final Coefficient<C> coefficient, final String op, final Object... args) {
        this.instructions.add(new Instruction<>(coefficient.clone(), op, args));
        coefficient.unity();
    }

    public List<Instruction<C>> getInstructions() {
        return this.instructions;
    }

    public void removeInstruction(final Instruction<C> instruction) {
        this.instructions.remove(instruction);
    }

    public Instruction<C> lastInstruction() {
        return this.instructions.get(this.instructions.size() - 1);
    }

    // this should be part of processor!
    public <S> TraverserFactory<C, S> getTraverserFactory() {
        return new CompleteTraverserFactory<>();
    }

    @Override
    public String toString() {
        return this.instructions.toString();
    }

    @Override
    public Bytecode<C> clone() {
        try {
            final Bytecode<C> clone = (Bytecode<C>) super.clone();
            clone.strategies = new ArrayList<>(this.strategies);
            clone.instructions = new ArrayList<>(this.instructions);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
