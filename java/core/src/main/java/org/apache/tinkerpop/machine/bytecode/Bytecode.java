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

import org.apache.tinkerpop.machine.coefficient.Coefficient;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Bytecode<C> implements Cloneable {

    private List<Instruction<C>> instructions = new ArrayList<>();
    private List<SourceInstruction> sourceInstructions = new ArrayList<>();

    public void addSourceInstruction(final String op, final Object... args) {
        this.sourceInstructions.add(new SourceInstruction(op, args));
    }

    public List<SourceInstruction> getSourceInstructions() {
        return this.sourceInstructions;
    }

    ///

    public void addInstruction(final Coefficient<C> coefficient, final String op, final Object... args) {
        this.instructions.add(new Instruction<>(coefficient.clone(), op, args));
        coefficient.unity();
    }

    public List<Instruction<C>> getInstructions() {
        return this.instructions;
    }

    public Instruction<C> lastInstruction() {
        return this.instructions.get(this.instructions.size() - 1);
    }

    @Override
    public String toString() {
        return this.instructions.toString();
    }

    @Override
    public Bytecode<C> clone() {
        try {
            final Bytecode<C> clone = (Bytecode<C>) super.clone();
            clone.sourceInstructions = new ArrayList<>(this.sourceInstructions);
            clone.instructions = new ArrayList<>(this.instructions);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
