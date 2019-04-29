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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Bytecode<C> implements Cloneable, Serializable {

    private List<SourceInstruction> sourceInstructions = new ArrayList<>();
    private List<Instruction<C>> instructions = new ArrayList<>();
    private Bytecode<C> parent = null;

    public void addSourceInstruction(final String op, final Object... args) {
        BytecodeUtil.linkBytecodeChildren(this, args);
        this.sourceInstructions.add(new SourceInstruction(op, args));
    }

    public void addUniqueSourceInstruction(final String op, final Object... args) {
        BytecodeUtil.linkBytecodeChildren(this, args);
        this.sourceInstructions.removeIf(instruction -> instruction.op().equals(op));
        this.sourceInstructions.add(new SourceInstruction(op, args));
    }

    public List<SourceInstruction> getSourceInstructions() {
        return this.sourceInstructions;
    }

    public Optional<Bytecode<C>> getParent() {
        return Optional.ofNullable(this.parent);
    }

    public void setParent(final Bytecode<C> parent) {
        this.parent = parent;
    }

    ///

    public void addInstruction(final Coefficient<C> coefficient, final String op, final Object... args) {
        BytecodeUtil.linkBytecodeChildren(this, args);
        this.instructions.add(new Instruction<>(coefficient, op, args));
    }

    public void addInstruction(final int index, final Coefficient<C> coefficient, final String op, final Object... args) {
        BytecodeUtil.linkBytecodeChildren(this, args);
        this.instructions.add(index, new Instruction<>(coefficient, op, args));
    }

    public List<Instruction<C>> getInstructions() {
        return this.instructions;
    }

    public Instruction<C> lastInstruction() {
        return this.instructions.get(this.instructions.size() - 1);
    }

    public void addArgs(final Object... args) {
        final Instruction<C> lastInstruction = this.lastInstruction();
        BytecodeUtil.linkBytecodeChildren(this, args);
        final Object[] oldArgs = lastInstruction.args();
        final Object[] newArgs = new Object[oldArgs.length + args.length];
        System.arraycopy(oldArgs, 0, newArgs, 0, oldArgs.length);
        System.arraycopy(args, 0, newArgs, oldArgs.length, args.length);
        lastInstruction.args = newArgs;
    }

    @Override
    public int hashCode() {
        return this.sourceInstructions.hashCode() ^ this.instructions.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Bytecode &&
                this.instructions.equals(((Bytecode) object).instructions) &&
                this.sourceInstructions.equals(((Bytecode) object).sourceInstructions);
    }

    @Override
    public String toString() {
        return this.instructions.toString();
    }

    @Override
    public Bytecode<C> clone() {
        try {
            final Bytecode<C> clone = (Bytecode<C>) super.clone();
            clone.sourceInstructions = new ArrayList<>(this.sourceInstructions.size());
            clone.instructions = new ArrayList<>(this.instructions.size());
            for (final SourceInstruction sourceInstruction : this.sourceInstructions) {
                clone.addSourceInstruction(sourceInstruction.op(), sourceInstruction.args());
            }
            for (final Instruction<C> instruction : this.instructions) {
                clone.addInstruction(instruction.coefficient(), instruction.op(), instruction.args());
                clone.lastInstruction().setLabel(instruction.label());
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
