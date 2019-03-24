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
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Instruction<C> {

    private final Coefficient<C> coefficient;
    private final String op;
    private Object[] args;
    private String label = null;

    public Instruction(final Coefficient<C> coefficient, final String op, final Object... args) {
        this.coefficient = coefficient.clone();
        this.op = op;
        this.args = args;
    }

    public Coefficient<C> coefficient() {
        return this.coefficient;
    }

    public String op() {
        return this.op;
    }

    public Object[] args() {
        return this.args;
    }

    public String label() {
        return this.label;
    }

    public void setLabel(final String label) {
        this.label = label;
    }

    public void addArg(final Object arg) {
        final Object[] newArgs = new Object[this.args.length + 1];
        System.arraycopy(this.args, 0, newArgs, 0, this.args.length);
        newArgs[newArgs.length - 1] = arg;
        this.args = newArgs;
    }

    public void addArgs(final Object... args) {
        for (final Object arg : args) {
            this.addArg(arg);
        }
    }

    @Override
    public int hashCode() {
        return this.coefficient.hashCode() ^ this.op.hashCode() ^ Arrays.hashCode(this.args) ^ (null == this.label ? 1 : this.label.hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof Instruction))
            return false;
        final Instruction other = (Instruction) object;
        return this.op.equals(other.op) &&
                Arrays.equals(this.args, other.args) &&
                this.coefficient.equals(other.coefficient) &&
                ((null == this.label && null == other.label) || (null != this.label && this.label.equals(other.label)));
    }

    @Override
    public String toString() {
        return StringFactory.makeInstructionString(this);
    }
}
