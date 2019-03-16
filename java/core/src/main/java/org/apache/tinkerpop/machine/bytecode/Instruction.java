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
import org.apache.tinkerpop.util.StringFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Instruction<C> {

    private final Coefficient<C> coefficient;
    private final String op;
    private Object[] args;
    private final Set<String> labels = new HashSet<>();

    public Instruction(final Coefficient<C> coefficient, final String op, final Object... args) {
        this.coefficient = coefficient;
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

    public Set<String> labels() {
        return this.labels;
    }

    public void addLabel(final String label) {
        this.labels.add(label);
    }

    public void addArg(final Object arg) {
        this.args = Arrays.copyOf(this.args, this.args.length + 1);
        this.args[this.args.length - 1] = arg;
    }

    @Override
    public String toString() {
        return StringFactory.makeInstructionString(this);
    }
}
