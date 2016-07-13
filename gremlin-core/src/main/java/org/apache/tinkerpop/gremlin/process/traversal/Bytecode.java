/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * When a {@link TraversalSource} is manipulated, a {@link Traversal} is spawned and then mutated, a language
 * agnostic representation of those mutations is recorded in a byte code instance. Byte code is simply a list
 * of ordered instructions where an instruction is a string operator and an array of arguments. Byte code is used by
 * {@link Translator} instances which translate a traversal to another language by analyzing the
 * byte code as opposed to the Java traversal object representation on heap.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Bytecode implements Cloneable, Serializable {

    private List<Instruction> sourceInstructions = new ArrayList<>();
    private List<Instruction> stepInstructions = new ArrayList<>();

    public void addSource(final String sourceName, final Object... arguments) {
        this.sourceInstructions.add(new Instruction(sourceName, flattenArguments(arguments)));
    }

    public void addStep(final String stepName, final Object... arguments) {
        this.stepInstructions.add(new Instruction(stepName, flattenArguments(arguments)));
    }

    public List<Instruction> getSourceInstructions() {
        return Collections.unmodifiableList(this.sourceInstructions);
    }

    public List<Instruction> getStepInstructions() {
        return Collections.unmodifiableList(this.stepInstructions);
    }

    @Override
    public String toString() {
        return Arrays.asList(this.sourceInstructions, this.stepInstructions).toString();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Bytecode &&
                this.sourceInstructions.equals(((Bytecode) object).sourceInstructions) &&
                this.stepInstructions.equals(((Bytecode) object).stepInstructions);
    }

    @Override
    public int hashCode() {
        return this.sourceInstructions.hashCode() + this.stepInstructions.hashCode();
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public Bytecode clone() {
        try {
            final Bytecode clone = (Bytecode) super.clone();
            clone.sourceInstructions = new ArrayList<>(this.sourceInstructions);
            clone.stepInstructions = new ArrayList<>(this.stepInstructions);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static class Instruction implements Serializable {

        private final String operator;
        private final Object[] arguments;

        private Instruction(final String operator, final Object... arguments) {
            this.operator = operator;
            this.arguments = arguments;
        }

        public String getOperator() {
            return this.operator;
        }

        public Object[] getArguments() {
            return this.arguments;
        }

        @Override
        public String toString() {
            return this.operator + "(" + StringFactory.removeEndBrackets(Arrays.asList(this.arguments)) + ")";
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof Instruction &&
                    this.operator.equals(((Instruction) object).operator) &&
                    Arrays.equals(this.arguments, ((Instruction) object).arguments);
        }

        @Override
        public int hashCode() {
            return this.operator.hashCode() + Arrays.hashCode(this.arguments);
        }


    }

    /////

    private static final Object[] flattenArguments(final Object... arguments) {
        if (arguments.length == 0)
            return new Object[]{};
        final List<Object> flatArguments = new ArrayList<>();
        for (final Object object : arguments) {
            if (object instanceof Object[]) {
                for (final Object nestObject : (Object[]) object) {
                    flatArguments.add(convertArgument(nestObject));
                }
            } else
                flatArguments.add(convertArgument(object));
        }
        return flatArguments.toArray();
    }

    private static final Object convertArgument(final Object argument) {
        return argument instanceof Traversal ? ((Traversal) argument).asAdmin().getBytecode() : argument;
    }
}
