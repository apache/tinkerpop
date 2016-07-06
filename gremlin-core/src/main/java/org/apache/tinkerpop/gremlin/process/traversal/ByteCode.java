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

import org.apache.tinkerpop.gremlin.process.traversal.util.TranslatorHelper;

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
        this.sourceInstructions.add(new Instruction(sourceName, flattenArguments(arguments).toArray()));
    }

    public void addStep(final String stepName, final Object... arguments) {
        this.stepInstructions.add(new Instruction(stepName, flattenArguments(arguments).toArray()));
    }

    public List<Instruction> getSourceInstructions() {
        return Collections.unmodifiableList(this.sourceInstructions);
    }

    public List<Instruction> getStepInstructions() {
        return Collections.unmodifiableList(this.stepInstructions);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("[");
        for (final Instruction instruction : this.sourceInstructions) {
            builder.append(instruction).append(",\n");
        }
        for (final Instruction instruction : this.stepInstructions) {
            builder.append(instruction).append(",\n");
        }
        if (builder.length() > 2)
            builder.delete(builder.length() - 2, builder.length());
        builder.append("]");
        return builder.toString();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Bytecode &&
                this.sourceInstructions.equals(((Bytecode) object).sourceInstructions) &&
                this.stepInstructions.equals(((Bytecode) object).sourceInstructions);
    }

    @Override
    public int hashCode() {
        return this.sourceInstructions.hashCode() + this.stepInstructions.hashCode();
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
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
            return "[\"" + this.operator + "\"," + stringifyArguments() + "]";
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

        private String stringifyArguments() {
            final List<Object> objects = TranslatorHelper.flattenArguments(this.arguments);
            final StringBuilder builder = new StringBuilder("[");
            for (final Object object : objects) {
                if (object instanceof Traversal)
                    builder.append(((Traversal) object).asAdmin().getBytecode());
                else if (object instanceof String)
                    builder.append("\"").append(object).append("\"");
                else
                    builder.append(object);
                builder.append(",");
            }
            if (!objects.isEmpty())
                builder.deleteCharAt(builder.length() - 1);
            builder.append("]");

            return builder.toString();

        }

    }

    /////

    private static List<Object> flattenArguments(final Object... arguments) {
        if (arguments.length == 0)
            return Collections.emptyList();
        final List<Object> flatArguments = new ArrayList<>();
        for (final Object object : arguments) {
            if (object instanceof Object[]) {
                Collections.addAll(flatArguments, (Object[]) object);
            } else
                flatArguments.add(convertArgument(object));
        }
        return flatArguments;
    }

    private static Object convertArgument(final Object argument) {
       if(argument instanceof Traversal.Admin)
           return ((Traversal.Admin) argument).getBytecode();
        return argument;
    }
}
