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
package org.apache.tinkerpop.util;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.SourceInstruction;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StringFactory {

    private StringFactory() {
        // do nothing
    }

    public static String makeInstructionString(final Instruction<?> instruction) {
        String name = instruction.op();
        if (!instruction.coefficient().isUnity())
            name = "|" + instruction.coefficient().value() + "|" + name;
        if (instruction.args().length > 0)
            name = name + "(";
        for (final Object object : instruction.args()) {
            name = name + object + ",";
        }
        if (instruction.args().length > 0) {
            name = name.substring(0, name.length() - 1);
            name = name + ")";
        }
        if (!instruction.labels().isEmpty())
            name = name + "@" + instruction.labels();
        return name;
    }

    public static String makeSourceInstructionString(final SourceInstruction sourceInstruction) {
        String name = sourceInstruction.op();
        if (sourceInstruction.args().length > 0)
            name = name + "(";
        for (final Object object : sourceInstruction.args()) {
            name = name + object + ",";
        }
        if (sourceInstruction.args().length > 0) {
            name = name.substring(0, name.length() - 1);
            name = name + ")";
        }
        return name;
    }

    public static String makeFunctionString(final CFunction<?> function, final Object... args) {
        String name = function.getClass().getSimpleName();
        if (!function.coefficient().isUnity())
            name = "|" + function.coefficient().value() + "|" + name;
        if (args.length > 0)
            name = name + "(";
        for (final Object object : args) {
            name = name + object + ",";
        }
        if (args.length > 0) {
            name = name.substring(0, name.length() - 1);
            name = name + ")";
        }
        if (!function.labels().isEmpty())
            name = name + "@" + function.labels();
        return name;
    }

    public static String makeTraverserString(final Traverser<?, ?> traverser) {
        return "|" + traverser.coefficient() + "|" + traverser.object();
    }
}
