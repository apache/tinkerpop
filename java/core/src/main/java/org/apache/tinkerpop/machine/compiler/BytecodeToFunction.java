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
package org.apache.tinkerpop.machine.compiler;

import org.apache.tinkerpop.language.Symbols;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.IncrMap;
import org.apache.tinkerpop.machine.functions.InjectInitial;
import org.apache.tinkerpop.machine.functions.IsFilter;
import org.apache.tinkerpop.machine.functions.PathMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BytecodeToFunction {

    public static <C> List<CFunction<C>> compile(final Bytecode<C> bytecode) throws Exception {
        final List<CFunction<C>> functions = new ArrayList<>();
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            functions.add(BytecodeToFunction.generateFunction(instruction));
        }
        return functions;
    }

    private static <C> CFunction<C> generateFunction(final Instruction<C> instruction) throws Exception {
        final String op = instruction.op();
        final C coefficient = instruction.coefficient();
        final Set<String> labels = instruction.labels();
        switch (op) {
            case Symbols.INJECT:
                return new InjectInitial<>(coefficient, labels, instruction.args());
            case Symbols.IS:
                return new IsFilter<>(coefficient, labels, instruction.args()[0]);
            case Symbols.INCR:
                return new IncrMap<>(coefficient, labels);
            case Symbols.PATH:
                return new PathMap<>(coefficient, labels);
            default:
                throw new Exception("This is an unknown instruction:" + instruction.op());
        }
    }
}
