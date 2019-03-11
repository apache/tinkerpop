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

import org.apache.tinkerpop.language.Symbols;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.filter.IdentityFilter;
import org.apache.tinkerpop.machine.functions.filter.IsFilter;
import org.apache.tinkerpop.machine.functions.initial.InjectInitial;
import org.apache.tinkerpop.machine.functions.map.IncrMap;
import org.apache.tinkerpop.machine.functions.map.MapMap;
import org.apache.tinkerpop.machine.functions.map.PathMap;
import org.apache.tinkerpop.machine.functions.reduce.CountReduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BytecodeUtil {

    public static <C> Bytecode<C> optimize(final Bytecode<C> bytecode) {
        Instruction<C> toRemove = null;
        for (Instruction<C> instruction : bytecode.getInstructions()) {
            if (instruction.op().equals(Symbols.IDENTITY))
                toRemove = instruction;
        }
        bytecode.removeInstruction(toRemove);
        return bytecode;
    }

    public static <C> List<CFunction<C>> compile(final Bytecode<C> bytecode) {
        final List<CFunction<C>> functions = new ArrayList<>();
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            functions.add(BytecodeUtil.generateFunction(instruction));
        }
        return functions;
    }

    private static <C> CFunction<C> generateFunction(final Instruction<C> instruction) {
        final String op = instruction.op();
        final Coefficient<C> coefficient = instruction.coefficient();
        final Set<String> labels = instruction.labels();
        switch (op) {
            case Symbols.COUNT:
                return new CountReduce<>(coefficient, labels);
            case Symbols.IDENTITY:
                return new IdentityFilter<>(coefficient, labels);
            case Symbols.INJECT:
                return new InjectInitial<>(coefficient, labels, instruction.args());
            case Symbols.IS:
                return new IsFilter<>(coefficient, labels, instruction.args()[0]);
            case Symbols.INCR:
                return new IncrMap<>(coefficient, labels);
            case Symbols.MAP:
                return new MapMap<>(coefficient, labels, compile((Bytecode<C>) instruction.args()[0]));
            case Symbols.PATH:
                return new PathMap<>(coefficient, labels);
            default:
                throw new RuntimeException("This is an unknown instruction:" + instruction.op());
        }
    }
}
