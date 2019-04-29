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
package org.apache.tinkerpop.machine.strategy.decoration;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.strategy.AbstractStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExplainStrategy extends AbstractStrategy<Strategy.DecorationStrategy> implements Strategy.DecorationStrategy {

    private static final String ORIGINAL = "Original";
    private static final String COMPILATION = "Compilation";
    private static final String EXECUTION_PLAN = "Execution Plan";

    @Override
    public <C> void apply(final Bytecode<C> bytecode) {
        if (bytecode.lastInstruction().op().equals(Symbols.EXPLAIN)) {
            bytecode.getInstructions().remove(bytecode.lastInstruction());
            bytecode.getSourceInstructions().removeIf(instruction ->
                    instruction.op().equals(Symbols.WITH_STRATEGY) &&
                            instruction.args()[0].equals(ExplainStrategy.class));
            final Bytecode<C> clone = bytecode.clone();
            bytecode.getInstructions().clear();
            bytecode.addInstruction(
                    (Coefficient<C>) LongCoefficient.create(),
                    Symbols.INJECT,
                    ExplainStrategy.explainBytecode(clone));
        }
    }

    private static <C> String explainBytecode(final Bytecode<C> bytecode) {
        final Map<String, String> explain = new LinkedHashMap<>();
        explain.put(ORIGINAL, bytecode.toString());
        for (final Strategy strategy : BytecodeUtil.getStrategies(bytecode)) {
            BytecodeUtil.strategize(bytecode, strategy);
            explain.put(strategy.toString(), bytecode.toString());
        }
        final Compilation<C, ?, ?> compilation = Compilation.compile(bytecode);
        explain.put(COMPILATION, compilation.getFunctions().toString());
        explain.put(EXECUTION_PLAN + " [" + BytecodeUtil.getProcessorFactory(bytecode).get().getClass().getSimpleName() + "]", compilation.getProcessor().toString());

        int maxLength = 0;
        for (final String key : explain.keySet()) {
            if (maxLength < key.length())
                maxLength = key.length();
        }
        final StringBuilder builder = new StringBuilder();
        for (final Map.Entry<String, String> entry : explain.entrySet()) {
            if (entry.getKey().equals(COMPILATION)) {
                for (int i = 0; i < maxLength; i++) {
                    builder.append("-");
                }
                builder.append("\n");
            }
            builder.append(entry.getKey());
            ExplainStrategy.addSpaces(builder, entry.getKey(), maxLength);
            builder.append("\t\t").append(entry.getValue()).append("\n");
        }
        builder.replace(builder.length() - 1, builder.length(), ""); // removes trailing newline character
        return builder.toString();

    }

    private static void addSpaces(final StringBuilder builder, final String current, final int maxLength) {
        final int spaces = maxLength - current.length();
        for (int i = 0; i < spaces; i++) {
            builder.append(" ");
        }
    }
}
