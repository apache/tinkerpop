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
package org.apache.tinkerpop.machine.processor;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.CompositeCompiler;
import org.apache.tinkerpop.machine.bytecode.CoreCompiler;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.structure.EmptyStructure;
import org.apache.tinkerpop.machine.traverser.ShellTraverser;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExplainProcessor extends SimpleProcessor<Long, String, String> {

    public ExplainProcessor(final Bytecode bytecode) {
        bytecode.getInstructions().remove(bytecode.getInstructions().size() - 1);
        super.traverser = new ShellTraverser<>(LongCoefficient.create(), ExplainProcessor.processBytecode(bytecode));
    }

    @Override
    public void addStart(final Traverser<Long, String> traverser) {
        throw new IllegalStateException("This shouldn't occur"); // TODO: exception handling system
    }

    private static String processBytecode(final Bytecode<Long> bytecode) {
        final Map<String, String> explain = new LinkedHashMap<>();
        explain.put("Original", bytecode.toString());
        for (final Strategy strategy : BytecodeUtil.getStrategies(bytecode)) {
            BytecodeUtil.strategize(bytecode, strategy);
            explain.put(strategy.toString(), bytecode.toString());
        }
        int maxLength = 0;
        for (final String key : explain.keySet()) {
            if (maxLength < key.length())
                maxLength = key.length();
        }
        final StringBuilder builder = new StringBuilder();
        for (final Map.Entry<String, String> entry : explain.entrySet()) {
            final int spaces = maxLength - entry.getKey().length();
            builder.append(entry.getKey());
            for (int i = 0; i < spaces; i++) {
                builder.append(" ");
            }
            builder.append("\t\t").append(entry.getValue()).append("\n");
        }
        final String functions = CompositeCompiler.compile(bytecode, Arrays.asList(CoreCompiler.instance(),
                BytecodeUtil.getStructureFactory(bytecode).orElse(EmptyStructure.instance()).getCompiler().orElse(new CoreCompiler()))).toString();

        builder.append("Final");
        for (int i = 0; i < maxLength - "Final".length(); i++) {
            builder.append(" ");
        }
        builder.append("\t\t").append(functions);
        return builder.toString();

    }
}
