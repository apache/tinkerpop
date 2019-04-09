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
package org.apache.tinkerpop.machine.processor.rxjava.strategy;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.SourceInstruction;
import org.apache.tinkerpop.machine.bytecode.compiler.CommonCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.CompositeCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.FunctionType;
import org.apache.tinkerpop.machine.processor.rxjava.RxJavaProcessor;
import org.apache.tinkerpop.machine.strategy.AbstractStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RxJavaStrategy extends AbstractStrategy<Strategy.ProviderStrategy> implements Strategy.ProviderStrategy {

    @Override
    public <C> void apply(final Bytecode<C> bytecode) {
        if (bytecode.getParent().isEmpty()) { // root bytecode
            final String id = UUID.randomUUID().toString();
            bytecode.addSourceInstruction(RxJavaProcessor.RX_ROOT_BYTECODE_ID, id);
        } else if (!BytecodeUtil.hasSourceInstruction(bytecode, CommonCompiler.Symbols.WITH_PROCESSOR)) {
            if (RxJavaStrategy.isSimple(bytecode)) {
                bytecode.addSourceInstruction(CommonCompiler.Symbols.WITH_PROCESSOR, RxJavaProcessor.class, Map.of(RxJavaProcessor.RX_THREAD_POOL_SIZE, 0)); // guaranteed serial execution
            } else {
                final Bytecode<C> root = BytecodeUtil.getRootBytecode(bytecode);
                final List<SourceInstruction> processors = BytecodeUtil.getSourceInstructions(root, CommonCompiler.Symbols.WITH_PROCESSOR); // potential parallel execution
                for (final SourceInstruction sourceInstruction : processors) {
                    bytecode.addSourceInstruction(sourceInstruction.op(), sourceInstruction.args());
                }
            }
        }
    }

    private static boolean isSimple(final Bytecode<?> bytecode) {
        final CompositeCompiler compiler = BytecodeUtil.getCompilers(bytecode);
        return bytecode.getInstructions().size() < 5 && bytecode.getInstructions().stream().noneMatch(i -> {
            final FunctionType functionType = compiler.getFunctionType(i.op());
            return FunctionType.FLATMAP == functionType || FunctionType.BRANCH == functionType;
        });
    }
}
