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
package org.apache.tinkerpop.machine.structure.jdbc.strategy;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.strategy.AbstractStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JDBCStrategy extends AbstractStrategy<Strategy.ProviderStrategy> implements Strategy.ProviderStrategy {

    @Override
    public <C> void apply(final Bytecode<C> bytecode) {
        final List<Instruction<C>> dbInstructions = new ArrayList<>();
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            if (instruction.op().equals(Symbols.DB)) {
                dbInstructions.add(instruction);
            }
        }
        for (final Instruction<C> instruction : dbInstructions) {
            BytecodeUtil.replaceInstruction(bytecode, instruction,
                    new Instruction<>(
                            instruction.coefficient(),
                            instruction.label(),
                            Symbols.DB,
                            BytecodeUtil.getStructureFactory(BytecodeUtil.getRootBytecode(bytecode)).get().mint()));
        }
    }

    @Override
    public Set<Class<? extends ProviderStrategy>> applyPost() {
        return Set.of(JDBCQueryStrategy.class);
    }
}
