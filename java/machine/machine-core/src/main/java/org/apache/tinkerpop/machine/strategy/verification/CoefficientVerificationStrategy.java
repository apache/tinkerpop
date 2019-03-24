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
package org.apache.tinkerpop.machine.strategy.verification;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.compiler.CompositeCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.FunctionType;
import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.strategy.AbstractStrategy;
import org.apache.tinkerpop.machine.strategy.Strategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CoefficientVerificationStrategy extends AbstractStrategy<Strategy.VerificationStrategy> implements Strategy.VerificationStrategy {

    @Override
    public <C> void apply(final Bytecode<C> bytecode) {
        final CompositeCompiler compiler = BytecodeUtil.getCompilers(bytecode); // TODO: put the compilers at the root?
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            if (FunctionType.REDUCE == compiler.getFunctionType(instruction.op())) {
                if (!instruction.coefficient().isUnity())
                    throw new IllegalStateException("Reduce functions can not have non-unity coefficients: " + instruction);
            }
        }
    }

}
