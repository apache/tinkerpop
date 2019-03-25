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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.function.CFunction;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CompositeCompiler implements BytecodeCompiler {

    private final List<BytecodeCompiler> compilers;

    private CompositeCompiler(final List<BytecodeCompiler> compilers) {
        this.compilers = compilers;
    }

    @Override
    public <C> CFunction<C> compile(final Instruction<C> instruction) {
        for (BytecodeCompiler compiler : this.compilers) {
            final CFunction<C> function = compiler.compile(instruction);
            if (null != function)
                return function;
        }
        throw new IllegalStateException("You need a new compiler: " + instruction);
    }

    @Override
    public FunctionType getFunctionType(final String op) {
        for (final BytecodeCompiler compiler : this.compilers) {
            final FunctionType type = compiler.getFunctionType(op);
            if (null != type)
                return type;
        }
        throw new IllegalStateException("You need a new compiler: " + op);
    }

    public static <C> List<CFunction<C>> compile(final Bytecode<C> bytecode, final List<BytecodeCompiler> compilers) {
        return new CompositeCompiler(compilers).compile(bytecode);
    }

    public static <C> CompositeCompiler create(final List<BytecodeCompiler> compilers) {
        return new CompositeCompiler(compilers);
    }

    @Override
    public String toString() {
        return this.compilers.toString();
    }
}
