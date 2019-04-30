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
package org.apache.tinkerpop.machine.structure.jdbc.bytecode.compiler;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.BytecodeCompiler;
import org.apache.tinkerpop.machine.bytecode.compiler.FunctionType;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.structure.jdbc.function.flatmap.SqlFlatMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JDBCCompiler implements BytecodeCompiler {

    private static final JDBCCompiler INSTANCE = new JDBCCompiler();

    private JDBCCompiler() {
        // static instance
    }

    @Override
    public <C> CFunction<C> compile(final Instruction<C> instruction) {
        final String op = instruction.op();
        if (op.equals(Symbols.JDBC_SQL))
            return SqlFlatMap.compile(instruction);
        else
            return null;
    }

    @Override
    public FunctionType getFunctionType(final String op) {
        return op.equals(Symbols.JDBC_SQL) ? FunctionType.FLATMAP : null;
    }

    public static JDBCCompiler instance() {
        return INSTANCE;
    }

    public static class Symbols {

        private Symbols() {
            // static instance
        }

        public static final String JDBC_SQL = "jdbc:sql";
    }
}
