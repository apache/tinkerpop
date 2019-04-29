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
import org.apache.tinkerpop.machine.structure.jdbc.JDBCDatabase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JDBCQueryStrategy extends AbstractStrategy<Strategy.ProviderStrategy> implements Strategy.ProviderStrategy {

    @Override
    public <C> void apply(final Bytecode<C> bytecode) {
        if (bytecode.getParent().isEmpty() && BytecodeUtil.startsWith(bytecode, Symbols.DB, Symbols.VALUES, Symbols.DB, Symbols.VALUES, Symbols.HAS_KEY_VALUE, Symbols.PATH)) {
            final JDBCDatabase db = (JDBCDatabase) bytecode.getInstructions().get(0).args()[0];
            bytecode.getInstructions().remove(0); // DB
            final String table1 = (String) bytecode.getInstructions().get(0).args()[0];
            final String as1 = bytecode.getInstructions().get(0).label();
            bytecode.getInstructions().remove(0); // VALUES
            bytecode.getInstructions().remove(0); // DB
            final String table2 = (String) bytecode.getInstructions().get(0).args()[0];
            final String as2 = bytecode.getInstructions().get(0).label();
            bytecode.getInstructions().remove(0); // VALUES
            final String join = as1 + "." + bytecode.getInstructions().get(0).args()[0] + "=" + as2 + "." + ((Bytecode<C>) ((Bytecode<C>) bytecode.getInstructions().get(0).args()[1]).getInstructions().get(0).args()[1]).getInstructions().get(0).args()[0];
            final String query = "SELECT " + as1 + ".*, " + as2 + ".* FROM " + table1 + " AS " + as1 + ", " + table2 + " AS " + as2 + " WHERE " + join;
            final Instruction<C> inst = bytecode.getInstructions().remove(0); // HAS_KEY_VALUE
            bytecode.getInstructions().remove(0); // PATH
            bytecode.addInstruction(0, inst.coefficient(), "jdbc:sql", db.getConnection(), as1, as2, query);
        }
    }
}
