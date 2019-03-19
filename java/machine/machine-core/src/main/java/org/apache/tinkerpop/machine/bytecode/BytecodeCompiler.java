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

import org.apache.tinkerpop.machine.function.CFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface BytecodeCompiler {

    public default <C> List<CFunction<C>> compile(final Bytecode<C> bytecode) {
        final List<CFunction<C>> functions = new ArrayList<>();
        for (final Instruction<C> instruction : bytecode.getInstructions()) {
            functions.add(this.compile(instruction));
        }
        return functions;
    }

    public <C> CFunction<C> compile(final Instruction<C> instruction);

    public FunctionType getFunctionType(final String op);
}
