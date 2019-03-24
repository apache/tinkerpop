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
package org.apache.tinkerpop.machine.function.branch;

import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.BranchFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BranchBranch<C, S, E> extends AbstractFunction<C> implements BranchFunction<C, S, E> {

    private final Map<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches;

    public BranchBranch(final Coefficient<C> coefficient, final Set<String> labels, final Map<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches) {
        super(coefficient, labels);
        this.branches = branches;
    }

    @Override
    public Map<Compilation<C, S, ?>, List<Compilation<C, S, E>>> getBranches() {
        return this.branches;
    }

    public static <C, S, E> Map<Compilation<C, S, ?>, List<Compilation<C, S, E>>> makeBranches(final Object... args) {
        final Map<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches = new HashMap<>();
        for (int i = 0; i < args.length; i = i + 2) {
            final Compilation<C, S, ?> predicate = Symbols.DEFAULT.equals(args[i]) ? null : Compilation.compileOne(args[i]);
            final Compilation<C, S, E> branch = Compilation.compileOne(args[i + 1]);
            List<Compilation<C, S, E>> list = branches.get(predicate);
            if (null == list) {
                list = new ArrayList<>();
                branches.put(predicate, list);
            }
            list.add(branch);
        }
        return branches;
    }
}
