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

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.processor.HasNextProcessor;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IfBranch<C, S, E> extends AbstractFunction<C> implements BranchFunction<C, S, E, Boolean> {

    private final Compilation<C, S, Boolean> branchSelector;
    private final Map<Boolean, List<Compilation<C, S, E>>> branches;
    /////
    private final Compilation<C, S, ?> predicate;


    public IfBranch(final Coefficient<C> coefficient, final Set<String> labels,
                    final Compilation<C, S, ?> predicate,
                    final Compilation<C, S, E> trueBranch,
                    final Compilation<C, S, E> falseBranch) {
        super(coefficient, labels);
        this.predicate = predicate;
        this.branchSelector = new Compilation<>(new HasNextProcessor<>(predicate));
        this.branches = new HashMap<>();
        this.branches.put(Boolean.TRUE, Collections.singletonList(trueBranch));
        if (null != falseBranch)
            this.branches.put(Boolean.FALSE, Collections.singletonList(falseBranch));
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.predicate, this.branches);
    }

    @Override
    public Compilation<C, S, Boolean> getBranchSelector() {
        return this.branchSelector;
    }

    @Override
    public Map<Boolean, List<Compilation<C, S, E>>> getBranches() {
        return this.branches;
    }
}
