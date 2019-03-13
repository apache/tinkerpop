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
package org.apache.tinkerpop.machine.functions.branch;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.functions.branch.selector.HasNextSelector;
import org.apache.tinkerpop.machine.functions.branch.selector.Selector;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseBranch<C, S, E> extends AbstractFunction<C, S, Iterator<Traverser<C, E>>> implements BranchFunction<C, S, E, Boolean> {

    private final HasNextSelector<C, S> branchSelector;
    private final Map<Boolean, List<Compilation<C, S, E>>> branches;
    /////
    private final Compilation<C, S, ?> predicate;
    private final Compilation<C, S, E> trueBranch;
    private final Compilation<C, S, E> falseBranch;


    public ChooseBranch(final Coefficient<C> coefficient, final Set<String> labels,
                        final Compilation<C, S, ?> predicate,
                        final Compilation<C, S, E> trueBranch,
                        final Compilation<C, S, E> falseBranch) {
        super(coefficient, labels);
        this.predicate = predicate;
        this.trueBranch = trueBranch;
        this.falseBranch = falseBranch;

        this.branchSelector = new HasNextSelector<>(predicate);
        this.branches = new HashMap<>();
        this.branches.put(Boolean.TRUE, Collections.singletonList(trueBranch));
        this.branches.put(Boolean.FALSE, Collections.singletonList(falseBranch));
    }

    @Override
    public Iterator<Traverser<C, E>> apply(final Traverser<C, S> traverser) {
        return this.predicate.filterTraverser(traverser) ?
                this.trueBranch.addTraverser(traverser) :
                this.falseBranch.addTraverser(traverser);
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.predicate, this.trueBranch, this.falseBranch);
    }

    @Override
    public Selector<C, S, Boolean> getBranchSelector() {
        return this.branchSelector;
    }

    @Override
    public Map<Boolean, List<Compilation<C, S, E>>> getBranches() {
        return this.branches;
    }
}
