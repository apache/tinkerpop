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
import org.apache.tinkerpop.machine.functions.branch.selector.Selector;
import org.apache.tinkerpop.machine.functions.branch.selector.TrueSelector;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.MultiIterator;
import org.apache.tinkerpop.util.StringFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionBranch<C, S, E> extends AbstractFunction<C, S, Iterator<Traverser<C, E>>> implements BranchFunction<C, S, E, Boolean> {

    private final Map<Boolean, List<Compilation<C, S, E>>> branches;


    public UnionBranch(final Coefficient<C> coefficient, final Set<String> labels, final List<Compilation<C, S, E>> branches) {
        super(coefficient, labels);
        this.branches = new HashMap<>();
        this.branches.put(Boolean.TRUE, branches);
    }

    @Override
    public Iterator<Traverser<C, E>> apply(final Traverser<C, S> traverser) {
        final MultiIterator<Traverser<C, E>> iterator = new MultiIterator<>();
        for (final Compilation<C, S, E> branch : this.branches.get(Boolean.TRUE)) {
            iterator.addIterator(branch.addTraverser(traverser));
        }
        return iterator;
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.branches.values().iterator().next()); // make a flat array
    }

    @Override
    public Selector<C, S, Boolean> getBranchSelector() {
        return TrueSelector.instance();
    }

    @Override
    public Map<Boolean, List<Compilation<C, S, E>>> getBranches() {
        return this.branches;
    }
}
