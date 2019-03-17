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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.function.branch.selector.Selector;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.util.EmptyIterator;
import org.apache.tinkerpop.util.MultiIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class BranchStep<C, S, E, M> extends AbstractStep<C, S, E> {

    private final Selector<C, S, M> branchSelector;
    private final Map<M, List<Compilation<C, S, E>>> branches;
    private Iterator<Traverser<C, E>> nextTraversers = EmptyIterator.instance();

    BranchStep(final Step<C, ?, S> previousStep, final BranchFunction<C, S, E, M> branchFunction) {
        super(previousStep, branchFunction);
        this.branchSelector = branchFunction.getBranchSelector();
        this.branches = branchFunction.getBranches();
    }

    @Override
    public boolean hasNext() {
        this.stageOutput();
        return this.nextTraversers.hasNext();
    }

    @Override
    public Traverser<C, E> next() {
        this.stageOutput();
        return this.nextTraversers.next();
    }

    private final void stageOutput() {
        while (!this.nextTraversers.hasNext() && this.previousStep.hasNext()) {
            final Traverser<C, S> traverser = this.previousStep.next();
            final Optional<M> token = this.branchSelector.from(traverser);
            if (token.isPresent()) {
                final List<Compilation<C, S, E>> matches = this.branches.get(token.get());
                if (1 == matches.size())
                    this.nextTraversers = matches.get(0).addTraverser(traverser);
                else {
                    this.nextTraversers = new MultiIterator<>();
                    for (final Compilation<C, S, E> branch : matches) {
                        ((MultiIterator<Traverser<C, E>>) this.nextTraversers).addIterator(branch.addTraverser(traverser));
                    }
                }
            }
        }
    }

    @Override
    public void reset() {
        this.nextTraversers = EmptyIterator.instance();
    }
}