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
package org.apache.tinkerpop.machine.beam;

import org.apache.beam.sdk.values.TupleTag;
import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.functions.branch.selector.Selector;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchFn<C, S, E, M> extends AbstractFn<C, S, E> {

    private final Map<M, TupleTag> branches;
    private final Selector<C, S, M> branchSelector;

    public BranchFn(final BranchFunction<C, S, E, M> branchFunction, final Map<M, TupleTag> branches) {
        super(branchFunction);
        this.branches = branches;
        this.branchSelector = branchFunction.getBranchSelector();
    }

    @ProcessElement
    public void processElement(final @Element Traverser<C, S> traverser, final MultiOutputReceiver out) {
        final Optional<M> selector = this.branchSelector.from(traverser);
        if (selector.isPresent())
            out.get(this.branches.get(selector.get())).output(traverser.clone());

    }
}