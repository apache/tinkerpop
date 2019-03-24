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
package org.apache.tinkerpop.machine.processor.beam;

import org.apache.beam.sdk.values.TupleTag;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchFn<C, S, E> extends AbstractFn<C, S, S> {

    private final Map<Compilation<C, S, ?>, List<TupleTag<Traverser<C, S>>>> branches;
    private final List<TupleTag<Traverser<C, S>>> defaultBranches;

    public BranchFn(final BranchFunction<C, S, E> branchFunction, final Map<Compilation<C, S, ?>, List<TupleTag<Traverser<C, S>>>> branches) {
        super(branchFunction);
        this.branches = new HashMap<>(branches);
        this.defaultBranches = branches.getOrDefault(null, Collections.emptyList());
        this.branches.remove(null);
    }

    @ProcessElement
    public void processElement(final @Element Traverser<C, S> traverser, final MultiOutputReceiver outputs) {
        boolean found = false;
        for (final Map.Entry<Compilation<C, S, ?>, List<TupleTag<Traverser<C, S>>>> entry : this.branches.entrySet()) {
            if (entry.getKey().filterTraverser(traverser)) {
                found = true;
                for (final TupleTag<Traverser<C, S>> output : entry.getValue()) {
                    outputs.get(output).output(traverser.clone());
                }
            }
        }
        if (!found) {
            for (final TupleTag<Traverser<C, S>> output : this.defaultBranches) {
                outputs.get(output).output(traverser.clone());
            }
        }
    }
}