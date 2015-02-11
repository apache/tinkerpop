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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.branch;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.Traverser;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.TraversalOptionParent;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ComputerAwareStep;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S, E, M> extends ComputerAwareStep<S, E> implements TraversalOptionParent<M, S, E> {

    protected Traversal.Admin<S, M> branchTraversal;
    protected Map<M, List<Traversal.Admin<S, E>>> traversalOptions = new HashMap<>();
    private boolean first = true;

    public BranchStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setBranchTraversal(final Traversal.Admin<S, M> branchTraversal) {
        this.integrateChild(this.branchTraversal = branchTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (this.traversalOptions.containsKey(pickToken))
            this.traversalOptions.get(pickToken).add(traversalOption);
        else
            this.traversalOptions.put(pickToken, new ArrayList<>(Collections.singletonList(traversalOption)));
        traversalOption.addStep(new EndStep(traversalOption));
        this.integrateChild(traversalOption, TYPICAL_GLOBAL_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.unmodifiableList(this.traversalOptions.values().stream()
                .flatMap(list -> list.stream())
                .collect(Collectors.toList()));
    }

    @Override
    public List<Traversal.Admin<S, M>> getLocalChildren() {
        return Collections.singletonList(this.branchTraversal);
    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            if (!this.first) {
                for (final List<Traversal.Admin<S, E>> options : this.traversalOptions.values()) {
                    for (final Traversal.Admin<S, E> option : options) {
                        if (option.hasNext())
                            return option.getEndStep();
                    }
                }
            }
            this.first = false;
            ///
            final Traverser.Admin<S> start = this.starts.next();
            final M choice = TraversalUtil.apply(start, this.branchTraversal);
            final List<Traversal.Admin<S, E>> branch = this.traversalOptions.containsKey(choice) ? this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);
            if (null != branch) {
                branch.forEach(traversal -> {
                    traversal.reset();
                    traversal.addStart(start.split());
                });
            }
            if (choice != Pick.any) {
                final List<Traversal.Admin<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
                if (null != anyBranch)
                    anyBranch.forEach(traversal -> {
                        traversal.reset();
                        traversal.addStart(start.split());
                    });
            }
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        final List<Traverser<E>> ends = new ArrayList<>();
        final Traverser.Admin<S> start = this.starts.next();
        final M choice = TraversalUtil.apply(start, this.branchTraversal);
        final List<Traversal.Admin<S, E>> branch = this.traversalOptions.containsKey(choice) ? this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);
        if (null != branch) {
            branch.forEach(traversal -> {
                final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                split.setStepId(traversal.getStartStep().getId());
                ends.add(split);
            });
        }
        if (choice != Pick.any) {
            final List<Traversal.Admin<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
            if (null != anyBranch) {
                anyBranch.forEach(traversal -> {
                    final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                    split.setStepId(traversal.getStartStep().getId());
                    ends.add(split);
                });
            }
        }
        return ends.iterator();
    }

    @Override
    public BranchStep<S, E, M> clone() throws CloneNotSupportedException {
        final BranchStep<S, E, M> clone = (BranchStep<S, E, M>) super.clone();
        clone.traversalOptions = new HashMap<>(this.traversalOptions.size());
        for (final Map.Entry<M, List<Traversal.Admin<S, E>>> entry : this.traversalOptions.entrySet()) {
            final List<Traversal.Admin<S, E>> traversals = entry.getValue();
            if (traversals.size() > 0) {
                final List<Traversal.Admin<S, E>> clonedTraversals = clone.traversalOptions.compute(entry.getKey(), (k, v) ->
                        (v == null) ? new ArrayList<>(traversals.size()) : v);
                for (final Traversal.Admin<S, E> traversal : traversals) {
                    final Traversal.Admin<S, E> clonedTraversal = traversal.clone();
                    clonedTraversals.add(clonedTraversal);
                    clone.integrateChild(clonedTraversal, TYPICAL_GLOBAL_OPERATIONS);
                }
            }
        }
        clone.branchTraversal = this.branchTraversal.clone();
        clone.integrateChild(clone.branchTraversal, TYPICAL_LOCAL_OPERATIONS);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.branchTraversal, this.traversalOptions);
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
    }
}
