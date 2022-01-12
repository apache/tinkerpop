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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BranchStep<S, E, M> extends ComputerAwareStep<S, E> implements TraversalOptionParent<M, S, E> {

    protected Traversal.Admin<S, M> branchTraversal;
    protected Map<Pick, List<Traversal.Admin<S, E>>> traversalPickOptions = new HashMap<>();
    protected List<Pair<Traversal.Admin, Traversal.Admin<S, E>>> traversalOptions = new ArrayList<>();

    private boolean first = true;
    private boolean hasBarrier;

    public BranchStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setBranchTraversal(final Traversal.Admin<S, M> branchTraversal) {
        this.branchTraversal = this.integrateChild(branchTraversal);
    }

    @Override
    public void addChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (pickToken instanceof Pick) {
            if (this.traversalPickOptions.containsKey(pickToken))
                this.traversalPickOptions.get(pickToken).add(traversalOption);
            else
                this.traversalPickOptions.put((Pick) pickToken, new ArrayList<>(Collections.singletonList(traversalOption)));
        } else {
            final Traversal.Admin pickOptionTraversal;
            if (pickToken instanceof Traversal) {
                pickOptionTraversal = ((Traversal) pickToken).asAdmin();
            } else {
                pickOptionTraversal = new PredicateTraversal(pickToken);
            }
            this.traversalOptions.add(Pair.with(pickOptionTraversal, traversalOption));
        }

        traversalOption.addStep(new EndStep(traversalOption));

        if (!this.hasBarrier && !TraversalHelper.getStepsOfAssignableClassRecursively(Barrier.class, traversalOption).isEmpty())
            this.hasBarrier = true;
        this.integrateChild(traversalOption);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.unmodifiableList(Stream.concat(
                this.traversalPickOptions.values().stream().flatMap(List::stream),
                this.traversalOptions.stream().map(Pair::getValue1)).collect(Collectors.toList()));
    }

    @Override
    public List<Traversal.Admin<S, M>> getLocalChildren() {
        return Collections.singletonList(this.branchTraversal);
    }

    @Override
    protected Iterator<Traverser.Admin<E>> standardAlgorithm() {
        while (true) {
            if (!this.first) {
                // this block is ignored on the first pass through the while(true) giving the opportunity for
                // the traversalOptions to be prepared. Iterate all of them and simply return the ones that yield
                // results. applyCurrentTraverser() will have only injected the current traverser into the options
                // that met the choice requirements.
                for (final Traversal.Admin<S, E> option : getGlobalChildren()) {
                    // checking hasStarts() first on the step in case there is a ReducingBarrierStep which will
                    // always return true for hasNext()
                    if (option.getStartStep().hasStarts() && option.hasNext())
                        return option.getEndStep();
                }
            }

            this.first = false;

            // pass the current traverser to applyCurrentTraverser() which will make the "choice" of traversal to
            // apply with the given traverser. as this is in a while(true) this phase essentially prepares the options
            // for execution above
            if (this.hasBarrier) {
                if (!this.starts.hasNext())
                    throw FastNoSuchElementException.instance();
                while (this.starts.hasNext()) {
                    this.applyCurrentTraverser(this.starts.next());
                }
            } else {
                this.applyCurrentTraverser(this.starts.next());
            }
        }
    }

    /**
     * Choose the right traversal option to apply and seed those options with this traverser.
     */
    private void applyCurrentTraverser(final Traverser.Admin<S> start) {
        // first get the value of the choice based on the current traverser and use that to select the right traversal
        // option to which that traverser should be routed
        final Object choice = TraversalUtil.apply(start, this.branchTraversal);
        final List<Traversal.Admin<S, E>> branches = pickBranches(choice);

        // if a branch is identified, then split the traverser and add it to the start of the option so that when
        // that option is iterated (in the calling method) that value can be applied.
        if (null != branches)
            branches.forEach(traversal -> traversal.addStart(start.split()));

        if (choice != Pick.any) {
            final List<Traversal.Admin<S, E>> anyBranch = this.traversalPickOptions.get(Pick.any);
            if (null != anyBranch)
                anyBranch.forEach(traversal -> traversal.addStart(start.split()));
        }
    }

    @Override
    protected Iterator<Traverser.Admin<E>> computerAlgorithm() {
        final List<Traverser.Admin<E>> ends = new ArrayList<>();
        final Traverser.Admin<S> start = this.starts.next();
        final Object choice = TraversalUtil.apply(start, this.branchTraversal);
        final List<Traversal.Admin<S, E>> branches = pickBranches(choice);
        if (null != branches) {
            branches.forEach(traversal -> {
                final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                split.setStepId(traversal.getStartStep().getId());
                //split.addLabels(this.labels);
                ends.add(split);
            });
        }
        if (choice != Pick.any) {
            final List<Traversal.Admin<S, E>> anyBranch = this.traversalPickOptions.get(Pick.any);
            if (null != anyBranch) {
                anyBranch.forEach(traversal -> {
                    final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                    split.setStepId(traversal.getStartStep().getId());
                    //split.addLabels(this.labels);
                    ends.add(split);
                });
            }
        }
        return ends.iterator();
    }

    private List<Traversal.Admin<S, E>> pickBranches(final Object choice) {
        final List<Traversal.Admin<S, E>> branches = new ArrayList<>();
        if (choice instanceof Pick) {
            if (this.traversalPickOptions.containsKey(choice)) {
                branches.addAll(this.traversalPickOptions.get(choice));
            }
        }
        for (final Pair<Traversal.Admin, Traversal.Admin<S, E>> p : this.traversalOptions) {
            if (TraversalUtil.test(choice, p.getValue0())) {
                branches.add(p.getValue1());
            }
        }
        return branches.isEmpty() ? this.traversalPickOptions.get(Pick.none) : branches;
    }

    @Override
    public BranchStep<S, E, M> clone() {
        final BranchStep<S, E, M> clone = (BranchStep<S, E, M>) super.clone();
        clone.traversalPickOptions = new HashMap<>(this.traversalPickOptions.size());
        clone.traversalOptions = new ArrayList<>(this.traversalOptions.size());
        for (final Map.Entry<Pick, List<Traversal.Admin<S, E>>> entry : this.traversalPickOptions.entrySet()) {
            final List<Traversal.Admin<S, E>> traversals = entry.getValue();
            if (traversals.size() > 0) {
                final List<Traversal.Admin<S, E>> clonedTraversals = clone.traversalPickOptions.compute(entry.getKey(), (k, v) ->
                        (v == null) ? new ArrayList<>(traversals.size()) : v);
                for (final Traversal.Admin<S, E> traversal : traversals) {
                    clonedTraversals.add(traversal.clone());
                }
            }
        }
        for (final Pair<Traversal.Admin, Traversal.Admin<S, E>> pair : this.traversalOptions) {
            clone.traversalOptions.add(Pair.with(pair.getValue0().clone(), pair.getValue1().clone()));
        }
        clone.branchTraversal = this.branchTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.branchTraversal);
        this.getGlobalChildren().forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.traversalPickOptions != null)
            result ^= this.traversalPickOptions.hashCode();
        if (this.traversalOptions != null)
            result ^= this.traversalOptions.hashCode();
        if (this.branchTraversal != null)
            result ^= this.branchTraversal.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final List<Pair> combinedOptions = Stream.concat(
                this.traversalPickOptions.entrySet().stream().map(e -> Pair.with(e.getKey(), e.getValue())),
                this.traversalOptions.stream()).collect(Collectors.toList());
        return StringFactory.stepString(this, this.branchTraversal, combinedOptions);
    }

    @Override
    public void reset() {
        super.reset();
        this.getGlobalChildren().forEach(Traversal.Admin::reset);
        this.first = true;
    }
}