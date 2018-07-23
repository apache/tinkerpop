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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S, E, M> extends ComputerAwareStep<S, E> implements TraversalOptionParent<M, S, E> {

    protected Traversal.Admin<S, M> branchTraversal;
    protected Map<Object, List<Traversal.Admin<S, E>>> traversalOptions = new HashMap<>();
    private boolean first = true;
    private boolean hasBarrier = false;

    public BranchStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setBranchTraversal(final Traversal.Admin<S, M> branchTraversal) {
        this.branchTraversal = this.integrateChild(branchTraversal);
    }

    @Override
    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        final Object pickTokenKey = PickTokenKey.make(pickToken);
        if (this.traversalOptions.containsKey(pickTokenKey))
            this.traversalOptions.get(pickTokenKey).add(traversalOption);
        else
            this.traversalOptions.put(pickTokenKey, new ArrayList<>(Collections.singletonList(traversalOption)));

        // adding an IdentityStep acts as a placeholder when reducing barriers get in the way - see the
        // standardAlgorithm() method for more information.
        if (TraversalHelper.hasStepOfAssignableClass(ReducingBarrierStep.class, traversalOption))
            traversalOption.addStep(0, new IdentityStep(traversalOption));
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
        return Collections.unmodifiableList(this.traversalOptions.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()));
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
                // that met the choice requirements.  Note that in addGlobalChildOption an IdentityStep was added to
                // be a holder for that current traverser. That allows us to check that first step for an injected
                // traverser as part of the condition for using that traversal option in the output. This is necessary
                // because barriers like fold(), max(), etc. will always return true for hasNext() even if a traverser
                // was not seeded in applyCurrentTraverser().
                for (final List<Traversal.Admin<S, E>> options : this.traversalOptions.values()) {
                    for (final Traversal.Admin<S, E> option : options) {
                        if (option.getStartStep().hasNext() && option.hasNext())
                            return option.getEndStep();
                    }
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
        final Object choice = PickTokenKey.make(TraversalUtil.apply(start, this.branchTraversal));
        final List<Traversal.Admin<S, E>> branch = this.traversalOptions.containsKey(choice) ?
                this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);

        // if a branch is identified, then split the traverser and add it to the start of the option so that when
        // that option is iterated (in the calling method) that value can be applied.
        if (null != branch)
            branch.forEach(traversal -> traversal.addStart(start.split()));

        if (choice != Pick.any) {
            final List<Traversal.Admin<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
            if (null != anyBranch)
                anyBranch.forEach(traversal -> traversal.addStart(start.split()));
        }
    }

    @Override
    protected Iterator<Traverser.Admin<E>> computerAlgorithm() {
        final List<Traverser.Admin<E>> ends = new ArrayList<>();
        final Traverser.Admin<S> start = this.starts.next();
        final Object choice = PickTokenKey.make(TraversalUtil.apply(start, this.branchTraversal));
        final List<Traversal.Admin<S, E>> branch = this.traversalOptions.containsKey(choice) ? this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);
        if (null != branch) {
            branch.forEach(traversal -> {
                final Traverser.Admin<E> split = (Traverser.Admin<E>) start.split();
                split.setStepId(traversal.getStartStep().getId());
                //split.addLabels(this.labels);
                ends.add(split);
            });
        }
        if (choice != Pick.any) {
            final List<Traversal.Admin<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
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

    @Override
    public BranchStep<S, E, M> clone() {
        final BranchStep<S, E, M> clone = (BranchStep<S, E, M>) super.clone();
        clone.traversalOptions = new HashMap<>(this.traversalOptions.size());
        for (final Map.Entry<Object, List<Traversal.Admin<S, E>>> entry : this.traversalOptions.entrySet()) {
            final List<Traversal.Admin<S, E>> traversals = entry.getValue();
            if (traversals.size() > 0) {
                final List<Traversal.Admin<S, E>> clonedTraversals = clone.traversalOptions.compute(entry.getKey(), (k, v) ->
                        (v == null) ? new ArrayList<>(traversals.size()) : v);
                for (final Traversal.Admin<S, E> traversal : traversals) {
                    clonedTraversals.add(traversal.clone());
                }
            }
        }
        clone.branchTraversal = this.branchTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.branchTraversal);
        this.traversalOptions.values().stream().flatMap(List::stream).forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.traversalOptions != null)
            result ^= this.traversalOptions.hashCode();
        if (this.branchTraversal != null)
            result ^= this.branchTraversal.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.branchTraversal, this.traversalOptions);
    }

    @Override
    public void reset() {
        super.reset();
        this.getGlobalChildren().forEach(Traversal.Admin::reset);
        this.first = true;
    }

    /**
     * PickTokenKey is basically a wrapper for numbers that are used as a PickToken. This is
     * required in order to treat equal numbers of different data types as a match.
     */
    private static class PickTokenKey {

        final Number number;

        private PickTokenKey(final Number number) {
            this.number = number;
        }

        static Object make(final Object value) {
            return value instanceof Number ? new PickTokenKey((Number) value) : value;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final PickTokenKey other = (PickTokenKey) o;
            return 0 == NumberHelper.compare(number, other.number);
        }

        @Override
        public int hashCode() {
            return number.hashCode();
        }

        @Override
        public String toString() {
            return number.toString();
        }
    }
}
