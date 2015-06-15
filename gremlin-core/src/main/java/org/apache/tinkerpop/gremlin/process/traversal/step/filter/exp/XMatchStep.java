/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.filter.exp;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class XMatchStep<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    public enum Conjunction {AND, OR}

    private List<Traversal.Admin<Object, Object>> conjunctionTraversals = new ArrayList<>();
    private boolean first = true;
    private Set<String> matchStartLabels = null;
    private final Conjunction conjunction;
    private final MatchAlgorithm matchAlgorithm = new GreedyMatchAlgorithm();

    public XMatchStep(final Traversal.Admin traversal, final Conjunction conjunction, final Traversal... conjunctionTraversals) {
        super(traversal);
        this.conjunction = conjunction;
        this.conjunctionTraversals = (List) Stream.of(conjunctionTraversals).map(Traversal::asAdmin).map(this::integrateChild).collect(Collectors.toList());
    }

    public Set<String> getMatchStartLabels() {
        if (null == this.matchStartLabels) {
            this.matchStartLabels = new HashSet<>();
            for (final Traversal.Admin<Object, Object> andTraversal : this.conjunctionTraversals) {
                this.matchStartLabels.addAll(andTraversal.getStartStep() instanceof XMatchStep ? ((XMatchStep) andTraversal.getStartStep()).getMatchStartLabels() : ((SelectOneStep) andTraversal.getStartStep()).getScopeKeys());
            }
            this.matchStartLabels = Collections.unmodifiableSet(this.matchStartLabels);
        }
        return this.matchStartLabels;
    }

    public List<Traversal.Admin<Object, Object>> getGlobalChildren() {
        return Collections.unmodifiableList(this.conjunctionTraversals);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.conjunction, this.conjunctionTraversals);
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
    }

    @Override
    public XMatchStep<S> clone() {
        final XMatchStep<S> clone = (XMatchStep<S>) super.clone();
        clone.conjunctionTraversals = new ArrayList<>();
        for (final Traversal.Admin<Object, Object> traversal : this.conjunctionTraversals) {
            clone.conjunctionTraversals.add(clone.integrateChild(traversal.clone()));
        }
        // TODO: does it need to clone the match algorithm?
        return clone;
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            Traverser.Admin traverser = null;
            if (this.first) {
                this.matchAlgorithm.initialize(this.conjunction, this.conjunctionTraversals);
                this.first = false;
            } else {
                for (final Traversal.Admin<?, ?> conjunctionTraversal : this.conjunctionTraversals) {
                    if (conjunctionTraversal.hasNext()) {
                        traverser = conjunctionTraversal.getEndStep().next().asAdmin();
                        break;
                    }
                }
            }
            if (null == traverser) {
                traverser = this.starts.next();
                if (this.conjunction == Conjunction.OR) {
                    for (final Traversal.Admin<?, ?> conjunctionTraversal : this.conjunctionTraversals) {
                        conjunctionTraversal.addStart(traverser.split());
                    }
                }
            }
            final Optional<Traversal.Admin<Object, Object>> optional = this.matchAlgorithm.apply(traverser); // determine which sub-pattern the traverser should try next
            if (optional.isPresent()) {
                final Traversal.Admin<Object, Object> traversal = optional.get();
                traverser.path().addLabel(traversal.getStartStep().getId()); // unique identifier for the traversal match sub-pattern
                traversal.addStart(traverser);  // go down the traversal match sub-pattern
            } else
                // TODO: trim off internal traversal labels from path
                // TODO: simply iterate through traversals.startStep.getId() and remove those labels
                // TODO: however, they are globally unique so it might not be necessary especially if we return Map<String,Object>
                return IteratorUtils.of(traverser); // the traverser has survived all requisite match patterns and is ready to move onto the next step
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        if (this.first) {
            this.matchAlgorithm.initialize(this.conjunction, this.conjunctionTraversals);
            this.first = false;
        }
        final Traverser.Admin traverser = this.starts.next();
        final Optional<Traversal.Admin<Object, Object>> optional = this.matchAlgorithm.apply(traverser); // determine which sub-pattern the traverser should try next
        if (optional.isPresent()) {
            final Traversal.Admin<Object, Object> traversal = optional.get();
            traverser.path().addLabel(traversal.getStartStep().getId()); // unique identifier for the traversal match sub-pattern
            traverser.setStepId(traversal.getStartStep().getId()); // go down the traversal match sub-pattern
            return IteratorUtils.of(traverser);
        } else {
            // TODO: trim off internal traversal labels from path
            // TODO: simply iterate through traversals.startStep.getId() and remove those labels
            // TODO: however, they are globally unique so it might not be necessary especially if we return Map<String,Object>
            traverser.asAdmin().setStepId(this.getNextStep().getId());
            return IteratorUtils.of(traverser); // the traverser has survived all requisite match patterns and is ready to move onto the next step
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.conjunctionTraversals.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    //////////////////////////////

    public static class XMatchEndStep<S> extends AbstractStep<S, S> {

        private final String matchKey;
        private final String matchStepId;

        public XMatchEndStep(final Traversal.Admin traversal, final XMatchStep matchStep, final String matchKey) {
            super(traversal);
            this.matchKey = matchKey;
            this.matchStepId = matchStep.getId();
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            while (true) {
                final Traverser.Admin<S> start = this.starts.next();
                // no end label
                if (null == this.matchKey) {
                    if (this.traverserStepIdSetByChild) start.setStepId(this.matchStepId);
                    return start;
                }
                // side-effect check
                final Optional<S> optional = start.getSideEffects().get(this.matchKey);
                if (optional.isPresent() && start.get().equals(optional.get())) {
                    if (this.traverserStepIdSetByChild) start.setStepId(this.matchStepId);
                    return start;
                }
                // path check
                final Path path = start.path();
                if (!path.hasLabel(this.matchKey) || start.get().equals(path.getSingle(Pop.head, this.matchKey))) {
                    if (this.traverserStepIdSetByChild) start.setStepId(this.matchStepId);
                    return start;
                }
            }
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.matchKey, this.matchStepId);
        }
    }


    //////////////////////////////

    public interface MatchAlgorithm extends Function<Traverser.Admin<Object>, Optional<Traversal.Admin<Object, Object>>> {

        public static Set<String> getStartLabels(final Traversal.Admin<Object, Object> traversal) {
            final Step<?, ?> startStep = traversal.getStartStep();
            if (startStep instanceof XMatchStep)
                return ((XMatchStep) startStep).getMatchStartLabels();
            else if (startStep instanceof SelectOneStep)
                return ((SelectOneStep) startStep).getScopeKeys();
            else
                return Collections.emptySet();
        }

        public void initialize(final Conjunction conjunction, final List<Traversal.Admin<Object, Object>> traversals);
    }

    public static class GreedyMatchAlgorithm implements MatchAlgorithm {

        private List<Traversal.Admin<Object, Object>> traversals;
        private List<String> traversalLabels = new ArrayList<>();
        private List<Set<String>> startLabels = new ArrayList<>();
        private Conjunction conjunction;

        @Override
        public void initialize(final Conjunction conjunction, final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;
            this.conjunction = conjunction;
            for (final Traversal.Admin<Object, Object> traversal : this.traversals) {
                this.traversalLabels.add(traversal.getStartStep().getId());
                this.startLabels.add(MatchAlgorithm.getStartLabels(traversal));
            }
        }

        @Override
        public Optional<Traversal.Admin<Object, Object>> apply(final Traverser.Admin<Object> traverser) {
            final Path path = traverser.path();
            if (Conjunction.AND == this.conjunction) {
                int count = 0;
                for (int i = 0; i < this.traversals.size(); i++) {
                    count++;
                    if (this.startLabels.get(i).stream().filter(path::hasLabel).findAny().isPresent() && !path.hasLabel(this.traversalLabels.get(i))) {
                        return Optional.of(this.traversals.get(i));
                    }
                }
                if (count != this.traversals.size())
                    throw new IllegalStateException("The provided match and-pattern is unsolvable: " + this.traversals);
                return Optional.empty();
            } else {
                int count = 0;
                for (int i = 0; i < this.traversals.size(); i++) {
                    count++;
                    if (path.hasLabel(this.traversalLabels.get(i)))
                        return Optional.empty();
                    else if (this.startLabels.get(i).stream().filter(path::hasLabel).findAny().isPresent()) {
                        return Optional.of(this.traversals.get(i));
                    }
                }
                if (count == 0)
                    throw new IllegalStateException("The provided match or-pattern is unsolvable: " + this.traversals);
                return Optional.empty();
            }
        }
    }
}
