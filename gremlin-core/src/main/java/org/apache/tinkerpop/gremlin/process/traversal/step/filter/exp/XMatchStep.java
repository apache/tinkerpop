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
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConjunctionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConjunctionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class XMatchStep<S, E> extends ComputerAwareStep<S, Map<String, E>> implements TraversalParent, Scoping {

    public enum Conjunction {AND, OR}

    private List<Traversal.Admin<Object, Object>> conjunctionTraversals = new ArrayList<>();
    private boolean first = true;
    private Set<String> matchStartLabels = new HashSet<>();
    private Set<String> matchEndLabels = new HashSet<>();
    private final Conjunction conjunction;
    private final String startKey;
    private final MatchAlgorithm matchAlgorithm = new CountMatchAlgorithm();

    public XMatchStep(final Traversal.Admin traversal, final String startKey, final Conjunction conjunction, final Traversal... conjunctionTraversals) {
        super(traversal);
        this.conjunction = conjunction;
        this.traversal.getEndStep().addLabel(this.startKey = startKey);
        this.conjunctionTraversals = (List) Stream.of(conjunctionTraversals).map(Traversal::asAdmin).map(this::integrateChild).collect(Collectors.toList());
        this.conjunctionTraversals.forEach(this::configureStartAndEndSteps); // recursively convert to SelectOneStep, XMatchStep, or XMatchEndStep
    }

    private void configureStartAndEndSteps(final Traversal.Admin<?, ?> conjunctionTraversal) {
        ConjunctionStrategy.instance().apply(conjunctionTraversal);
        // START STEP to SelectOneStep OR XMatchStep
        final Step<?, ?> startStep = conjunctionTraversal.getStartStep();
        if (startStep instanceof ConjunctionStep) {
            final XMatchStep xMatchStep = new XMatchStep(conjunctionTraversal, this.startKey,
                    startStep instanceof AndStep ? XMatchStep.Conjunction.AND : XMatchStep.Conjunction.OR,
                    ((ConjunctionStep<?>) startStep).getLocalChildren().toArray(new Traversal[((ConjunctionStep<?>) startStep).getLocalChildren().size()]));
            TraversalHelper.replaceStep(startStep, xMatchStep, conjunctionTraversal);
            this.matchStartLabels.addAll(xMatchStep.matchStartLabels);
            this.matchEndLabels.addAll(xMatchStep.matchEndLabels);
        } else if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
            if (startStep.getLabels().size() > 1)
                throw new IllegalArgumentException("The start step of a match()-traversal can only have one label: " + startStep);
            final String label = startStep.getLabels().iterator().next();
            this.matchStartLabels.add(label);
            TraversalHelper.replaceStep(conjunctionTraversal.getStartStep(), new SelectOneStep<>(conjunctionTraversal, Scope.global, Pop.last, label), conjunctionTraversal);
        }
        // END STEP to XMatchStep
        final Step<?, ?> endStep = conjunctionTraversal.getEndStep();
        if (!(endStep instanceof XMatchStep.XMatchEndStep)) {
            if (endStep.getLabels().size() > 1)
                throw new IllegalArgumentException("The end step of a match()-traversal can have at most one label: " + endStep);
            final String label = endStep.getLabels().size() == 0 ? null : endStep.getLabels().iterator().next();
            if (null != label) endStep.removeLabel(label);
            final Step<?, ?> xMatchEndStep = new XMatchEndStep(conjunctionTraversal, label);
            if (null != label) {
                this.matchEndLabels.add(label);
                xMatchEndStep.addLabel(label);
            }
            conjunctionTraversal.asAdmin().addStep(xMatchEndStep);
        }
    }

    public List<Traversal.Admin<Object, Object>> getGlobalChildren() {
        return Collections.unmodifiableList(this.conjunctionTraversals);
    }

    @Override
    public Scope getScope() {
        return Scope.global;
    }

    @Override
    public Scope recommendNextScope() {
        return Scope.local;
    }

    @Override
    public void setScope(final Scope scope) {

    }

    @Override
    public Set<String> getScopeKeys() {
        final HashSet<String> scopedKeys = new HashSet<>();
        scopedKeys.addAll(this.matchStartLabels);
        scopedKeys.addAll(this.matchEndLabels);
        return scopedKeys;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.conjunction, this.conjunctionTraversals);
    }

    @Override
    public void reset() {
        super.reset();
        //this.first = true;
    }

    @Override
    public XMatchStep<S, E> clone() {
        final XMatchStep<S, E> clone = (XMatchStep<S, E>) super.clone();
        clone.conjunctionTraversals = new ArrayList<>();
        for (final Traversal.Admin<Object, Object> traversal : this.conjunctionTraversals) {
            clone.conjunctionTraversals.add(clone.integrateChild(traversal.clone()));
        }
        // TODO: does it need to clone the match algorithm?
        return clone;
    }

    private boolean hasMatched(final Conjunction conjunction, final Traverser<S> traverser) {
        final Path path = traverser.path();
        int counter = 0;
        for (final Traversal.Admin<Object, Object> conjunctionTraversal : this.conjunctionTraversals) {
            if (path.hasLabel(conjunctionTraversal.getStartStep().getId())) {
                if (conjunction == Conjunction.OR) return true;
                counter++;
            }
        }
        return this.conjunctionTraversals.size() == counter;
    }

    private Map<String, E> getBindings(final Traverser<S> traverser) {
        final Map<String, E> bindings = new HashMap<>();
        traverser.path().forEach((object, labels) -> {
            for (final String label : labels) {
                if (this.matchEndLabels.contains(label)) {
                    bindings.put(label, (E) object);
                } else if (this.matchStartLabels.contains(label)) {
                    bindings.put(label, (E) object);
                }
            }
        });
        return bindings;
    }

    @Override
    protected Iterator<Traverser<Map<String, E>>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            Traverser.Admin traverser = null;
            if (this.first) {
                this.first = false;
                this.matchAlgorithm.initialize(this.conjunctionTraversals);
            } else {
                for (final Traversal.Admin<?, ?> conjunctionTraversal : this.conjunctionTraversals) {
                    if (conjunctionTraversal.hasNext()) {
                        traverser = conjunctionTraversal.getEndStep().next().asAdmin();
                        break;
                    }
                }
            }
            if (null == traverser)
                traverser = this.starts.next();
            else if (hasMatched(this.conjunction, traverser))
                return IteratorUtils.of(traverser.split(this.getBindings(traverser), this));

            if (this.conjunction == Conjunction.AND) {
                final Traversal.Admin<Object, Object> conjunctionTraversal = this.matchAlgorithm.apply(traverser); // determine which sub-pattern the traverser should try next
                traverser.path().addLabel(conjunctionTraversal.getStartStep().getId()); // unique identifier for the traversal match sub-pattern
                conjunctionTraversal.addStart(traverser);  // go down the traversal match sub-pattern
            } else {
                for (final Traversal.Admin<?, ?> conjunctionTraversal : this.conjunctionTraversals) {
                    final Traverser split = traverser.split();
                    split.path().addLabel(conjunctionTraversal.getStartStep().getId());
                    conjunctionTraversal.addStart(split);
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser<Map<String, E>>> computerAlgorithm() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            this.matchAlgorithm.initialize(this.conjunctionTraversals);
        }
        final Traverser.Admin traverser = this.starts.next();
        if (hasMatched(this.conjunction, traverser)) {
            traverser.setStepId(this.getNextStep().getId());
            return IteratorUtils.of(traverser.split(this.getBindings(traverser), this));
        }

        if (this.conjunction == Conjunction.AND) {
            final Traversal.Admin<Object, Object> conjunctionTraversal = this.matchAlgorithm.apply(traverser); // determine which sub-pattern the traverser should try next
            traverser.path().addLabel(conjunctionTraversal.getStartStep().getId()); // unique identifier for the traversal match sub-pattern
            traverser.setStepId(conjunctionTraversal.getStartStep().getId()); // go down the traversal match sub-pattern
            return IteratorUtils.of(traverser);
        } else {
            final List<Traverser<Map<String, E>>> traversers = new ArrayList<>();
            this.conjunctionTraversals.forEach(conjunctionTraversal -> {
                final Traverser.Admin split = traverser.split();
                split.path().addLabel(conjunctionTraversal.getStartStep().getId());
                split.setStepId(conjunctionTraversal.getStartStep().getId());
                traversers.add(split);
            });
            return traversers.iterator();
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.startKey.hashCode() ^ this.conjunctionTraversals.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    //////////////////////////////

    public class XMatchEndStep extends EndStep {

        private final String matchKey;

        public XMatchEndStep(final Traversal.Admin traversal, final String matchKey) {
            super(traversal);
            this.matchKey = matchKey;
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            while (true) {
                final Traverser.Admin<S> start = this.starts.next();
                // no end label
                if (null == this.matchKey) {
                    if (this.traverserStepIdSetByChild) start.setStepId(XMatchStep.this.getId());
                    return start;
                }
                // side-effect check
                final Optional<S> optional = start.getSideEffects().get(this.matchKey);
                if (optional.isPresent() && start.get().equals(optional.get())) {
                    if (this.traverserStepIdSetByChild) start.setStepId(XMatchStep.this.getId());
                    return start;
                }
                // path check
                final Path path = start.path();
                if (!path.hasLabel(this.matchKey) || start.get().equals(path.getSingle(Pop.first, this.matchKey))) {
                    if (this.traverserStepIdSetByChild) start.setStepId(XMatchStep.this.getId());
                    return start;
                }
            }
        }
    }


    //////////////////////////////

    public interface MatchAlgorithm extends Function<Traverser.Admin<Object>, Traversal.Admin<Object, Object>> {

        public static String getStartLabel(final Traversal.Admin<Object, Object> traversal) {
            final Step<?, ?> startStep = traversal.getStartStep();
            if (startStep instanceof XMatchStep)
                return ((XMatchStep) startStep).startKey;
            else if (startStep instanceof SelectOneStep)
                return ((SelectOneStep<?,?>) startStep).getScopeKeys().iterator().next();
            else
                throw new IllegalArgumentException("The provided start step must be either an XMatchStep or a SelectOneStep: " + startStep);
        }

        public void initialize(final List<Traversal.Admin<Object, Object>> traversals);
    }

    public static class GreedyMatchAlgorithm implements MatchAlgorithm {

        private List<Traversal.Admin<Object, Object>> traversals;
        private List<String> traversalLabels = new ArrayList<>();
        private List<String> startLabels = new ArrayList<>();

        @Override
        public void initialize(final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;
            for (final Traversal.Admin<Object, Object> traversal : this.traversals) {
                this.traversalLabels.add(traversal.getStartStep().getId());
                this.startLabels.add(MatchAlgorithm.getStartLabel(traversal));
            }
        }

        @Override
        public Traversal.Admin<Object, Object> apply(final Traverser.Admin<Object> traverser) {
            final Path path = traverser.path();
            for (int i = 0; i < this.traversals.size(); i++) {
                if (path.hasLabel(this.startLabels.get(i)) && !path.hasLabel(this.traversalLabels.get(i))) {
                    return this.traversals.get(i);
                }
            }
            throw new IllegalArgumentException("The provided match pattern is unsolvable: " + this.traversals);
        }
    }

    public static class CountMatchAlgorithm implements MatchAlgorithm {

        private List<Traversal.Admin<Object, Object>> traversals;
        private List<Integer[]> counts = new ArrayList<>();
        private List<String> traversalLabels = new ArrayList<>();
        private List<String> startLabels = new ArrayList<>();

        @Override
        public void initialize(final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;

            for (int i = 0; i < this.traversals.size(); i++) {
                final Traversal.Admin<Object, Object> traversal = this.traversals.get(i);
                this.traversalLabels.add(traversal.getStartStep().getId());
                this.startLabels.add(MatchAlgorithm.getStartLabel(traversal));
                this.counts.add(new Integer[]{i, 0});
            }
        }

        @Override
        public Traversal.Admin<Object, Object> apply(final Traverser.Admin<Object> traverser) {
            Collections.sort(this.counts, (a, b) -> b[1].compareTo(a[1]));
            final Path path = traverser.path();
            for (final Integer[] indexCounts : this.counts) {
                if (path.hasLabel(this.startLabels.get(indexCounts[0])) && !path.hasLabel(this.traversalLabels.get(indexCounts[0]))) {
                    indexCounts[1]++;
                    return this.traversals.get(indexCounts[0]);
                }
            }
            throw new IllegalArgumentException("The provided match pattern is unsolvable: " + this.traversals);
        }
    }
}
