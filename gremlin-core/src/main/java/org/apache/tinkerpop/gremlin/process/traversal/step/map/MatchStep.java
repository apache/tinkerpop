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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConjunctionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
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
public final class MatchStep<S, E> extends ComputerAwareStep<S, Map<String, E>> implements TraversalParent, Scoping {

    private List<Traversal.Admin<Object, Object>> matchTraversals = new ArrayList<>();
    private boolean first = true;
    private Set<String> matchStartLabels = new HashSet<>();
    private Set<String> matchEndLabels = new HashSet<>();
    private Set<String> scopeKeys = null;
    private final ConjunctionStep.Conjunction conjunction;
    private final String startKey;
    private MatchAlgorithm matchAlgorithm;
    private Class<? extends MatchAlgorithm> matchAlgorithmClass = CountMatchAlgorithm.class; // default is CountMatchAlgorithm (use MatchAlgorithmStrategy to change)

    public MatchStep(final Traversal.Admin traversal, final String startKey, final ConjunctionStep.Conjunction conjunction, final Traversal... matchTraversals) {
        super(traversal);
        this.conjunction = conjunction;
        this.startKey = startKey;
        if (null != this.startKey) {
            if (this.traversal.getEndStep() instanceof StartStep)  // in case a match() is after the start step
                this.traversal.addStep(new IdentityStep<>(this.traversal));
            this.traversal.getEndStep().addLabel(this.startKey);
        }
        this.matchTraversals = (List) Stream.of(matchTraversals).map(Traversal::asAdmin).collect(Collectors.toList());
        this.matchTraversals.forEach(this::configureStartAndEndSteps); // recursively convert to MatchStep, MatchStartStep, or MatchEndStep
        this.matchTraversals.forEach(this::integrateChild);
    }

    private void configureStartAndEndSteps(final Traversal.Admin<?, ?> matchTraversal) {
        ConjunctionStrategy.instance().apply(matchTraversal);
        // START STEP to XMatchStep OR XMatchStartStep
        final Step<?, ?> startStep = matchTraversal.getStartStep();
        if (startStep instanceof ConjunctionStep) {
            final MatchStep matchStep = new MatchStep(matchTraversal, null,
                    startStep instanceof AndStep ? ConjunctionStep.Conjunction.AND : ConjunctionStep.Conjunction.OR,
                    ((ConjunctionStep<?>) startStep).getLocalChildren().toArray(new Traversal[((ConjunctionStep<?>) startStep).getLocalChildren().size()]));
            TraversalHelper.replaceStep(startStep, matchStep, matchTraversal);
            this.matchStartLabels.addAll(matchStep.matchStartLabels);
            this.matchEndLabels.addAll(matchStep.matchEndLabels);
        } else if (startStep instanceof StartStep) {
            if (startStep.getLabels().size() != 1)
                throw new IllegalArgumentException("The start step of a match()-traversal must have one and only one label: " + startStep);
            final String label = startStep.getLabels().iterator().next();
            this.matchStartLabels.add(label);
            TraversalHelper.replaceStep((Step) matchTraversal.getStartStep(), new MatchStartStep(matchTraversal, label), matchTraversal);
        } else if (startStep instanceof WhereStep) {
            final WhereStep<?> whereStep = (WhereStep<?>) startStep;
            if (whereStep.getStartKey().isPresent()) {           // where('a',eq('b')) --> as('a').where(eq('b'))
                TraversalHelper.insertBeforeStep(new MatchStartStep(matchTraversal, whereStep.getStartKey().get()), (Step) whereStep, matchTraversal);
                whereStep.removeStartKey();
            } else if (!whereStep.getLocalChildren().isEmpty()) { // where(as('a').out()) -> as('a').where(out())
                final Traversal.Admin<?, ?> whereTraversal = whereStep.getLocalChildren().get(0);
                if (whereTraversal.getStartStep() instanceof WhereStep.WhereStartStep && !((WhereStep.WhereStartStep) whereTraversal.getStartStep()).getScopeKeys().isEmpty()) {
                    TraversalHelper.insertBeforeStep(new MatchStartStep(matchTraversal, ((WhereStep.WhereStartStep<?>) whereTraversal.getStartStep()).getScopeKeys().iterator().next()), (Step) whereStep, matchTraversal);
                    ((WhereStep.WhereStartStep) whereTraversal.getStartStep()).removeScopeKey();
                }
            }
        } else {
            throw new IllegalArgumentException("All match()-traversals must have a start label (i.e. variable): " + matchTraversal);
        }
        // END STEP to XMatchEndStep
        final Step<?, ?> endStep = matchTraversal.getEndStep();
        if (!(endStep instanceof MatchStep.MatchEndStep)) {
            if (endStep.getLabels().size() > 1)
                throw new IllegalArgumentException("The end step of a match()-traversal can have at most one label: " + endStep);
            final String label = endStep.getLabels().size() == 0 ? null : endStep.getLabels().iterator().next();
            if (null != label) endStep.removeLabel(label);
            final Step<?, ?> xMatchEndStep = new MatchEndStep(matchTraversal, label);
            if (null != label) this.matchEndLabels.add(label);
            matchTraversal.asAdmin().addStep(xMatchEndStep);
        }
        // this turns barrier computations into locally computable traversals
        if (!TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, matchTraversal).isEmpty()) {
            final Traversal.Admin newTraversal = new DefaultTraversal<>();
            TraversalHelper.removeToTraversal(matchTraversal.getStartStep().getNextStep(), matchTraversal.getEndStep(), newTraversal);
            TraversalHelper.insertAfterStep(new TraversalFlatMapStep<>(matchTraversal, newTraversal), matchTraversal.getStartStep(), matchTraversal);
        }
    }

    public ConjunctionStep.Conjunction getConjunction() {
        return this.conjunction;
    }

    public void addGlobalChild(final Traversal.Admin<?, ?> globalChildTraversal) {
        this.configureStartAndEndSteps(globalChildTraversal);
        this.matchTraversals.add(this.integrateChild(globalChildTraversal));
    }

    @Override
    public void removeGlobalChild(final Traversal.Admin<?, ?> globalChildTraversal) {
        this.matchTraversals.remove(globalChildTraversal);
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getGlobalChildren() {
        return Collections.unmodifiableList(this.matchTraversals);
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

    public Optional<String> getStartKey() {
        return Optional.ofNullable(this.startKey);
    }

    @Override
    public Set<String> getScopeKeys() {
        if (null == this.scopeKeys) {
            this.scopeKeys = new HashSet<>();
            this.matchTraversals.forEach(traversal -> {
                if (traversal.getStartStep() instanceof Scoping)
                    this.scopeKeys.addAll(((Scoping) traversal.getStartStep()).getScopeKeys());
                if (traversal.getEndStep() instanceof Scoping)
                    this.scopeKeys.addAll(((Scoping) traversal.getEndStep()).getScopeKeys());
            });
            this.scopeKeys.removeAll(this.matchEndLabels);
            this.scopeKeys.remove(this.startKey);
        }
        return this.scopeKeys;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.startKey, this.conjunction, this.matchTraversals);
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
    }

    public void setMatchAlgorithm(final Class<? extends MatchAlgorithm> matchAlgorithmClass) {
        this.matchAlgorithmClass = matchAlgorithmClass;
    }

    public MatchAlgorithm getMatchAlgorithm() {
        return this.matchAlgorithm;
    }

    @Override
    public MatchStep<S, E> clone() {
        final MatchStep<S, E> clone = (MatchStep<S, E>) super.clone();
        clone.matchTraversals = new ArrayList<>();
        for (final Traversal.Admin<Object, Object> traversal : this.matchTraversals) {
            clone.matchTraversals.add(clone.integrateChild(traversal.clone()));
        }
        clone.initializeMatchAlgorithm();
        return clone;
    }

    private boolean hasMatched(final ConjunctionStep.Conjunction conjunction, final Traverser<S> traverser) {
        final Path path = traverser.path();
        int counter = 0;
        for (final Traversal.Admin<Object, Object> matchTraversal : this.matchTraversals) {
            if (path.hasLabel(matchTraversal.getStartStep().getId())) {
                if (conjunction == ConjunctionStep.Conjunction.OR) return true;
                counter++;
            }
        }
        return this.matchTraversals.size() == counter;
    }

    private Map<String, E> getBindings(final Traverser<S> traverser) {
        final Map<String, E> bindings = new HashMap<>();
        traverser.path().forEach((object, labels) -> {
            for (final String label : labels) {
                if (this.matchStartLabels.contains(label) || this.matchEndLabels.contains(label))
                    bindings.put(label, (E) object);
            }
        });
        return bindings;
    }

    private void initializeMatchAlgorithm() {
        try {
            this.matchAlgorithm = this.matchAlgorithmClass.getConstructor().newInstance();
        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        this.matchAlgorithm.initialize(this.matchTraversals);
    }

    @Override
    protected Iterator<Traverser<Map<String, E>>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            Traverser.Admin traverser = null;
            if (this.first) {
                this.first = false;
                this.initializeMatchAlgorithm();
            } else {
                for (final Traversal.Admin<?, ?> matchTraversal : this.matchTraversals) {
                    if (matchTraversal.hasNext()) {
                        traverser = matchTraversal.getEndStep().next().asAdmin();
                        break;
                    }
                }
            }
            if (null == traverser) {
                traverser = this.starts.next();
                traverser.path().addLabel(this.getId()); // so the traverser never returns to this branch ever again
            } else if (hasMatched(this.conjunction, traverser))
                return IteratorUtils.of(traverser.split(this.getBindings(traverser), this));

            if (this.conjunction == ConjunctionStep.Conjunction.AND) {
                this.getMatchAlgorithm().apply(traverser).addStart(traverser); // determine which sub-pattern the traverser should try next
            } else {  // OR
                for (final Traversal.Admin<?, ?> matchTraversal : this.matchTraversals) {
                    matchTraversal.addStart(traverser.split());
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser<Map<String, E>>> computerAlgorithm() throws NoSuchElementException {
        final Traverser.Admin traverser = this.starts.next();
        if (!traverser.path().hasLabel(this.getId()))
            traverser.path().addLabel(this.getId()); // so the traverser never returns to this branch ever again
        if (hasMatched(this.conjunction, traverser)) {
            traverser.setStepId(this.getNextStep().getId());
            return IteratorUtils.of(traverser.split(this.getBindings(traverser), this));
        }
        if (this.conjunction == ConjunctionStep.Conjunction.AND) {
            final Traversal.Admin<Object, Object> matchTraversal = this.getMatchAlgorithm().apply(traverser); // determine which sub-pattern the traverser should try next
            traverser.setStepId(matchTraversal.getStartStep().getId()); // go down the traversal match sub-pattern
            return IteratorUtils.of(traverser);
        } else { // OR
            final List<Traverser<Map<String, E>>> traversers = new ArrayList<>(this.matchTraversals.size());
            this.matchTraversals.forEach(matchTraversal -> {
                final Traverser.Admin split = traverser.split();
                split.setStepId(matchTraversal.getStartStep().getId());
                traversers.add(split);
            });
            return traversers.iterator();
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.matchTraversals.hashCode() ^ this.conjunction.hashCode() ^ (null == this.startKey ? "null".hashCode() : this.startKey.hashCode());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    //////////////////////////////

    public static class MatchStartStep extends AbstractStep<Object, Object> implements Scoping {

        private final String selectKey;
        private Set<String> scopeKeys = null;

        public MatchStartStep(final Traversal.Admin traversal, final String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
        }

        @Override
        protected Traverser<Object> processNextStart() throws NoSuchElementException {
            final Traverser.Admin<Object> traverser = this.starts.next();
            traverser.path().addLabel(this.getId());
            ((MatchStep<?, ?>) this.getTraversal().getParent()).getMatchAlgorithm().recordStart(traverser, this.getTraversal());
            // TODO: sideEffect check?
            return traverser.split(traverser.path().get(Pop.last, this.selectKey), this);
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.selectKey);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.selectKey.hashCode();
        }

        @Override
        public Scope getScope() {
            return Scope.global;
        }

        @Override
        public Scope recommendNextScope() {
            return Scope.global;
        }

        @Override
        public void setScope(final Scope scope) {

        }

        public String getSelectKey() {
            return this.selectKey;
        }

        @Override
        public Set<String> getScopeKeys() {
            if (null == this.scopeKeys) {
                this.scopeKeys = new HashSet<>();
                this.scopeKeys.add(this.selectKey);
                this.getTraversal().getSteps().forEach(step -> {
                    if (step instanceof MatchStep || step instanceof WhereStep)
                        this.scopeKeys.addAll(((Scoping) step).getScopeKeys());
                });
            }
            return this.scopeKeys;
            // return Collections.singleton(this.selectKey) TODO: why not work?
        }
    }

    public static class MatchEndStep extends EndStep<Object> {

        private final String matchKey;

        public MatchEndStep(final Traversal.Admin traversal, final String matchKey) {
            super(traversal);
            this.matchKey = matchKey;
        }

        @Override
        protected Traverser<Object> processNextStart() throws NoSuchElementException {
            while (true) {
                final Traverser.Admin traverser = this.starts.next();
                // no end label
                if (null == this.matchKey) {
                    if (this.traverserStepIdSetByChild)
                        traverser.setStepId(((MatchStep<?, ?>) this.getTraversal().getParent()).getId());
                    ((MatchStep<?, ?>) this.getTraversal().getParent()).getMatchAlgorithm().recordEnd(traverser, this.getTraversal());
                    return traverser;
                }
                // TODO: sideEffect check?
                // path check
                final Path path = traverser.path();
                if (!path.hasLabel(this.matchKey) || traverser.get().equals(path.get(Pop.last, this.matchKey))) {
                    if (this.traverserStepIdSetByChild)
                        traverser.setStepId(((MatchStep<?, ?>) this.getTraversal().getParent()).getId());
                    traverser.path().addLabel(this.matchKey);
                    ((MatchStep<?, ?>) this.getTraversal().getParent()).getMatchAlgorithm().recordEnd(traverser, this.getTraversal());
                    return traverser;
                }
            }
        }

        public Optional<String> getMatchKey() {
            return Optional.ofNullable(this.matchKey);
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.matchKey);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ (null == this.matchKey ? "null".hashCode() : this.matchKey.hashCode());
        }
    }


    //////////////////////////////

    public interface MatchAlgorithm extends Function<Traverser.Admin<Object>, Traversal.Admin<Object, Object>>, Serializable {

        public static Function<List<Traversal.Admin<Object, Object>>, IllegalStateException> UNMATCHABLE_PATTERN = traversals -> new IllegalStateException("The provided match pattern is unsolvable: " + traversals);

        public static Set<String> getRequiredLabels(final Traversal.Admin<Object, Object> traversal) {
            final Step<?, ?> startStep = traversal.getStartStep();
            if (startStep instanceof Scoping)
                return ((Scoping) startStep).getScopeKeys();
            else
                throw new IllegalArgumentException("The provided start step must be a scoping step: " + startStep);
        }

        public static boolean isWhereTraversal(final Traversal.Admin<Object, Object> traversal) {
            return traversal.getStartStep() instanceof WhereStep || traversal.getStartStep().getNextStep() instanceof WhereStep;
        }

        public void initialize(final List<Traversal.Admin<Object, Object>> traversals);

        public default void recordStart(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {

        }

        public default void recordEnd(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {

        }
    }

    public static class GreedyMatchAlgorithm implements MatchAlgorithm {

        private List<Traversal.Admin<Object, Object>> traversals;
        private List<String> traversalLabels;
        private List<Set<String>> requiredLabels;

        @Override
        public void initialize(final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;
            this.traversalLabels = new ArrayList<>();
            this.requiredLabels = new ArrayList<>();
            for (final Traversal.Admin<Object, Object> traversal : this.traversals) {
                this.traversalLabels.add(traversal.getStartStep().getId());
                this.requiredLabels.add(MatchAlgorithm.getRequiredLabels(traversal));
            }
        }

        @Override
        public Traversal.Admin<Object, Object> apply(final Traverser.Admin<Object> traverser) {
            final Path path = traverser.path();
            for (int i = 0; i < this.traversals.size(); i++) {
                if (!path.hasLabel(this.traversalLabels.get(i)) && !this.requiredLabels.get(i).stream().filter(label -> !path.hasLabel(label)).findAny().isPresent()) {
                    return this.traversals.get(i);
                }
            }
            throw UNMATCHABLE_PATTERN.apply(this.traversals);
        }
    }

    public static class CountMatchAlgorithm implements MatchAlgorithm {

        protected List<Traversal.Admin<Object, Object>> traversals;
        protected List<Integer[]> counts;
        protected List<String> traversalLabels;
        protected List<Set<String>> requiredLabels;
        protected Map<Traversal.Admin<Object, Object>, Boolean> whereTraversals;

        @Override
        public void initialize(final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;
            this.whereTraversals = new HashMap<>();
            this.counts = new ArrayList<>();
            this.traversalLabels = new ArrayList<>();
            this.requiredLabels = new ArrayList<>();
            for (int i = 0; i < this.traversals.size(); i++) {
                final Traversal.Admin<Object, Object> traversal = this.traversals.get(i);
                this.traversalLabels.add(traversal.getStartStep().getId());
                this.requiredLabels.add(MatchAlgorithm.getRequiredLabels(traversal));
                this.counts.add(new Integer[]{i, 0});
                this.whereTraversals.put(traversal, MatchAlgorithm.isWhereTraversal(traversal));
            }
        }

        @Override
        public Traversal.Admin<Object, Object> apply(final Traverser.Admin<Object> traverser) {
            final Path path = traverser.path();
            for (final Integer[] indexCounts : this.counts) {
                if (!path.hasLabel(this.traversalLabels.get(indexCounts[0])) && !this.requiredLabels.get(indexCounts[0]).stream().filter(label -> !path.hasLabel(label)).findAny().isPresent()) {
                    return this.traversals.get(indexCounts[0]);
                }
            }
            throw UNMATCHABLE_PATTERN.apply(this.traversals);
        }

        public void recordEnd(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {
            final int currentIndex = this.traversals.indexOf(traversal);
            this.counts.stream().filter(array -> currentIndex == array[0]).findAny().get()[1]++;
            Collections.sort(this.counts, (a, b) -> this.whereTraversals.get(traversal) ? 1 : a[1].compareTo(b[1]));
        }
    }
}
