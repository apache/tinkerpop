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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.PathUtil;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
public final class MatchStep<S, E> extends ComputerAwareStep<S, Map<String, E>> implements TraversalParent, Scoping, PathProcessor {

    public enum TraversalType {WHERE_PREDICATE, WHERE_TRAVERSAL, MATCH_TRAVERSAL}

    private List<Traversal.Admin<Object, Object>> matchTraversals;
    private boolean first = true;
    private Set<String> matchStartLabels = new HashSet<>();
    private Set<String> matchEndLabels = new HashSet<>();
    private Set<String> scopeKeys = null;
    private final ConnectiveStep.Connective connective;
    private final String computedStartLabel;
    private MatchAlgorithm matchAlgorithm;
    private Class<? extends MatchAlgorithm> matchAlgorithmClass = CountMatchAlgorithm.class; // default is CountMatchAlgorithm (use MatchAlgorithmStrategy to change)
    private Map<String, Set<String>> referencedLabelsMap; // memoization of referenced labels for MatchEndSteps (Map<startStepId, referencedLabels>)

    private Set<List<Object>> dedups = null;
    private Set<String> dedupLabels = null;
    private Set<String> keepLabels = null;

    public MatchStep(final Traversal.Admin traversal, final ConnectiveStep.Connective connective, final Traversal... matchTraversals) {
        super(traversal);
        this.connective = connective;
        this.matchTraversals = (List) Stream.of(matchTraversals).map(Traversal::asAdmin).collect(Collectors.toList());
        this.matchTraversals.forEach(this::configureStartAndEndSteps); // recursively convert to MatchStep, MatchStartStep, or MatchEndStep
        this.matchTraversals.forEach(this::integrateChild);
        this.standardAlgorithmBarrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        this.computedStartLabel = Helper.computeStartLabel(this.matchTraversals);
    }

    //////////////////
    private String pullOutVariableStartStepToParent(final WhereTraversalStep<?> whereStep) {
        return this.pullOutVariableStartStepToParent(new HashSet<>(), whereStep.getLocalChildren().get(0), true).size() != 1 ? null : pullOutVariableStartStepToParent(new HashSet<>(), whereStep.getLocalChildren().get(0), false).iterator().next();
    }

    private Set<String> pullOutVariableStartStepToParent(final Set<String> selectKeys, final Traversal.Admin<?, ?> traversal, boolean testRun) {
        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof WhereTraversalStep.WhereStartStep && !((WhereTraversalStep.WhereStartStep) startStep).getScopeKeys().isEmpty()) {
            selectKeys.addAll(((WhereTraversalStep.WhereStartStep<?>) startStep).getScopeKeys());
            if (!testRun) ((WhereTraversalStep.WhereStartStep) startStep).removeScopeKey();
        } else if (startStep instanceof ConnectiveStep || startStep instanceof NotStep) {
            ((TraversalParent) startStep).getLocalChildren().forEach(child -> this.pullOutVariableStartStepToParent(selectKeys, child, testRun));
        }
        return selectKeys;
    }
    //////////////////

    private void configureStartAndEndSteps(final Traversal.Admin<?, ?> matchTraversal) {
        ConnectiveStrategy.instance().apply(matchTraversal);
        // START STEP to MatchStep OR MatchStartStep
        final Step<?, ?> startStep = matchTraversal.getStartStep();
        if (startStep instanceof ConnectiveStep) {
            final MatchStep matchStep = new MatchStep(matchTraversal,
                    startStep instanceof AndStep ? ConnectiveStep.Connective.AND : ConnectiveStep.Connective.OR,
                    ((ConnectiveStep<?>) startStep).getLocalChildren().toArray(new Traversal[((ConnectiveStep<?>) startStep).getLocalChildren().size()]));
            TraversalHelper.replaceStep(startStep, matchStep, matchTraversal);
            this.matchStartLabels.addAll(matchStep.matchStartLabels);
            this.matchEndLabels.addAll(matchStep.matchEndLabels);
        } else if (startStep instanceof NotStep) {
            final DefaultTraversal notTraversal = new DefaultTraversal<>();
            TraversalHelper.removeToTraversal(startStep, startStep.getNextStep(), notTraversal);
            matchTraversal.addStep(0, new WhereTraversalStep<>(matchTraversal, notTraversal));
            this.configureStartAndEndSteps(matchTraversal);
        } else if (StartStep.isVariableStartStep(startStep)) {
            final String label = startStep.getLabels().iterator().next();
            this.matchStartLabels.add(label);
            TraversalHelper.replaceStep((Step) matchTraversal.getStartStep(), new MatchStartStep(matchTraversal, label), matchTraversal);
        } else if (startStep instanceof WhereTraversalStep) {  // necessary for GraphComputer so the projection is not select'd from a path
            final WhereTraversalStep<?> whereStep = (WhereTraversalStep<?>) startStep;
            TraversalHelper.insertBeforeStep(new MatchStartStep(matchTraversal, this.pullOutVariableStartStepToParent(whereStep)), (Step) whereStep, matchTraversal);             // where(as('a').out()) -> as('a').where(out())
        } else if (startStep instanceof WherePredicateStep) {  // necessary for GraphComputer so the projection is not select'd from a path
            final WherePredicateStep<?> whereStep = (WherePredicateStep<?>) startStep;
            TraversalHelper.insertBeforeStep(new MatchStartStep(matchTraversal, whereStep.getStartKey().orElse(null)), (Step) whereStep, matchTraversal);   // where('a',eq('b')) --> as('a').where(eq('b'))
            whereStep.removeStartKey();
        } else {
            throw new IllegalArgumentException("All match()-traversals must have a single start label (i.e. variable): " + matchTraversal);
        }
        // END STEP to MatchEndStep
        final Step<?, ?> endStep = matchTraversal.getEndStep();
        if (endStep.getLabels().size() > 1)
            throw new IllegalArgumentException("The end step of a match()-traversal can have at most one label: " + endStep);
        final String label = endStep.getLabels().size() == 0 ? null : endStep.getLabels().iterator().next();
        if (null != label) endStep.removeLabel(label);
        final Step<?, ?> matchEndStep = new MatchEndStep(matchTraversal, label);
        if (null != label) this.matchEndLabels.add(label);
        matchTraversal.asAdmin().addStep(matchEndStep);

        // this turns barrier computations into locally computable traversals
        if (TraversalHelper.getStepsOfAssignableClass(Barrier.class, matchTraversal).stream().
                filter(s -> !(s instanceof NoOpBarrierStep)).findAny().isPresent()) { // exclude NoOpBarrierSteps from the determination as they are optimization barriers
            final Traversal.Admin newTraversal = new DefaultTraversal<>();
            TraversalHelper.removeToTraversal(matchTraversal.getStartStep().getNextStep(), matchTraversal.getEndStep(), newTraversal);
            TraversalHelper.insertAfterStep(new TraversalFlatMapStep<>(matchTraversal, newTraversal), matchTraversal.getStartStep(), matchTraversal);
        }
    }


    public ConnectiveStep.Connective getConnective() {
        return this.connective;
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
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
        if (null != this.dedupLabels)
            this.keepLabels.addAll(this.dedupLabels);
    }

    @Override
    public Set<String> getKeepLabels() {
        return keepLabels;
    }

    public Set<String> getMatchEndLabels() {
        return this.matchEndLabels;
    }

    public Set<String> getMatchStartLabels() {
        return this.matchStartLabels;
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
            this.scopeKeys.remove(this.computedStartLabel);
            this.scopeKeys = Collections.unmodifiableSet(this.scopeKeys);
        }
        return this.scopeKeys;
    }

    @Override
    public Set<ScopingInfo> getScopingInfo() {
        final Set<String> labels = this.getScopeKeys();
        final Set<ScopingInfo> scopingInfoSet = new HashSet<>();
        for (String label : labels) {
            final ScopingInfo scopingInfo = new ScopingInfo();
            scopingInfo.label = label;
            scopingInfo.pop = Pop.last;
            scopingInfoSet.add(scopingInfo);
        }
        return scopingInfoSet;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.dedupLabels, this.connective, this.matchTraversals);
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
        if (null == this.matchAlgorithm)
            this.initializeMatchAlgorithm(this.traverserStepIdAndLabelsSetByChild);
        return this.matchAlgorithm;
    }

    @Override
    public MatchStep<S, E> clone() {
        final MatchStep<S, E> clone = (MatchStep<S, E>) super.clone();
        clone.matchTraversals = new ArrayList<>();
        for (final Traversal.Admin<Object, Object> traversal : this.matchTraversals) {
            clone.matchTraversals.add(traversal.clone());
        }
        if (this.dedups != null) clone.dedups = new HashSet<>();
        clone.standardAlgorithmBarrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        for (final Traversal.Admin<Object, Object> traversal : this.matchTraversals) {
            this.integrateChild(traversal);
        }
    }

    public void setDedupLabels(final Set<String> labels) {
        if (!labels.isEmpty()) {
            this.dedups = new HashSet<>();
            this.dedupLabels = new HashSet<>(labels);
            if (null != this.keepLabels)
                this.keepLabels.addAll(this.dedupLabels);
        }
    }

    /*public boolean isDeduping() {
        return this.dedupLabels != null;
    }*/

    private boolean isDuplicate(final Traverser<S> traverser) {
        if (null == this.dedups)
            return false;
        final Path path = traverser.path();
        for (final String label : this.dedupLabels) {
            if (!path.hasLabel(label))
                return false;
        }
        final List<Object> objects = new ArrayList<>(this.dedupLabels.size());
        for (final String label : this.dedupLabels) {
            objects.add(path.get(Pop.last, label));
        }
        return this.dedups.contains(objects);
    }

    private boolean hasMatched(final ConnectiveStep.Connective connective, final Traverser.Admin<S> traverser) {
        int counter = 0;
        boolean matched = false;
        for (final Traversal.Admin<Object, Object> matchTraversal : this.matchTraversals) {
            if (traverser.getTags().contains(matchTraversal.getStartStep().getId())) {
                if (connective == ConnectiveStep.Connective.OR) {
                    matched = true;
                    break;
                }
                counter++;
            }
        }
        if (!matched)
            matched = this.matchTraversals.size() == counter;
        if (matched && this.dedupLabels != null) {
            final Path path = traverser.path();
            final List<Object> objects = new ArrayList<>(this.dedupLabels.size());
            for (final String label : this.dedupLabels) {
                objects.add(path.get(Pop.last, label));
            }
            this.dedups.add(objects);
        }
        return matched;
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

    private void initializeMatchAlgorithm(final boolean onComputer) {
        try {
            this.matchAlgorithm = this.matchAlgorithmClass.getConstructor().newInstance();
        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        this.matchAlgorithm.initialize(onComputer, this.matchTraversals);
    }

    private boolean hasPathLabel(final Path path, final Set<String> labels) {
        for (final String label : labels) {
            if (path.hasLabel(label))
                return true;
        }
        return false;
    }

    private Map<String, Set<String>> getReferencedLabelsMap() {
        if (null == this.referencedLabelsMap) {
            this.referencedLabelsMap = new HashMap<>();
            for (final Traversal.Admin<?, ?> traversal : this.matchTraversals) {
                final Set<String> referencedLabels = new HashSet<>();
                for (final Step<?, ?> step : traversal.getSteps()) {
                    referencedLabels.addAll(PathUtil.getReferencedLabels(step));
                }
                this.referencedLabelsMap.put(traversal.getStartStep().getId(), referencedLabels);
            }
        }
        return this.referencedLabelsMap;
    }

    private TraverserSet standardAlgorithmBarrier;

    @Override
    protected Iterator<Traverser.Admin<Map<String, E>>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.first) {
                this.first = false;
                this.initializeMatchAlgorithm(false);
                if (null != this.keepLabels &&
                        this.keepLabels.containsAll(this.matchEndLabels) &&
                        this.keepLabels.containsAll(this.matchStartLabels))
                    this.keepLabels = null;
            } else { // TODO: if(standardAlgorithmBarrier.isEmpty()) -- leads to consistent counts without retracting paths, but orders of magnitude slower (or make Traverser.tags an equality concept)
                boolean stop = false;
                for (final Traversal.Admin<?, ?> matchTraversal : this.matchTraversals) {
                    while (matchTraversal.hasNext()) { // TODO: perhaps make MatchStep a LocalBarrierStep ??
                        this.standardAlgorithmBarrier.add(matchTraversal.nextTraverser());
                        if (null == this.keepLabels || this.standardAlgorithmBarrier.size() >= PathRetractionStrategy.MAX_BARRIER_SIZE) {
                            stop = true;
                            break;
                        }
                    }
                    if (stop) break;
                }
            }
            final Traverser.Admin traverser;
            if (this.standardAlgorithmBarrier.isEmpty()) {
                traverser = this.starts.next();
                if (!traverser.getTags().contains(this.getId())) {
                    traverser.getTags().add(this.getId()); // so the traverser never returns to this branch ever again
                    if (!this.hasPathLabel(traverser.path(), this.matchStartLabels))
                        traverser.addLabels(Collections.singleton(this.computedStartLabel)); // if the traverser doesn't have a legal start, then provide it the pre-computed one
                }
            } else
                traverser = this.standardAlgorithmBarrier.remove();

            ///
            if (!this.isDuplicate(traverser)) {
                if (hasMatched(this.connective, traverser))
                    return IteratorUtils.of(traverser.split(this.getBindings(traverser), this));

                if (this.connective == ConnectiveStep.Connective.AND) {
                    final Traversal.Admin<Object, Object> matchTraversal = this.getMatchAlgorithm().apply(traverser);
                    traverser.getTags().add(matchTraversal.getStartStep().getId());
                    matchTraversal.addStart(traverser); // determine which sub-pattern the traverser should try next
                } else {  // OR
                    for (final Traversal.Admin<?, ?> matchTraversal : this.matchTraversals) {
                        final Traverser.Admin split = traverser.split();
                        split.getTags().add(matchTraversal.getStartStep().getId());
                        matchTraversal.addStart(split);
                    }
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser.Admin<Map<String, E>>> computerAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.first) {
                this.first = false;
                this.initializeMatchAlgorithm(true);
                if (null != this.keepLabels &&
                        this.keepLabels.containsAll(this.matchEndLabels) &&
                        this.keepLabels.containsAll(this.matchStartLabels))
                    this.keepLabels = null;
            }
            final Traverser.Admin traverser = this.starts.next();
            if (!traverser.getTags().contains(this.getId())) {
                traverser.getTags().add(this.getId()); // so the traverser never returns to this branch ever again
                if (!this.hasPathLabel(traverser.path(), this.matchStartLabels))
                    traverser.addLabels(Collections.singleton(this.computedStartLabel)); // if the traverser doesn't have a legal start, then provide it the pre-computed one
            }
            ///
            if (!this.isDuplicate(traverser)) {
                if (hasMatched(this.connective, traverser)) {
                    traverser.setStepId(this.getNextStep().getId());
                    traverser.addLabels(this.labels);
                    return IteratorUtils.of(traverser.split(this.getBindings(traverser), this));
                }
                if (this.connective == ConnectiveStep.Connective.AND) {
                    final Traversal.Admin<Object, Object> matchTraversal = this.getMatchAlgorithm().apply(traverser); // determine which sub-pattern the traverser should try next
                    traverser.getTags().add(matchTraversal.getStartStep().getId());
                    traverser.setStepId(matchTraversal.getStartStep().getId()); // go down the traversal match sub-pattern
                    return IteratorUtils.of(traverser);
                } else { // OR
                    final List<Traverser.Admin<Map<String, E>>> traversers = new ArrayList<>(this.matchTraversals.size());
                    for (final Traversal.Admin<?, ?> matchTraversal : this.matchTraversals) {
                        final Traverser.Admin split = traverser.split();
                        split.getTags().add(matchTraversal.getStartStep().getId());
                        split.setStepId(matchTraversal.getStartStep().getId());
                        traversers.add(split);
                    }
                    return traversers.iterator();
                }
            }
        }
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.connective.hashCode();
        for (final Traversal t : this.matchTraversals) {
            result ^= t.hashCode();
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.LABELED_PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    //////////////////////////////

    public static final class MatchStartStep extends AbstractStep<Object, Object> implements Scoping {

        private final String selectKey;
        private Set<String> scopeKeys;
        private MatchStep<?, ?> parent;

        public MatchStartStep(final Traversal.Admin traversal, final String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
        }

        @Override
        protected Traverser.Admin<Object> processNextStart() throws NoSuchElementException {
            if (null == this.parent)
                this.parent = (MatchStep<?, ?>) this.getTraversal().getParent();

            final Traverser.Admin<Object> traverser = this.starts.next();
            this.parent.getMatchAlgorithm().recordStart(traverser, this.getTraversal());
            // TODO: sideEffect check?
            return null == this.selectKey ? traverser : traverser.split(traverser.path().get(Pop.last, this.selectKey), this);
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.selectKey);
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            if (null != this.selectKey)
                result ^= this.selectKey.hashCode();
            return result;
        }

        public Optional<String> getSelectKey() {
            return Optional.ofNullable(this.selectKey);
        }

        @Override
        public Set<String> getScopeKeys() {
            if (null == this.scopeKeys) { // computer the first time and then save resultant keys
                this.scopeKeys = new HashSet<>();
                if (null != this.selectKey)
                    this.scopeKeys.add(this.selectKey);
                final Set<String> endLabels = ((MatchStep<?, ?>) this.getTraversal().getParent()).getMatchEndLabels();
                TraversalHelper.anyStepRecursively(step -> {
                    if (step instanceof WherePredicateStep || step instanceof WhereTraversalStep) {
                        for (final String key : ((Scoping) step).getScopeKeys()) {
                            if (endLabels.contains(key)) this.scopeKeys.add(key);
                        }
                    }
                    return false;
                }, this.getTraversal());
                // this is the old way but it only checked for where() as the next step, not arbitrarily throughout traversal
                // just going to keep this in case something pops up in the future and this is needed as an original reference.
                /* if (this.getNextStep() instanceof WhereTraversalStep || this.getNextStep() instanceof WherePredicateStep)
                   this.scopeKeys.addAll(((Scoping) this.getNextStep()).getScopeKeys());*/
                this.scopeKeys = Collections.unmodifiableSet(this.scopeKeys);
            }
            return this.scopeKeys;
        }
    }

    public static final class MatchEndStep extends EndStep<Object> implements Scoping {

        private final String matchKey;
        private final Set<String> matchKeyCollection;
        private MatchStep<?, ?> parent;

        public MatchEndStep(final Traversal.Admin traversal, final String matchKey) {
            super(traversal);
            this.matchKey = matchKey;
            this.matchKeyCollection = null == matchKey ? Collections.emptySet() : Collections.singleton(this.matchKey);
        }


        private <S> Traverser.Admin<S> retractUnnecessaryLabels(final Traverser.Admin<S> traverser) {
            if (null == this.parent.getKeepLabels())
                return traverser;

            final Set<String> keepers = new HashSet<>(this.parent.getKeepLabels());
            final Set<String> tags = traverser.getTags();
            for (final Traversal.Admin<?, ?> matchTraversal : this.parent.matchTraversals) { // get remaining traversal patterns for the traverser
                final String startStepId = matchTraversal.getStartStep().getId();
                if (!tags.contains(startStepId)) {
                    keepers.addAll(this.parent.getReferencedLabelsMap().get(startStepId)); // get the reference labels required for those remaining traversals
                }
            }
            return PathProcessor.processTraverserPathLabels(traverser, keepers); // remove all reference labels that are no longer required
        }

        @Override
        protected Traverser.Admin<Object> processNextStart() throws NoSuchElementException {
            if (null == this.parent)
                this.parent = ((MatchStep) this.getTraversal().getParent().asStep());

            while (true) {
                final Traverser.Admin traverser = this.starts.next();
                // no end label
                if (null == this.matchKey) {
                    // if (this.traverserStepIdAndLabelsSetByChild) -- traverser equality is based on stepId, lets ensure they are all at the parent
                    traverser.setStepId(this.parent.getId());
                    this.parent.getMatchAlgorithm().recordEnd(traverser, this.getTraversal());
                    return this.retractUnnecessaryLabels(traverser);
                }
                // TODO: sideEffect check?
                // path check
                final Path path = traverser.path();
                if (!path.hasLabel(this.matchKey) || traverser.get().equals(path.get(Pop.last, this.matchKey))) {
                    // if (this.traverserStepIdAndLabelsSetByChild) -- traverser equality is based on stepId and thus, lets ensure they are all at the parent
                    traverser.setStepId(this.parent.getId());
                    traverser.addLabels(this.matchKeyCollection);
                    this.parent.getMatchAlgorithm().recordEnd(traverser, this.getTraversal());
                    return this.retractUnnecessaryLabels(traverser);
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
            int result = super.hashCode();
            if (null != this.matchKey)
                result ^= this.matchKey.hashCode();
            return result;
        }

        @Override
        public Set<String> getScopeKeys() {
            return this.matchKeyCollection;
        }
    }


    //////////////////////////////

    public static final class Helper {
        private Helper() {
        }

        public static Optional<String> getEndLabel(final Traversal.Admin<Object, Object> traversal) {
            final Step<?, ?> endStep = traversal.getEndStep();
            return endStep instanceof ProfileStep ?           // TOTAL HACK
                    ((MatchEndStep) endStep.getPreviousStep()).getMatchKey() :
                    ((MatchEndStep) endStep).getMatchKey();
        }

        public static Set<String> getStartLabels(final Traversal.Admin<Object, Object> traversal) {
            return ((Scoping) traversal.getStartStep()).getScopeKeys();
        }

        public static boolean hasStartLabels(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {
            for (final String label : Helper.getStartLabels(traversal)) {
                if (!traverser.path().hasLabel(label))
                    return false;
            }
            return true;
        }

        public static boolean hasEndLabel(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {
            final Optional<String> endLabel = Helper.getEndLabel(traversal);
            return endLabel.isPresent() && traverser.path().hasLabel(endLabel.get()); // TODO: !isPresent?
        }

        public static boolean hasExecutedTraversal(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {
            return traverser.getTags().contains(traversal.getStartStep().getId());
        }

        public static TraversalType getTraversalType(final Traversal.Admin<Object, Object> traversal) {
            final Step<?, ?> nextStep = traversal.getStartStep().getNextStep();
            if (nextStep instanceof WherePredicateStep)
                return TraversalType.WHERE_PREDICATE;
            else if (nextStep instanceof WhereTraversalStep)
                return TraversalType.WHERE_TRAVERSAL;
            else
                return TraversalType.MATCH_TRAVERSAL;
        }

        public static String computeStartLabel(final List<Traversal.Admin<Object, Object>> traversals) {
            {
                // a traversal start label, that's not used as an end label, must be the step's start label
                final Set<String> startLabels = new HashSet<>();
                final Set<String> endLabels = new HashSet<>();
                for (final Traversal.Admin<Object, Object> traversal : traversals) {
                    Helper.getEndLabel(traversal).ifPresent(endLabels::add);
                    startLabels.addAll(Helper.getStartLabels(traversal));
                }
                startLabels.removeAll(endLabels);
                if (!startLabels.isEmpty())
                    return startLabels.iterator().next();
            }
            final List<String> sort = new ArrayList<>();
            for (final Traversal.Admin<Object, Object> traversal : traversals) {
                Helper.getStartLabels(traversal).stream().filter(startLabel -> !sort.contains(startLabel)).forEach(sort::add);
                Helper.getEndLabel(traversal).ifPresent(endLabel -> {
                    if (!sort.contains(endLabel))
                        sort.add(endLabel);
                });
            }
            Collections.sort(sort, (a, b) -> {
                for (final Traversal.Admin<Object, Object> traversal : traversals) {
                    final Optional<String> endLabel = Helper.getEndLabel(traversal);
                    if (endLabel.isPresent()) {
                        final Set<String> startLabels = Helper.getStartLabels(traversal);
                        if (a.equals(endLabel.get()) && startLabels.contains(b))
                            return 1;
                        else if (b.equals(endLabel.get()) && startLabels.contains(a))
                            return -1;
                    }
                }
                return 0;
            });
            return sort.get(0);
        }
    }


    //////////////////////////////

    public interface MatchAlgorithm extends Function<Traverser.Admin<Object>, Traversal.Admin<Object, Object>>, Serializable {


        public static Function<List<Traversal.Admin<Object, Object>>, IllegalStateException> UNMATCHABLE_PATTERN = traversals -> new IllegalStateException("The provided match pattern is unsolvable: " + traversals);


        public void initialize(final boolean onComputer, final List<Traversal.Admin<Object, Object>> traversals);

        public default void recordStart(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {

        }

        public default void recordEnd(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {

        }
    }

    public static class GreedyMatchAlgorithm implements MatchAlgorithm {

        private List<Traversal.Admin<Object, Object>> traversals;

        @Override
        public void initialize(final boolean onComputer, final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;
        }

        @Override
        public Traversal.Admin<Object, Object> apply(final Traverser.Admin<Object> traverser) {
            for (final Traversal.Admin<Object, Object> traversal : this.traversals) {
                if (!Helper.hasExecutedTraversal(traverser, traversal) && Helper.hasStartLabels(traverser, traversal))
                    return traversal;
            }
            throw UNMATCHABLE_PATTERN.apply(this.traversals);
        }
    }

    public static class CountMatchAlgorithm implements MatchAlgorithm {

        protected List<Bundle> bundles;
        protected int counter = 0;
        protected boolean onComputer;

        public void initialize(final boolean onComputer, final List<Traversal.Admin<Object, Object>> traversals) {
            this.onComputer = onComputer;
            this.bundles = traversals.stream().map(Bundle::new).collect(Collectors.toList());
        }

        @Override
        public Traversal.Admin<Object, Object> apply(final Traverser.Admin<Object> traverser) {
            // optimization to favor processing StarGraph local objects first to limit message passing (GraphComputer only)
            // TODO: generalize this for future MatchAlgorithms (given that 3.2.0 will focus on RealTimeStrategy, it will probably go there)
            if (this.onComputer) {
                final List<Set<String>> labels = traverser.path().labels();
                final Set<String> lastLabels = labels.get(labels.size() - 1);
                Collections.sort(this.bundles,
                        Comparator.<Bundle>comparingLong(b -> Helper.getStartLabels(b.traversal).stream().filter(startLabel -> !lastLabels.contains(startLabel)).count()).
                                thenComparingInt(b -> b.traversalType.ordinal()).
                                thenComparingDouble(b -> b.multiplicity));
            }

            Bundle startLabelsBundle = null;
            for (final Bundle bundle : this.bundles) {
                if (!Helper.hasExecutedTraversal(traverser, bundle.traversal) && Helper.hasStartLabels(traverser, bundle.traversal)) {
                    if (bundle.traversalType != TraversalType.MATCH_TRAVERSAL || Helper.hasEndLabel(traverser, bundle.traversal))
                        return bundle.traversal;
                    else if (null == startLabelsBundle)
                        startLabelsBundle = bundle;
                }
            }
            if (null != startLabelsBundle) return startLabelsBundle.traversal;
            throw UNMATCHABLE_PATTERN.apply(this.bundles.stream().map(record -> record.traversal).collect(Collectors.toList()));
        }

        @Override
        public void recordStart(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {
            this.getBundle(traversal).startsCount++;
        }

        @Override
        public void recordEnd(final Traverser.Admin<Object> traverser, final Traversal.Admin<Object, Object> traversal) {
            this.getBundle(traversal).incrementEndCount();
            if (!this.onComputer) {  // if on computer, sort on a per traverser-basis with bias towards local star graph
                if (this.counter < 200 || this.counter % 250 == 0) // aggressively sort for the first 200 results -- after that, sort every 250
                    Collections.sort(this.bundles, Comparator.<Bundle>comparingInt(b -> b.traversalType.ordinal()).thenComparingDouble(b -> b.multiplicity));
                this.counter++;
            }
        }

        protected Bundle getBundle(final Traversal.Admin<Object, Object> traversal) {
            for (final Bundle bundle : this.bundles) {
                if (bundle.traversal == traversal)
                    return bundle;
            }
            throw new IllegalStateException("No equivalent traversal could be found in " + CountMatchAlgorithm.class.getSimpleName() + ": " + traversal);
        }

        ///////////

        public class Bundle {
            public Traversal.Admin<Object, Object> traversal;
            public TraversalType traversalType;
            public long startsCount;
            public long endsCount;
            public double multiplicity;

            public Bundle(final Traversal.Admin<Object, Object> traversal) {
                this.traversal = traversal;
                this.traversalType = Helper.getTraversalType(traversal);
                this.startsCount = 0l;
                this.endsCount = 0l;
                this.multiplicity = 0.0d;
            }

            public final void incrementEndCount() {
                this.multiplicity = (double) ++this.endsCount / (double) this.startsCount;
            }
        }
    }
}
