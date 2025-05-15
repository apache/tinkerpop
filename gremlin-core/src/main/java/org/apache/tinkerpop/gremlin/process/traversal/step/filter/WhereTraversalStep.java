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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereTraversalStep<S> extends FilterStep<S> implements TraversalParent, Scoping, PathProcessor, BinaryReductionStep {

    protected Traversal.Admin<?, ?> whereTraversal;
    protected final Set<String> scopeKeys = new HashSet<>();
    protected Set<String> keepLabels;

    public WhereTraversalStep(final Traversal.Admin traversal, final Traversal<?, ?> whereTraversal) {
        super(traversal);
        this.whereTraversal = whereTraversal.asAdmin();
        this.configureStartAndEndSteps(this.whereTraversal);
        if (this.scopeKeys.isEmpty())
            throw new IllegalArgumentException("A where()-traversal must have at least a start or end label (i.e. variable): " + whereTraversal);
        this.whereTraversal = this.integrateChild(this.whereTraversal);
    }

    private void configureStartAndEndSteps(final Traversal.Admin<?, ?> whereTraversal) {
        ConnectiveStrategy.instance().apply(whereTraversal);
        //// START STEP to WhereStartStep
        final Step<?, ?> startStep = whereTraversal.getStartStep();
        if (startStep instanceof ConnectiveStep || startStep instanceof NotStep) {       // for conjunction- and not-steps
            ((TraversalParent) startStep).getLocalChildren().forEach(this::configureStartAndEndSteps);
        } else if (StartStep.isVariableStartStep(startStep)) {  // as("a").out()... traversals
            final String label = startStep.getLabels().iterator().next();
            this.scopeKeys.add(label);
            TraversalHelper.replaceStep(startStep, new WhereStartStep(whereTraversal, label), whereTraversal);
        } else if (!whereTraversal.getEndStep().getLabels().isEmpty()) {                    // ...out().as("a") traversals
            TraversalHelper.insertBeforeStep(new WhereStartStep(whereTraversal, null), (Step) startStep, whereTraversal);
        }
        //// END STEP to WhereEndStep
        final Step<?, ?> endStep = whereTraversal.getEndStep();
        if (!endStep.getLabels().isEmpty()) {
            if (endStep.getLabels().size() > 1)
                throw new IllegalArgumentException("The end step of a where()-traversal can only have one label: " + endStep);
            final String label = endStep.getLabels().iterator().next();
            this.scopeKeys.add(label);
            endStep.removeLabel(label);
            whereTraversal.addStep(new WhereEndStep(whereTraversal, label));
        }
    }

    @Override
    public ElementRequirement getMaxRequirement() {
        return TraversalHelper.getVariableLocations(this.whereTraversal).contains(Scoping.Variable.START) ?
                PathProcessor.super.getMaxRequirement() :
                ElementRequirement.ID;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return TraversalUtil.test((Traverser.Admin) traverser, this.whereTraversal);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        return null == this.whereTraversal ? Collections.emptyList() : Collections.singletonList(this.whereTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.whereTraversal);
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.unmodifiableSet(this.scopeKeys);
    }

    @Override
    public WhereTraversalStep<S> clone() {
        final WhereTraversalStep<S> clone = (WhereTraversalStep<S>) super.clone();
        clone.whereTraversal = this.whereTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.whereTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.whereTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
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

    //////////////////////////////

    public static class WhereStartStep<S> extends ScalarMapStep<S, Object> implements Scoping {

        private String selectKey;

        public WhereStartStep(final Traversal.Admin traversal, final String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
        }

        @Override
        protected Object map(final Traverser.Admin<S> traverser) {
            if (this.getTraversal().getEndStep() instanceof WhereEndStep)
                ((WhereEndStep) this.getTraversal().getEndStep()).processStartTraverser(traverser);
            else if (this.getTraversal().getEndStep() instanceof ProfileStep && this.getTraversal().getEndStep().getPreviousStep() instanceof WhereEndStep)     // TOTAL SUCKY HACK!
                ((WhereEndStep) this.getTraversal().getEndStep().getPreviousStep()).processStartTraverser(traverser);
            return null == this.selectKey ? traverser.get() : this.getSafeScopeValue(Pop.last, this.selectKey, traverser);
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.selectKey);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ (null == this.selectKey ? "null".hashCode() : this.selectKey.hashCode());
        }

        public void removeScopeKey() {
            this.selectKey = null;
        }

        @Override
        public Set<String> getScopeKeys() {
            return null == this.selectKey ? Collections.emptySet() : Collections.singleton(this.selectKey);
        }
    }

    public static class WhereEndStep extends FilterStep<Object> implements Scoping {

        private final String matchKey;
        private Object matchValue = null;

        public WhereEndStep(final Traversal.Admin traversal, final String matchKey) {
            super(traversal);
            this.matchKey = matchKey;
        }

        public void processStartTraverser(final Traverser.Admin traverser) {
            if (null != this.matchKey)
                this.matchValue = this.getSafeScopeValue(Pop.last, this.matchKey, traverser);
        }

        @Override
        protected boolean filter(final Traverser.Admin<Object> traverser) {
            return null == this.matchKey || traverser.get().equals(this.matchValue);
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.matchKey);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ (null == this.matchKey ? "null".hashCode() : this.matchKey.hashCode());
        }

        @Override
        public Set<String> getScopeKeys() {
            return null == this.matchKey ? Collections.emptySet() : Collections.singleton(this.matchKey);
        }
    }


    //////////////////////////////
}
