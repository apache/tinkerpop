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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConjunctionP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScopeP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalP;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStep<S> extends FilterStep<S> implements TraversalParent, Scoping {

    protected P<Object> predicate;
    protected Scope scope;

    public WhereStep(final Traversal.Admin traversal, final Scope scope, final Optional<String> startKey, final P<?> predicate) {
        super(traversal);
        this.scope = scope;
        this.predicate = WhereStep.convertToTraversalP(startKey, predicate);
        for (final Traversal.Admin<?, ?> whereTraversal : this.predicate.getTraversals()) {
            //// START STEP
            final Step<?, ?> startStep = whereTraversal.getStartStep();
            if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
                if (startStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The start step of a where()-traversal predicate can only have one label: " + startStep);
                TraversalHelper.replaceStep(whereTraversal.getStartStep(), new SelectOneStep<>(whereTraversal, scope, startStep.getLabels().iterator().next()), whereTraversal);
            }
            //// END STEP
            final Step<?, ?> endStep = whereTraversal.getEndStep();
            if (!endStep.getLabels().isEmpty()) {
                if (endStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The end step of a where()-traversal predicate can only have one label: " + endStep);
                final String label = endStep.getLabels().iterator().next();
                endStep.removeLabel(label);
                whereTraversal.addStep(new IsStep<>(whereTraversal, new ScopeP<>(P.eq(label))));
            }
        }
        this.predicate.getTraversals().forEach(this::integrateChild);
    }

    public WhereStep(final Traversal.Admin traversal, final Scope scope, final P<?> predicate) {
        this(traversal, scope, Optional.empty(), predicate);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        for (final Traversal.Admin<?, ?> traversal : this.predicate.getTraversals()) {
            final Step<?, ?> endStep = traversal.getEndStep();
            if (endStep instanceof IsStep && ((IsStep) endStep).getPredicate() instanceof ScopeP)
                ((ScopeP) ((IsStep) endStep).getPredicate()).bind(this, traverser);
        }
        return this.predicate.getBiPredicate().test(traverser, null);
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.predicate.getTraversals();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.scope, this.predicate);
    }

    @Override
    public Set<String> getScopeKeys() {
        final Set<String> keys = new HashSet<>();
        this.predicate.getTraversals().forEach(traversal -> {
            final Step<?, ?> startStep = traversal.getStartStep();
            final Step<?, ?> endStep = traversal.getEndStep();
            if (startStep instanceof SelectOneStep)
                keys.addAll(((SelectOneStep<?, ?>) startStep).getScopeKeys());
            if (endStep instanceof IsStep && ((IsStep) endStep).getPredicate() instanceof ScopeP)
                keys.add(((ScopeP) ((IsStep) endStep).getPredicate()).getKey());
        });
        return keys;
    }

    @Override
    public WhereStep<S> clone() {
        final WhereStep<S> clone = (WhereStep<S>) super.clone();
        clone.predicate = this.predicate.clone();
        clone.getLocalChildren().forEach(clone::integrateChild);
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.scope.hashCode() ^ this.predicate.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(Scope.local == this.scope ?
                new TraverserRequirement[]{TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS} :
                new TraverserRequirement[]{TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS});
    }

    @Override
    public void setScope(final Scope scope) {
        this.scope = scope;
        for (final Traversal.Admin<?, ?> traversal : this.predicate.getTraversals()) {
            final Step<?, ?> startStep = traversal.getStartStep();
            if (startStep instanceof Scoping)
                ((Scoping) startStep).setScope(scope);
        }
    }

    @Override
    public Scope getScope() {
        return this.scope;
    }

    @Override
    public Scope recommendNextScope() {
        return this.scope;
    }

    private static P convertToTraversalP(final Optional<String> startKey, final P<?> predicate) {
        if (predicate instanceof TraversalP)
            return predicate;
        else if (predicate instanceof ConjunctionP) {
            final List<P<?>> conjunctionPredicates = ((ConjunctionP) predicate).getPredicates();
            final P<?>[] ps = new P[conjunctionPredicates.size()];
            for (int i = 0; i < conjunctionPredicates.size(); i++) {
                ps[i] = convertToTraversalP(startKey, conjunctionPredicates.get(i));
            }
            return predicate instanceof AndP ? new AndP(ps) : new OrP(ps);
        } else {
            final Traversal.Admin<?, ?> whereTraversal = new DefaultGraphTraversal<>();
            // START STEP
            if (startKey.isPresent()) {
                final StartStep<?> startStep = new StartStep<>(whereTraversal);
                startStep.addLabel(startKey.get());
                whereTraversal.addStep(startStep);
            }
            // END STEP
            whereTraversal.addStep(new IsStep<>(whereTraversal, new ScopeP<>(predicate)));
            return new TraversalP(whereTraversal, false);
        }
    }
}
