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
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalP;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStep<S> extends FilterStep<S> implements TraversalParent, Scoping {

    protected P<Object> predicate;
    protected String startKey;
    protected String endKey;
    protected Scope scope;

    public WhereStep(final Traversal.Admin traversal, final Scope scope, final Optional<String> startKey, final P<?> predicate) {
        super(traversal);
        this.scope = scope;
        this.predicate = (P) predicate;
        if (!this.predicate.getTraversals().isEmpty()) {
            final Traversal.Admin<?, ?> whereTraversal = predicate.getTraversals().get(0);
            //// START STEP
            final Step<?, ?> startStep = whereTraversal.getStartStep();
            if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
                if (startStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The start step of a where()-traversal predicate can only have one label: " + startStep);
                TraversalHelper.replaceStep(whereTraversal.getStartStep(), new SelectOneStep<>(whereTraversal, scope, this.startKey = startStep.getLabels().iterator().next()), whereTraversal);
            }
            //// END STEP
            final Step<?, ?> endStep = whereTraversal.getEndStep();
            if (!endStep.getLabels().isEmpty()) {
                if (endStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The end step of a where()-traversal predicate can only have one label: " + endStep);
                this.endKey = endStep.getLabels().iterator().next();
            }
            this.predicate.getTraversals().forEach(this::integrateChild);
        } else {
            this.startKey = startKey.orElse(null);
            this.endKey = (String) (this.predicate.getValue() instanceof Collection ? ((Collection) this.predicate.getValue()).iterator().next() : this.predicate.getValue());
        }
    }

    public WhereStep(final Traversal.Admin traversal, final Scope scope, final P<?> predicate) {
        this(traversal, scope, Optional.empty(), predicate);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        if (this.predicate instanceof TraversalP) {
            return this.predicate.getBiPredicate().test(traverser, this.getOptionalScopeValueByKey(this.endKey, traverser).orElse(null));
        } else {
            final Object startObject = null == this.startKey ? traverser.get() : this.getOptionalScopeValueByKey(this.startKey, traverser).orElse(null);
            if (null == startObject) return false;
            final Object endObject;
            if (null == this.endKey) {
                endObject = null;
            } else {
                endObject = this.getOptionalScopeValueByKey(this.endKey, traverser).orElse(null);
                if (null == endObject) return false;
            }
            return this.predicate.getBiPredicate().test(startObject, endObject);
        }
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.predicate.getTraversals();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.scope, this.startKey, this.predicate);
    }

    @Override
    public Set<String> getScopeKeys() {
        final Set<String> keys = new HashSet<>();
        if (null != this.startKey)
            keys.add(this.startKey);
        if (null != this.endKey)
            keys.add(this.endKey);
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
        int result = super.hashCode() ^ this.scope.hashCode() ^ predicate.hashCode();
        if (this.startKey != null) result ^= this.startKey.hashCode();
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(Scope.local == this.scope || (this.endKey == null && this.startKey == null) ?
                new TraverserRequirement[]{TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS} :
                new TraverserRequirement[]{TraverserRequirement.OBJECT, TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS});
    }

    @Override
    public void setScope(final Scope scope) {
        this.scope = scope;
        if (this.predicate instanceof TraversalP) {
            final Step startStep = this.predicate.getTraversals().get(0).getStartStep();
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
}
