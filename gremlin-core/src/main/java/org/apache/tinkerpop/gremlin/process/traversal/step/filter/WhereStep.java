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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TraversalBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.P;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStep<S> extends FilterStep<S> implements TraversalParent, Scoping {

    private P predicate;
    private final String startKey;
    private final String endKey;
    private Scope scope;


    public WhereStep(final Traversal.Admin traversal, final Scope scope, final Optional<String> startKey, final P<?> predicate) {
        super(traversal);
        this.scope = scope;
        this.predicate = predicate;
        if (this.predicate.getBiPredicate() instanceof TraversalBiPredicate) {
            final Traversal<?, ?> whereTraversal = ((TraversalBiPredicate) this.predicate.getBiPredicate()).getTraversal();
            this.startKey = whereTraversal.asAdmin().getStartStep().getLabels().isEmpty() ? null : whereTraversal.asAdmin().getStartStep().getLabels().iterator().next();
            this.endKey = whereTraversal.asAdmin().getEndStep().getLabels().isEmpty() ? null : whereTraversal.asAdmin().getEndStep().getLabels().iterator().next();
            this.integrateChild(whereTraversal.asAdmin());
        } else {
            this.startKey = startKey.orElse(null);
            this.endKey = this.predicate.getValue() instanceof Collection ? ((Collection<String>) this.predicate.getValue()).iterator().next() : (String) this.predicate.getValue();
        }
    }

    public WhereStep(final Traversal.Admin traversal, final Scope scope, final P<?> predicate) {
        this(traversal, scope, Optional.empty(), predicate);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        final Object startObject;
        final Object endObject;

        if (this.noStartAndEndKeys()) {
            startObject = traverser.get();
            endObject = null;
        } else {
            if (Scope.local == this.scope) {
                final Map<String, Object> map = (Map<String, Object>) traverser.get();
                startObject = null == this.startKey ? traverser.get() : map.get(this.startKey);
                endObject = null == this.endKey ? null : map.get(this.endKey);
            } else {
                final Path path = traverser.path();
                startObject = null == this.startKey ? traverser.get() : path.hasLabel(this.startKey) ? path.get(this.startKey) : traverser.sideEffects(this.startKey);
                endObject = null == this.endKey ? null : path.hasLabel(this.endKey) ? path.get(this.endKey) : traverser.sideEffects(this.endKey);
            }
        }

        return this.predicate.getBiPredicate().test(startObject, endObject);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        return this.predicate.getBiPredicate() instanceof TraversalBiPredicate ? Collections.singletonList(((TraversalBiPredicate) this.predicate.getBiPredicate()).getTraversal()) : Collections.emptyList();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.scope, this.startKey, this.predicate);
    }

    @Override
    public WhereStep<S> clone() {
        final WhereStep<S> clone = (WhereStep<S>) super.clone();
        if (this.predicate.getBiPredicate() instanceof TraversalBiPredicate)
            clone.predicate = P.traversal(((TraversalBiPredicate) this.predicate.getBiPredicate()).getTraversal().clone());
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(Scope.local == this.scope || this.noStartAndEndKeys() ?
                TraverserRequirement.OBJECT : TraverserRequirement.OBJECT, TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    public void setScope(final Scope scope) {
        this.scope = scope;
    }

    @Override
    public Scope recommendNextScope() {
        return this.scope;
    }

    private boolean noStartAndEndKeys() {
        return null == this.endKey && null == this.startKey;
    }
}
