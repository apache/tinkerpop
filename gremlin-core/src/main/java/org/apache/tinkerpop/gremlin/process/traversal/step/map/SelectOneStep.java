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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectOneStep<S, E> extends MapStep<S, E> implements TraversalParent, Scoping, PathProcessor {

    private Scope scope;
    private final Pop pop;
    private final String selectLabel;
    private Traversal.Admin<S, E> selectTraversal = null;

    public SelectOneStep(final Traversal.Admin traversal, final Scope scope, Pop pop, final String selectLabel) {
        super(traversal);
        this.scope = scope;
        this.pop = pop;
        this.selectLabel = selectLabel;
    }

    public SelectOneStep(final Traversal.Admin traversal, final Scope scope, final String selectLabel) {
        this(traversal, scope, null, selectLabel);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final Optional<S> optional = this.getOptionalScopeValueByKey(this.pop, this.selectLabel, traverser);
        return optional.isPresent() ? TraversalUtil.applyNullable(optional.get(), this.selectTraversal) : null;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.scope, this.selectLabel, this.selectTraversal);
    }

    @Override
    public SelectOneStep<S, E> clone() {
        final SelectOneStep<S, E> clone = (SelectOneStep<S, E>) super.clone();
        if (null != this.selectTraversal)
            clone.selectTraversal = clone.integrateChild(this.selectTraversal.clone());
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.scope.hashCode() ^ this.selectLabel.hashCode() ^ (null == this.selectTraversal ? "null".hashCode() : this.selectTraversal.hashCode());
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.selectTraversal ? Collections.emptyList() : Collections.singletonList(this.selectTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> selectTraversal) {
        this.selectTraversal = this.integrateChild(selectTraversal);
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
    }

    @Override
    public Scope recommendNextScope() {
        return Scope.global;
    }

    @Override
    public Scope getScope() {
        return this.scope;
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.singleton(this.selectLabel);
    }
}


