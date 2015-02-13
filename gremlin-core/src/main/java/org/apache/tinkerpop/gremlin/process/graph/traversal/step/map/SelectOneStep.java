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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectOneStep<S, E> extends MapStep<S, E> implements TraversalParent, EngineDependent {

    private final String selectLabel;
    private Traversal.Admin<Object, Object> selectTraversal = new IdentityTraversal<>();
    private boolean requiresPaths = false;

    public SelectOneStep(final Traversal.Admin traversal, final String selectLabel) {
        super(traversal);
        this.selectLabel = selectLabel;
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        if (start instanceof Map && ((Map) start).containsKey(this.selectLabel))
            return (E) TraversalUtil.apply(((Map) start).get(this.selectLabel), this.selectTraversal);
        else
            return (E) TraversalUtil.apply(traverser.path().<Object>get(this.selectLabel), this.selectTraversal);
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.requiresPaths = traversalEngine.isComputer() ?
                TraversalHelper.getLabelsUpTo(this, this.traversal.asAdmin()).stream().filter(this.selectLabel::equals).findAny().isPresent() :
                TraversalHelper.getStepsUpTo(this, this.traversal.asAdmin()).stream()
                        .filter(step -> step instanceof CollectingBarrierStep)
                        .filter(step -> TraversalHelper.getLabelsUpTo(step, this.traversal.asAdmin()).stream().filter(this.selectLabel::equals).findAny().isPresent()
                                || (step.getLabel().isPresent() && this.selectLabel.equals(step.getLabel().get()))) // TODO: get rid of this (there is a test case to check it)
                        .findAny().isPresent();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabel, this.selectTraversal);
    }

    @Override
    public SelectOneStep<S, E> clone() throws CloneNotSupportedException {
        final SelectOneStep<S, E> clone = (SelectOneStep<S, E>) super.clone();
        clone.selectTraversal = this.selectTraversal.clone();
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return Collections.singletonList(this.selectTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> selectTraversal) {
        this.selectTraversal = this.integrateChild(selectTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.PATH_ACCESS);
        if (this.requiresPaths) requirements.add(TraverserRequirement.PATH);
        return requirements;
    }
}


