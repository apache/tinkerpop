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

import org.apache.tinkerpop.gremlin.process.Path;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements TraversalParent, EngineDependent {

    protected TraversalRing<Object, Object> traversalRing = new TraversalRing<>();
    private final List<String> selectLabels;
    private final boolean wasEmpty;
    private boolean requiresPaths = false;
    protected Function<Traverser<S>, Map<String, E>> selectFunction;

    public SelectStep(final Traversal.Admin traversal, final String... selectLabels) {
        super(traversal);
        this.wasEmpty = selectLabels.length == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabelsUpTo(this, this.traversal) : Arrays.asList(selectLabels);
        SelectStep.generateFunction(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.requiresPaths = traversalEngine.equals(TraversalEngine.COMPUTER) ?
                TraversalHelper.getLabelsUpTo(this, this.traversal.asAdmin()).stream().filter(this.selectLabels::contains).findAny().isPresent() :
                TraversalHelper.getStepsUpTo(this, this.traversal.asAdmin()).stream()
                        .filter(step -> step instanceof CollectingBarrierStep)
                        .filter(step -> TraversalHelper.getLabelsUpTo(step, this.traversal.asAdmin()).stream().filter(this.selectLabels::contains).findAny().isPresent()
                                || (step.getLabel().isPresent() && this.selectLabels.contains(step.getLabel().get()))) // TODO: get rid of this (there is a test case to check it)
                        .findAny().isPresent();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabels, this.traversalRing);
    }

    @Override
    public SelectStep<S, E> clone() throws CloneNotSupportedException {
        final SelectStep<S, E> clone = (SelectStep<S, E>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            clone.traversalRing.addTraversal(clone.integrateChild(traversal.clone(), TYPICAL_LOCAL_OPERATIONS));
        }
        SelectStep.generateFunction(clone);
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal, TYPICAL_LOCAL_OPERATIONS));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.PATH_ACCESS);
        if (this.requiresPaths) requirements.add(TraverserRequirement.PATH);
        return requirements;
    }

    //////////////////////

    private static final <S, E> void generateFunction(final SelectStep<S, E> selectStep) {
        selectStep.selectFunction = traverser -> {
            final S start = traverser.get();
            final Map<String, E> bindings = new LinkedHashMap<>();

            ////// PROCESS STEP BINDINGS
            final Path path = traverser.path();
            selectStep.selectLabels.forEach(label -> {
                if (path.hasLabel(label))
                    bindings.put(label, (E) TraversalUtil.apply(path.<Object>get(label), selectStep.traversalRing.next()));
            });

            ////// PROCESS MAP BINDINGS
            if (start instanceof Map) {
                if (selectStep.wasEmpty)
                    ((Map<String, Object>) start).forEach((k, v) -> bindings.put(k, (E) TraversalUtil.apply(v, selectStep.traversalRing.next())));
                else
                    selectStep.selectLabels.forEach(label -> {
                        if (((Map) start).containsKey(label)) {
                            bindings.put(label, (E) TraversalUtil.apply(((Map) start).get(label), selectStep.traversalRing.next()));
                        }
                    });
            }

            selectStep.traversalRing.reset();
            return bindings;
        };
        selectStep.setFunction(selectStep.selectFunction);
    }
}
