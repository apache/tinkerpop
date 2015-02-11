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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.apache.tinkerpop.gremlin.process.Step;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStep<E> extends FilterStep<Map<String, E>> implements TraversalParent {

    private final String firstKey;
    private final String secondKey;
    private BiPredicate biPredicate;
    private Traversal.Admin constraint;


    public WhereStep(final Traversal.Admin traversal, final String firstKey, final String secondKey, final BiPredicate<E, E> biPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = secondKey;
        this.biPredicate = biPredicate;
        this.constraint = null;
        WhereStep.generatePredicate(this);

    }

    public WhereStep(final Traversal.Admin traversal, final Traversal.Admin constraint) {
        super(traversal);
        this.firstKey = null;
        this.secondKey = null;
        this.biPredicate = null;
        this.constraint = constraint;
        this.integrateChild(this.constraint, Operation.SET_PARENT);
        WhereStep.generatePredicate(this);
    }

    @Override
    public List<Traversal.Admin> getLocalChildren() {
        return null == this.constraint ? Collections.emptyList() : Collections.singletonList(this.constraint);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.firstKey, this.biPredicate, this.secondKey, this.constraint);
    }

    @Override
    public WhereStep<E> clone() throws CloneNotSupportedException {
        final WhereStep<E> clone = (WhereStep<E>) super.clone();
        if (null != this.constraint)
            clone.constraint = clone.integrateChild(this.constraint.clone(), TYPICAL_LOCAL_OPERATIONS);
        WhereStep.generatePredicate(clone);
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    /////////////////////////

    private static final <E> void generatePredicate(final WhereStep<E> whereStep) {
        if (null == whereStep.constraint) {
            whereStep.setPredicate(traverser -> {
                final Map<String, E> map = traverser.get();
                if (!map.containsKey(whereStep.firstKey))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + whereStep.firstKey);
                if (!map.containsKey(whereStep.secondKey))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + whereStep.secondKey);
                return whereStep.biPredicate.test(map.get(whereStep.firstKey), map.get(whereStep.secondKey));
            });
        } else {
            final Step<?, ?> startStep = whereStep.constraint.getStartStep();
            final Step<?, ?> endStep = whereStep.constraint.getEndStep();
            whereStep.setPredicate(traverser -> {
                final Map<String, E> map = traverser.get();
                if (!map.containsKey(startStep.getLabel().get()))
                    throw new IllegalArgumentException("The provided key is not in the current map: " + startStep.getLabel().get());
                final Object startObject = map.get(startStep.getLabel().get());
                final Object endObject;
                if (endStep.getLabel().isPresent()) {
                    if (!map.containsKey(endStep.getLabel().get()))
                        throw new IllegalArgumentException("The provided key is not in the current map: " + endStep.getLabel().get());
                    endObject = map.get(endStep.getLabel().get());
                } else
                    endObject = null;

                startStep.addStart(whereStep.getTraversal().asAdmin().getTraverserGenerator().generate(startObject, (Step) startStep, traverser.bulk()));
                if (null == endObject) {
                    if (whereStep.constraint.hasNext()) {
                        whereStep.constraint.reset();
                        return true;
                    } else {
                        return false;
                    }

                } else {
                    while (whereStep.constraint.hasNext()) {
                        if (whereStep.constraint.next().equals(endObject)) {
                            whereStep.constraint.reset();
                            return true;
                        }
                    }
                    return false;
                }
            });
        }
    }
}
