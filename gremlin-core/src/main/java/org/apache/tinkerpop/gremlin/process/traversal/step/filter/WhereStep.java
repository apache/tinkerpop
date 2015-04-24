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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MarkerIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.P;

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
    private final BiPredicate biPredicate;
    private Traversal.Admin constraint;


    public WhereStep(final Traversal.Admin traversal, final String firstKey, final P<?> secondKeyPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = (String) secondKeyPredicate.getValue();
        this.biPredicate = secondKeyPredicate.getBiPredicate();
        this.constraint = null;
    }

    public WhereStep(final Traversal.Admin traversal, final Traversal.Admin constraint) {
        super(traversal);
        this.firstKey = null;
        this.secondKey = null;
        this.biPredicate = null;
        this.constraint = this.integrateChild(constraint);
    }

    @Override
    protected boolean filter(final Traverser.Admin<Map<String, E>> traverser) {
        if (null == this.constraint) {
            final Map<String, E> map = traverser.get();
            if (!map.containsKey(this.firstKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.firstKey);
            if (!map.containsKey(this.secondKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.secondKey);
            return this.biPredicate.test(map.get(this.firstKey), map.get(this.secondKey));
        } else {
            final Step<?, ?> startStep = this.constraint.getStartStep();
            Step<?, ?> endStep = this.constraint.getEndStep();
            if (endStep instanceof MarkerIdentityStep) // DAH!
                endStep = endStep.getPreviousStep();

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

            startStep.addStart(this.getTraversal().asAdmin().getTraverserGenerator().generate(startObject, (Step) startStep, traverser.bulk()));
            if (null == endObject) {
                if (this.constraint.hasNext()) {
                    this.constraint.reset();
                    return true;
                } else {
                    return false;
                }

            } else {
                while (this.constraint.hasNext()) {
                    if (this.constraint.next().equals(endObject)) {
                        this.constraint.reset();
                        return true;
                    }
                }
                return false;
            }
        }
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
    public WhereStep<E> clone() {
        final WhereStep<E> clone = (WhereStep<E>) super.clone();
        if (null != this.constraint)
            clone.constraint = clone.integrateChild(this.constraint.clone());
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }
}
