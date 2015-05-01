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
    private Traversal.Admin traversalConstraint;


    public WhereStep(final Traversal.Admin traversal, final String firstKey, final P<?> secondKeyPredicate) {
        super(traversal);
        this.firstKey = firstKey;
        this.secondKey = (String) secondKeyPredicate.getValue();
        this.biPredicate = secondKeyPredicate.getBiPredicate();
        this.traversalConstraint = null;
    }

    public WhereStep(final Traversal.Admin traversal, final Traversal.Admin traversalConstraint) {
        super(traversal);
        this.biPredicate = null;
        this.traversalConstraint = this.integrateChild(traversalConstraint);
        // TODO: do we need to compile the traversal first (probably)
        ///  get the start-step as()
        final Step<?, ?> startStep = this.traversalConstraint.getStartStep();
        if (startStep.getLabels().isEmpty())
            throw new IllegalArgumentException("Where traversal must have their start step labeled with as(): " + this.traversalConstraint);
        if (startStep.getLabels().size() > 1)
            throw new IllegalArgumentException("Where traversal can not have multiple labels on the start step: " + this.traversalConstraint);
        this.firstKey = startStep.getLabels().iterator().next();
        /// get the end-step as()
        Step<?, ?> endStep = this.traversalConstraint.getEndStep();
        if (endStep instanceof MarkerIdentityStep) endStep = endStep.getPreviousStep();  // DAH
        if (endStep.getLabels().size() > 1)
            throw new IllegalArgumentException("Where traversal can not have multiple labels on the end step: " + this.traversalConstraint);
        this.secondKey = endStep.getLabels().isEmpty() ? null : endStep.getLabels().iterator().next();
    }

    @Override
    protected boolean filter(final Traverser.Admin<Map<String, E>> traverser) {
        final Map<String, E> map = traverser.get();
        // bi-predicate predicate
        if (null == this.traversalConstraint) {
            if (!map.containsKey(this.firstKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.firstKey);
            if (!map.containsKey(this.secondKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.secondKey);
            return this.biPredicate.test(map.get(this.firstKey), map.get(this.secondKey));
        }
        // traversal predicate
        else {
            final Object startObject = map.get(this.firstKey);
            if (null == startObject)
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.firstKey);
            if (null != this.secondKey && !map.containsKey(this.secondKey))
                throw new IllegalArgumentException("The provided key is not in the current map: " + this.secondKey);
            final Object endObject = null == this.secondKey ? null : map.get(this.secondKey);
            //
            this.traversalConstraint.addStart(this.getTraversal().asAdmin().getTraverserGenerator().generate(startObject, this.traversalConstraint.getStartStep(), traverser.bulk()));
            if (null == endObject) {
                if (this.traversalConstraint.hasNext()) {
                    this.traversalConstraint.reset();
                    return true;
                }
            } else {
                while (this.traversalConstraint.hasNext()) {
                    if (this.traversalConstraint.next().equals(endObject)) {
                        this.traversalConstraint.reset();
                        return true;
                    }
                }
            }
            return false;
        }
    }

    @Override
    public List<Traversal.Admin> getLocalChildren() {
        return null == this.traversalConstraint ? Collections.emptyList() : Collections.singletonList(this.traversalConstraint);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.firstKey, this.biPredicate, this.secondKey, this.traversalConstraint);
    }

    @Override
    public WhereStep<E> clone() {
        final WhereStep<E> clone = (WhereStep<E>) super.clone();
        if (null != this.traversalConstraint)
            clone.traversalConstraint = clone.integrateChild(this.traversalConstraint.clone());
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }
}
