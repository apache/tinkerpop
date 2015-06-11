/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.filter.exp;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TailLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class XMatchStep<S> extends AbstractStep<S, S> implements TraversalParent {

    private final List<Traversal.Admin<?, ?>> andTraversals = new ArrayList<>();
    private final List<String> traversalLabels = new ArrayList<>();
    private final List<String> startLabels = new ArrayList<>();

    public XMatchStep(final Traversal.Admin traversal, final Traversal... andTraversals) {
        super(traversal);
        int counter = 0;
        for (final Traversal andTraversal : andTraversals) {
            final String traversalLabel = "t" + counter++;
            this.traversalLabels.add(traversalLabel);
            //// START STEP
            final Step<?, ?> startStep = andTraversal.asAdmin().getStartStep();
            if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
                if (startStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The start step of a where()-traversal predicate can only have one label: " + startStep);
                final String startLabel = startStep.getLabels().iterator().next();
                final Step<?, ?> selectOneStep = new SelectOneStep<>(andTraversal.asAdmin(), Scope.global, startLabel);
                selectOneStep.addLabel(traversalLabel);
                this.startLabels.add(startLabel);
                TraversalHelper.replaceStep(andTraversal.asAdmin().getStartStep(), selectOneStep, andTraversal.asAdmin());
                TraversalHelper.insertAfterStep(new TailLocalStep<>(andTraversal.asAdmin(), 1), selectOneStep, andTraversal.asAdmin());
            }
            //// END STEP
            final Step<?, ?> endStep = andTraversal.asAdmin().getEndStep();
            if (!endStep.getLabels().isEmpty()) {
                if (endStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The end step of a where()-traversal predicate can only have one label: " + endStep);
                final String label = endStep.getLabels().iterator().next();
                endStep.removeLabel(label);
                final Step<?, ?> isOrAllowStep = new IsOrAllowStep<>(andTraversal.asAdmin(), label);
                isOrAllowStep.addLabel(label);
                andTraversal.asAdmin().addStep(isOrAllowStep);
            }
            this.andTraversals.add(this.integrateChild(andTraversal.asAdmin()));
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {

            for (final Traversal.Admin<?, ?> andTraversal : this.andTraversals) {
                if (andTraversal.hasNext()) {
                    this.starts.add((Traverser.Admin) andTraversal.getEndStep().next().asAdmin());
                }
            }
            final Traverser<S> traverser = this.starts.next();
            boolean repeated = false;
            for (int i = 0; i < this.andTraversals.size(); i++) {
                if (traverser.path().hasLabel(this.startLabels.get(i)) && !traverser.path().hasLabel(this.traversalLabels.get(i))) {
                    repeated = true;
                    this.andTraversals.get(i).addStart((Traverser) traverser);
                    break;
                }
            }
            if (!repeated)
                return traverser;
        }
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        return Collections.unmodifiableList(this.andTraversals);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.andTraversals);
    }


    @Override
    public XMatchStep<S> clone() {
        final XMatchStep<S> clone = (XMatchStep<S>) super.clone();
        clone.andTraversals.clear();
        for (final Traversal.Admin<?, ?> traversal : this.andTraversals) {
            clone.andTraversals.add(clone.integrateChild(traversal.clone()));
        }
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.andTraversals.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }
}
