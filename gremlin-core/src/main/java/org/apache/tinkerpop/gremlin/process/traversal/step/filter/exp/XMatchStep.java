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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class XMatchStep<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    private List<Traversal.Admin<Object, Object>> andTraversals = new ArrayList<>();
    private final List<String> traversalLabels = new ArrayList<>();
    private final List<String> startLabels = new ArrayList<>();
    private boolean first = true;


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
                    throw new IllegalArgumentException("The start step of a match()-traversal can only have one label: " + startStep);
                final String startLabel = startStep.getLabels().iterator().next();
                final Step<?, ?> selectOneStep = new SelectOneStep<>(andTraversal.asAdmin(), Scope.global, Pop.head, startLabel);
                selectOneStep.addLabel(traversalLabel);
                this.startLabels.add(startLabel);
                TraversalHelper.replaceStep(andTraversal.asAdmin().getStartStep(), selectOneStep, andTraversal.asAdmin());
            }
            //// END STEP
            final Step<?, ?> endStep = andTraversal.asAdmin().getEndStep();
            if (!endStep.getLabels().isEmpty()) {
                if (endStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The end step of a match()-traversal can only have one label: " + endStep);
                final String label = endStep.getLabels().iterator().next();
                endStep.removeLabel(label);
                final Step<?, ?> isOrAllowStep = new IsOrAllowStep<>(andTraversal.asAdmin(), label);
                isOrAllowStep.addLabel(label);
                andTraversal.asAdmin().addStep(isOrAllowStep);
                andTraversal.asAdmin().addStep(new EndStep(andTraversal.asAdmin(), true));
            }
            this.andTraversals.add(this.integrateChild(andTraversal.asAdmin()));
        }
    }


    public List<Traversal.Admin<Object, Object>> getGlobalChildren() {
        return Collections.unmodifiableList(this.andTraversals);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.andTraversals);
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
    }

    @Override
    public XMatchStep<S> clone() {
        final XMatchStep<S> clone = (XMatchStep<S>) super.clone();
        clone.andTraversals = new ArrayList<>();
        for (final Traversal.Admin<Object,Object> traversal : this.andTraversals) {
            clone.andTraversals.add(clone.integrateChild(traversal.clone()));
        }
        return clone;
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (!this.first) {
                for (final Traversal.Admin<?, ?> andTraversal : this.andTraversals) {
                    if (andTraversal.hasNext())
                        this.starts.add((Traverser.Admin) andTraversal.getEndStep().next().asAdmin());
                }
            }
            this.first = false;
            final Traverser<S> traverser = this.starts.next();
            final Path path = traverser.path();
            boolean traverserPassed = true;
            for (int i = 0; i < this.andTraversals.size(); i++) {
                if (path.hasLabel(this.startLabels.get(i)) && !path.hasLabel(this.traversalLabels.get(i))) {
                    traverserPassed = false;
                    this.andTraversals.get(i).addStart((Traverser) traverser);
                    break;
                }
            }
            if (traverserPassed) {
                // TODO: trim off internal traversal labels from path
                return IteratorUtils.of(traverser);
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        final Traverser<S> traverser = this.starts.next();
        final Path path = traverser.path();
        for (int i = 0; i < this.andTraversals.size(); i++) {
            if (path.hasLabel(this.startLabels.get(i)) && !path.hasLabel(this.traversalLabels.get(i))) {
                traverser.asAdmin().setStepId(this.andTraversals.get(i).getStartStep().getId());
                return IteratorUtils.of(traverser);
            }
        }
        // TODO: trim off internal traversal labels from path
        traverser.asAdmin().setStepId(this.getNextStep().getId());
        return IteratorUtils.of(traverser);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for(final Traversal.Admin<Object,Object> andTraversal : this.andTraversals) {
            result ^= andTraversal.hashCode();
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }
}
