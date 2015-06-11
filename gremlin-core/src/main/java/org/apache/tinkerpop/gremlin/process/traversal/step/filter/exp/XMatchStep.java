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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class XMatchStep<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    private List<Traversal.Admin<Object, Object>> andTraversals = new ArrayList<>();
    private boolean first = true;

    private final MatchAlgorithm matchAlgorithm = new GreedyMatchAlgorithm();


    public XMatchStep(final Traversal.Admin traversal, final Traversal... andTraversals) {
        super(traversal);
        int counter = 0;
        for (final Traversal andTraversal : andTraversals) {
            final String traversalLabel = "t" + counter++;  // TODO: this should be specified in a finalization strategy and based on the traversal then static id
            //// START STEP
            final Step<?, ?> startStep = andTraversal.asAdmin().getStartStep();
            if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
                if (startStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The start step of a match()-traversal can only have one label: " + startStep);
                final String startLabel = startStep.getLabels().iterator().next();
                final Step<?, ?> selectOneStep = new SelectOneStep<>(andTraversal.asAdmin(), Scope.global, Pop.head, startLabel);
                selectOneStep.addLabel(traversalLabel);
                TraversalHelper.replaceStep(andTraversal.asAdmin().getStartStep(), selectOneStep, andTraversal.asAdmin());
            }
            //// END STEP
            final Step<?, ?> endStep = andTraversal.asAdmin().getEndStep();
            if (!endStep.getLabels().isEmpty()) {
                if (endStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The end step of a match()-traversal can only have one label: " + endStep);
                final String label = endStep.getLabels().iterator().next();
                endStep.removeLabel(label);
                final Step<?, ?> isOrAllowStep = new IsOrAllowStep<>(andTraversal.asAdmin(), label);      // TODO: perhaps just have a XMatchEndStep private static?
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
        for (final Traversal.Admin<Object, Object> traversal : this.andTraversals) {
            clone.andTraversals.add(clone.integrateChild(traversal.clone()));
        }
        // TODO: does it need to clone the match algorithm?
        return clone;
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.first) {
                this.matchAlgorithm.initialize(this.andTraversals);
                this.first = false;
            } else {
                for (final Traversal.Admin<?, ?> andTraversal : this.andTraversals) {
                    if (andTraversal.hasNext())
                        this.starts.add((Traverser.Admin) andTraversal.getEndStep().next().asAdmin());
                }
            }
            final Traverser.Admin traverser = this.starts.next();
            final Optional<Traversal.Admin<Object, Object>> optional = this.matchAlgorithm.apply(traverser);
            if (optional.isPresent())
                optional.get().addStart(traverser);
            else
                // TODO: trim off internal traversal labels from path
                return IteratorUtils.of(traverser);
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        if (this.first) {
            this.matchAlgorithm.initialize(this.andTraversals);
            this.first = false;
        }
        final Traverser.Admin traverser = this.starts.next();
        final Optional<Traversal.Admin<Object, Object>> optional = this.matchAlgorithm.apply(traverser);
        if (optional.isPresent()) {
            traverser.asAdmin().setStepId(optional.get().getStartStep().getId());
            return IteratorUtils.of(traverser);
        } else {
            // TODO: trim off internal traversal labels from path
            traverser.asAdmin().setStepId(this.getNextStep().getId());
            return IteratorUtils.of(traverser);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.andTraversals.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    //////////////////////////////

    public interface MatchAlgorithm extends Function<Traverser.Admin<Object>, Optional<Traversal.Admin<Object, Object>>> {
        public void initialize(final List<Traversal.Admin<Object, Object>> traversals);
    }

    public static class GreedyMatchAlgorithm implements MatchAlgorithm {

        private List<Traversal.Admin<Object, Object>> traversals;
        private List<String> traversalLabels = new ArrayList<>();
        private List<String> startLabels = new ArrayList<>();

        @Override
        public void initialize(final List<Traversal.Admin<Object, Object>> traversals) {
            this.traversals = traversals;
            for (final Traversal.Admin<Object, Object> traversal : traversals) {
                this.traversalLabels.add(traversal.getStartStep().getLabels().iterator().next());
                this.startLabels.add(((SelectOneStep<?, ?>) traversal.getStartStep()).getScopeKeys().iterator().next());
            }
        }

        @Override
        public Optional<Traversal.Admin<Object, Object>> apply(final Traverser.Admin<Object> traverser) {
            final Path path = traverser.path();
            for (int i = 0; i < this.traversals.size(); i++) {
                if (path.hasLabel(this.startLabels.get(i)) && !path.hasLabel(this.traversalLabels.get(i))) {
                    return Optional.of(this.traversals.get(i));
                }
            }
            return Optional.empty();
        }
    }
}
