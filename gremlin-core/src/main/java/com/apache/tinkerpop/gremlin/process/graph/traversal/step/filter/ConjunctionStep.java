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
import com.apache.tinkerpop.gremlin.process.Traverser;
import com.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.apache.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ConjunctionStep<S> extends AbstractStep<S, S> implements TraversalParent {

    private List<Traversal.Admin<S, ?>> conjunctionTraversals;
    private final boolean isAnd;

    public ConjunctionStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?>... conjunctionTraversals) {
        super(traversal);
        this.isAnd = this.getClass().equals(AndStep.class);
        this.conjunctionTraversals = Arrays.asList(conjunctionTraversals);
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.conjunctionTraversals) {
            this.integrateChild(conjunctionTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            boolean found = false;
            for (final Traversal.Admin<S, ?> traversal : this.conjunctionTraversals) {
                traversal.addStart(start.split());
                found = traversal.hasNext();
                traversal.reset();
                if (this.isAnd) {
                    if (!found)
                        break;
                } else if (found)
                    break;
            }
            if (found) return start;
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return Collections.unmodifiableList(this.conjunctionTraversals);
    }

    @Override
    public ConjunctionStep<S> clone() throws CloneNotSupportedException {
        final ConjunctionStep<S> clone = (ConjunctionStep<S>) super.clone();
        clone.conjunctionTraversals = new ArrayList<>();
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.conjunctionTraversals) {
            clone.conjunctionTraversals.add(clone.integrateChild(conjunctionTraversal.clone(), TYPICAL_LOCAL_OPERATIONS));
        }
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.conjunctionTraversals);
    }

    public boolean isConjunctionHasTree() {
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.conjunctionTraversals) {
            for (final Step<?, ?> step : conjunctionTraversal.getSteps()) {
                if (step instanceof ConjunctionStep) {
                    if (!((ConjunctionStep) step).isConjunctionHasTree())
                        return false;
                } else if (!(step instanceof HasStep))
                    return false;
            }
        }
        return true;
    }

    public ConjunctionTree getConjunctionHasTree() {
        return new ConjunctionTree(this);
    }


    ////////

    public static class ConjunctionMarker<S> extends AbstractStep<S, S> {

        public ConjunctionMarker(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            throw new IllegalStateException("This step should have been removed via a strategy: " + this.getClass().getCanonicalName());
        }
    }

    ////////

    public static class ConjunctionTree implements Iterable<ConjunctionTree.Entry> {

        private final List<Entry> tree = new ArrayList<>();
        private final boolean isAnd;

        public ConjunctionTree(final ConjunctionStep<?> conjunctionStep) {
            this.isAnd = conjunctionStep.isAnd;
            for (final Traversal.Admin<?, ?> conjunctionTraversal : conjunctionStep.conjunctionTraversals) {
                for (final Step<?, ?> step : conjunctionTraversal.getSteps()) {
                    if (step instanceof HasStep) {
                        (((HasStep<?>) step).getHasContainers()).forEach(container -> this.tree.add(new Entry(HasContainer.class, container)));
                    } else if (step instanceof ConjunctionStep) {
                        this.tree.add(new Entry(ConjunctionTree.class, ((ConjunctionStep) step).getConjunctionHasTree()));
                    } else {
                        throw new IllegalArgumentException("This conjunction supports more complex steps than HasStep");
                    }
                }
            }
        }

        @Override
        public String toString() {
            return (this.isAnd ? "and" : "or") + this.tree.toString();
        }

        public boolean isAnd() {
            return this.isAnd;
        }

        @Override
        public Iterator<Entry> iterator() {
            return this.tree.iterator();
        }

        public static class Entry {
            private Class entryClass;
            private Object entryValue;

            public Entry(final Class entryClass, final Object entryValue) {
                this.entryClass = entryClass;
                this.entryValue = entryValue;
            }

            public <V> V getValue() {
                return (V) this.entryValue;
            }

            public boolean isHasContainer() {
                return this.entryClass.equals(HasContainer.class);
            }

            public boolean isConjunctionTree() {
                return this.entryClass.equals(ConjunctionTree.class);
            }

            public String toString() {
                return this.entryValue.toString();
            }
        }
    }
}
