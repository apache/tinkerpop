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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractStep<S, E> implements Step<S, E> {

    protected Set<String> labels = new LinkedHashSet<>();
    protected String id = Traverser.Admin.HALT;
    protected Traversal.Admin traversal;
    protected ExpandableStepIterator<S> starts;
    protected Traverser.Admin<E> nextEnd = EmptyTraverser.instance();
    protected boolean traverserStepIdAndLabelsSetByChild = false;

    protected Step<?, S> previousStep = EmptyStep.instance();
    protected Step<E, ?> nextStep = EmptyStep.instance();

    public AbstractStep(final Traversal.Admin traversal) {
        this.traversal = traversal;
        this.starts = new ExpandableStepIterator<>(this, (TraverserSet<S>) traversal.getTraverserSetSupplier().get());
    }

    @Override
    public void setId(final String id) {
        Objects.requireNonNull(id);
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void addLabel(final String label) {
        this.labels.add(label);
    }

    @Override
    public void removeLabel(final String label) {
        this.labels.remove(label);
    }

    @Override
    public void clearLabels() {
        this.labels.clear();
    }

    @Override
    public Set<String> getLabels() {
        return Collections.unmodifiableSet(this.labels);
    }

    @Override
    public void reset() {
        this.starts.clear();
        this.nextEnd = EmptyTraverser.instance();
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {
        this.starts.add(starts);
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        this.starts.add(start);
    }

    @Override
    public boolean hasStarts() {
        return this.starts.hasNext();
    }

    @Override
    public void setPreviousStep(final Step<?, S> step) {
        this.previousStep = step;
    }

    @Override
    public Step<?, S> getPreviousStep() {
        return this.previousStep;
    }

    @Override
    public void setNextStep(final Step<E, ?> step) {
        this.nextStep = step;
    }

    @Override
    public Step<E, ?> getNextStep() {
        return this.nextStep;
    }

    @Override
    public Traverser.Admin<E> next() {
        if (EmptyTraverser.instance() != this.nextEnd) {
            try {
                return this.prepareTraversalForNextStep(this.nextEnd);
            } finally {
                this.nextEnd = EmptyTraverser.instance();
            }
        } else {
            while (true) {
                if (Thread.interrupted()) throw new TraversalInterruptedException();
                final Traverser.Admin<E> traverser = this.processNextStart();
                if (traverser.bulk() > 0)
                    return this.prepareTraversalForNextStep(traverser);
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (EmptyTraverser.instance() != this.nextEnd)
            return true;
        else {
            try {
                while (true) {
                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                    this.nextEnd = this.processNextStart();
                    if (this.nextEnd.bulk() > 0)
                        return true;
                    else
                        this.nextEnd = EmptyTraverser.instance();
                }
            } catch (final NoSuchElementException e) {
                return false;
            }
        }
    }

    @Override
    public <A, B> Traversal.Admin<A, B> getTraversal() {
        return this.traversal;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal;
    }

    protected abstract Traverser.Admin<E> processNextStart() throws NoSuchElementException;

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    @Override
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public AbstractStep<S, E> clone() {
        try {
            final AbstractStep<S, E> clone = (AbstractStep<S, E>) super.clone();
            clone.starts = new ExpandableStepIterator<>(clone, (TraverserSet<S>) traversal.getTraverserSetSupplier().get());
            clone.previousStep = EmptyStep.instance();
            clone.nextStep = EmptyStep.instance();
            clone.nextEnd = EmptyTraverser.instance();
            clone.traversal = EmptyTraversal.instance();
            clone.labels = new LinkedHashSet<>(this.labels);
            clone.reset();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other != null && other.getClass().equals(this.getClass()) && this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int result = this.getClass().hashCode();
        for (final String label : this.getLabels()) {
            result ^= label.hashCode();
        }
        return result;
    }

    public ExpandableStepIterator<S> getStarts() {
        return this.starts;
    }

    public boolean isTraverserStepIdAndLabelsSetByChild() {
        return traverserStepIdAndLabelsSetByChild;
    }

    protected Traverser.Admin<E> prepareTraversalForNextStep(final Traverser.Admin<E> traverser) {
        if (!this.traverserStepIdAndLabelsSetByChild) {
            traverser.setStepId(this.nextStep.getId());
            traverser.addLabels(this.labels);
        }
        return traverser;
    }
}
