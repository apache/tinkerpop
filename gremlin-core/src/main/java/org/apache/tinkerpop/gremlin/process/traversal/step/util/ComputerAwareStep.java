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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ComputerAwareStep<S, E> extends AbstractStep<S, E> implements EngineDependent {

    private Iterator<Traverser<E>> previousIterator = EmptyIterator.instance();

    public ComputerAwareStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<E> processNextStart() throws NoSuchElementException {
        while (true) {
            if (this.previousIterator.hasNext())
                return this.previousIterator.next();
            this.previousIterator = this.traverserStepIdAndLabelsSetByChild ? this.computerAlgorithm() : this.standardAlgorithm();
        }
    }

    @Override
    public void onEngine(final TraversalEngine engine) {
        this.traverserStepIdAndLabelsSetByChild = engine.isComputer();
    }

    @Override
    public ComputerAwareStep<S, E> clone() {
        final ComputerAwareStep<S, E> clone = (ComputerAwareStep<S, E>) super.clone();
        clone.previousIterator = EmptyIterator.instance();
        return clone;
    }

    protected abstract Iterator<Traverser<E>> standardAlgorithm() throws NoSuchElementException;

    protected abstract Iterator<Traverser<E>> computerAlgorithm() throws NoSuchElementException;

    //////

    public static class EndStep<S> extends AbstractStep<S, S> implements EngineDependent {

        public EndStep(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            final Traverser.Admin<S> start = this.starts.next();
            if (this.traverserStepIdAndLabelsSetByChild) {
                start.setStepId(((ComputerAwareStep<?, ?>) this.getTraversal().getParent()).getNextStep().getId());
                start.path().extend(((ComputerAwareStep<?, ?>) this.getTraversal().getParent()).getLabels());
            }
            return start;
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this);
        }

        @Override
        public void onEngine(final TraversalEngine engine) {
            this.traverserStepIdAndLabelsSetByChild = engine.isComputer();
        }
    }

}
