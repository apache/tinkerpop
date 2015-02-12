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
package com.tinkerpop.gremlin.process.graph.traversal.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Collections;
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
            this.previousIterator = this.traverserStepIdSetByChild ? this.computerAlgorithm() : this.standardAlgorithm();
        }
    }

    @Override
    public void onEngine(final TraversalEngine engine) {
        this.traverserStepIdSetByChild = engine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public ComputerAwareStep<S, E> clone() throws CloneNotSupportedException {
        final ComputerAwareStep<S, E> clone = (ComputerAwareStep<S, E>) super.clone();
        clone.previousIterator = Collections.emptyIterator();
        return clone;
    }

    protected abstract Iterator<Traverser<E>> standardAlgorithm() throws NoSuchElementException;

    protected abstract Iterator<Traverser<E>> computerAlgorithm() throws NoSuchElementException;

    //////

    public class EndStep extends AbstractStep<S, S> implements EngineDependent {

        public EndStep(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            final Traverser.Admin<S> start = this.starts.next();
            if (this.traverserStepIdSetByChild) start.setStepId(ComputerAwareStep.this.getNextStep().getId());
            return start;
        }

        @Override
        public String toString() {
            return TraversalHelper.makeStepString(this);
        }

        @Override
        public void onEngine(final TraversalEngine engine) {
            this.traverserStepIdSetByChild = engine.equals(TraversalEngine.COMPUTER);
        }
    }

}
