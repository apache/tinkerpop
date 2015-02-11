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
package com.tinkerpop.gremlin.process.traversal.step;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.iterator.MultiIterator;
import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExpandableStepIterator<E> implements Iterator<Traverser.Admin<E>>, Serializable {

    private final TraverserSet<E> traverserSet = new TraverserSet<>();
    private final MultiIterator<Traverser.Admin<E>> traverserIterators = new MultiIterator<>();
    private final Step<?, E> hostStep;

    public ExpandableStepIterator(final Step<?, E> hostStep) {
        this.hostStep = hostStep;
    }

    @Override
    public boolean hasNext() {
        return !this.traverserSet.isEmpty() || this.hostStep.getPreviousStep().hasNext() || this.traverserIterators.hasNext();
    }

    @Override
    public Traverser.Admin<E> next() {
        if (!this.traverserSet.isEmpty())
            return this.traverserSet.remove();
        if (this.traverserIterators.hasNext())
            return this.traverserIterators.next();
        /////////////
        if (this.hostStep.getPreviousStep().hasNext())
            return (Traverser.Admin<E>) this.hostStep.getPreviousStep().next();
        /////////////
        return this.traverserSet.remove();
    }

    public void add(final Iterator<Traverser.Admin<E>> iterator) {
        this.traverserIterators.addIterator(iterator);
    }

    public void add(final Traverser.Admin<E> traverser) {
        this.traverserSet.add(traverser);
    }

    @Override
    public String toString() {
        return this.traverserSet.toString();
    }

    public void clear() {
        this.traverserIterators.clear();
        this.traverserSet.clear();
    }
}
