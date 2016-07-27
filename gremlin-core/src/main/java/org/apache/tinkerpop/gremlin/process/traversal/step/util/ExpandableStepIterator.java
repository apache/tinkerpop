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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableStepIterator<S> implements Iterator<Traverser.Admin<S>>, Serializable {

    private final TraverserSet<S> traverserSet = new TraverserSet<>();
    private final MultiIterator<Traverser.Admin<S>> traverserIterators = new MultiIterator<>();
    private final Step<S,?> hostStep;

    public ExpandableStepIterator(final Step<S,?> hostStep) {
        this.hostStep = hostStep;
    }

    @Override
    public boolean hasNext() {
        return !this.traverserSet.isEmpty() || this.hostStep.getPreviousStep().hasNext() || this.traverserIterators.hasNext();
    }

    @Override
    public Traverser.Admin<S> next() {
        if (!this.traverserSet.isEmpty())
            return this.traverserSet.remove();
        if (this.traverserIterators.hasNext())
            return this.traverserIterators.next();
        /////////////
        if (this.hostStep.getPreviousStep().hasNext())
            return (Traverser.Admin<S>) this.hostStep.getPreviousStep().next();
        /////////////
        return this.traverserSet.remove();
    }

    public void add(final Iterator<Traverser.Admin<S>> iterator) {
        this.traverserIterators.addIterator(iterator);
    }

    public void add(final Traverser.Admin<S> traverser) {
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
