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
package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerResultStep<S> extends AbstractStep<ComputerResult, S> {

    private final boolean attachElements = Boolean.valueOf(System.getProperty("is.testing", "false"));
    private Iterator<Traverser.Admin<S>> currentIterator = EmptyIterator.instance();

    public ComputerResultStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public Iterator<Traverser.Admin<S>> attach(final Iterator<Traverser.Admin<S>> iterator, final Graph graph) {
        return IteratorUtils.map(iterator, traverser -> {
            traverser.setSideEffects(this.getTraversal().getSideEffects());   // necessary to ensure no NPE
            if (this.attachElements && (traverser.get() instanceof Attachable) && !(traverser.get() instanceof Property))
                traverser.set((S) ((Attachable<Element>) traverser.get()).attach(Attachable.Method.get(graph)));
            return traverser;
        });
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        while (true) {
            if (this.currentIterator.hasNext())
                return this.currentIterator.next();
            else {
                final ComputerResult result = this.starts.next().get();
                this.currentIterator = attach(result.memory().exists(TraversalVertexProgram.HALTED_TRAVERSERS)
                        ? result.memory().<TraverserSet<S>>get(TraversalVertexProgram.HALTED_TRAVERSERS).iterator()
                        : EmptyIterator.instance(), result.graph());
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

    @Override
    public ComputerResultStep<S> clone() {
        final ComputerResultStep<S> clone = (ComputerResultStep<S>) super.clone();
        clone.currentIterator = EmptyIterator.instance();
        return clone;
    }
}
