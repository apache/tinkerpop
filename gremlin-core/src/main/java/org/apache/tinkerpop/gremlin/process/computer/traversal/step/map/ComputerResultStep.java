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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerResultStep<S> extends AbstractStep<ComputerResult, S> {


    private final boolean attachElements; // should be part of graph computer with "propagate properties"
    private Iterator<Traverser.Admin<S>> currentIterator = EmptyIterator.instance();

    public ComputerResultStep(final Traversal.Admin traversal, final boolean attachElements) {
        super(traversal);
        this.attachElements = attachElements;
    }

    public Iterator<Traverser.Admin<S>> attach(final Iterator<Traverser.Admin<S>> iterator, final Graph graph) {
        return IteratorUtils.map(iterator, traverser -> {
            if (this.attachElements && (traverser.get() instanceof Attachable))
                traverser.set((S) ((Attachable<Element>) traverser.get()).attach(Attachable.Method.get(graph)));
            return traverser;
        });
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            if (this.currentIterator.hasNext())
                return this.currentIterator.next();
            else {
                final ComputerResult result = this.starts.next().get();
                result.memory().keys().forEach(key -> this.getTraversal().getSideEffects().set(key, result.memory().get(key)));
                final Step endStep = this.getPreviousStep() instanceof TraversalVertexProgramStep ?
                        ((TraversalVertexProgramStep<?>) this.getPreviousStep()).computerTraversal.getEndStep() :
                        EmptyStep.instance();
                if (endStep instanceof SideEffectCapStep) {
                    final List<String> sideEffectKeys = ((SideEffectCapStep<?, ?>) endStep).getSideEffectKeys();
                    if (sideEffectKeys.size() == 1)
                        this.currentIterator = this.getTraversal().getTraverserGenerator().generateIterator(IteratorUtils.of(result.memory().get(sideEffectKeys.get(0))), (Step) this, 1l);
                    else {
                        final Map<String, Object> sideEffects = new HashMap<>();
                        for (final String sideEffectKey : sideEffectKeys) {
                            sideEffects.put(sideEffectKey, result.memory().get(sideEffectKey));
                        }
                        this.currentIterator = this.getTraversal().getTraverserGenerator().generateIterator(IteratorUtils.of((S) sideEffects), (Step) this, 1l);
                    }
                } else if (result.memory().exists(ReducingBarrierStep.REDUCING)) {
                    this.currentIterator = this.getTraversal().getTraverserGenerator().generateIterator(IteratorUtils.of(result.memory().get(ReducingBarrierStep.REDUCING)), (Step) this, 1l);
                } else {
                    this.currentIterator = this.attach(result.memory().get(TraverserMapReduce.TRAVERSERS), result.graph());
                }
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
}
