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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerResultStep<S> extends AbstractStep<S, S> {

    private final transient GraphComputer graphComputer;
    private final transient ComputerResult computerResult;

    private Iterator<Traverser.Admin<S>> traversers;
    private Graph graph;
    private final boolean attachElements; // should be part of graph computer with "propagate properties"
    private boolean first = true;
    private boolean byPass = false;

    public ComputerResultStep(final Traversal.Admin traversal, final GraphComputer graphComputer, final boolean attachElements) {
        super(traversal);
        this.attachElements = attachElements;
        this.graphComputer = graphComputer;
        this.computerResult = null;
    }

    public ComputerResultStep(final Traversal.Admin traversal, final ComputerResult computerResult, final boolean attachElements) {
        super(traversal);
        this.attachElements = attachElements;
        this.graphComputer = null;
        this.computerResult = computerResult;
        this.populateTraversers(this.computerResult);
    }

    @Override
    public Traverser<S> processNextStart() {
        if (this.byPass) return this.starts.next();
        if (this.first && null == this.computerResult) {
            try {
                populateTraversers(this.graphComputer.program(TraversalVertexProgram.build().traversal(this.getTraversal(), !this.graphComputer.features().supportsDirectObjects()).create()).submit().get());
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.first = false;
        }

        final Traverser.Admin<S> traverser = this.traversers.next();
        if (this.attachElements && (traverser.get() instanceof Attachable))
            traverser.set((S) ((Attachable) traverser.get()).attach(this.graph));
        return traverser;
    }

    public void populateTraversers(final ComputerResult result) {
        this.graph = result.graph();
        result.memory().keys().forEach(key -> this.getTraversal().getSideEffects().set(key, result.memory().get(key)));
        final Step endStep = this.getPreviousStep();
        if (endStep instanceof SideEffectCapStep) {
            final List<String> sideEffectKeys = ((SideEffectCapStep<?, ?>) endStep).getSideEffectKeys();
            if (sideEffectKeys.size() == 1)
                this.traversers = IteratorUtils.of(this.getTraversal().getTraverserGenerator().generate(result.memory().get(sideEffectKeys.get(0)), this, 1l));
            else {
                final Map<String, Object> sideEffects = new HashMap<>();
                for (final String sideEffectKey : sideEffectKeys) {
                    sideEffects.put(sideEffectKey, result.memory().get(sideEffectKey));
                }
                this.traversers = IteratorUtils.of(this.getTraversal().getTraverserGenerator().generate((S) sideEffects, this, 1l));
            }
        } else if (result.memory().exists(ReducingBarrierStep.REDUCING)) {
            this.traversers = IteratorUtils.of(this.getTraversal().getTraverserGenerator().generate(result.memory().get(ReducingBarrierStep.REDUCING), this, 1l));
        } else {
            this.traversers = result.memory().get(TraverserMapReduce.TRAVERSERS);
        }
        this.first = false;
        this.byPass = false;
    }

    public void byPass() {
        this.byPass = true;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }
}
