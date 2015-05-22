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
package org.apache.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.ChainedComparator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMapReduce extends StaticMapReduce<Comparable, Traverser<?>, Comparable, Traverser<?>, Iterator<Traverser<?>>> {

    public static final String TRAVERSERS = Graph.Hidden.hide("traversers");

    private Traversal.Admin<?, ?> traversal;
    private Optional<Comparator<Comparable>> comparator = Optional.empty();
    private Optional<CollectingBarrierStep<?>> collectingBarrierStep = Optional.empty();
    private Optional<RangeGlobalStep<?>> rangeGlobalStep = Optional.empty();
    private Optional<TailGlobalStep<?>> tailGlobalStep = Optional.empty();
    private boolean dedupGlobal = false;

    private TraverserMapReduce() {
    }

    public TraverserMapReduce(final Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal;
        this.genericLoadState();
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversal(graph, configuration);
        this.genericLoadState();
    }

    private void genericLoadState() {
        final Step<?, ?> traversalEndStep = traversal.getEndStep().getPreviousStep();  // don't get the ComputerResultStep
        this.comparator = Optional.ofNullable(traversalEndStep instanceof OrderGlobalStep ? new ChainedComparator<Comparable>(((OrderGlobalStep) traversalEndStep).getComparators()) : null);
        if (!this.comparator.isPresent() && traversalEndStep instanceof CollectingBarrierStep)
            this.collectingBarrierStep = Optional.of((CollectingBarrierStep<?>) traversalEndStep);
        if (traversalEndStep instanceof RangeGlobalStep)
            this.rangeGlobalStep = Optional.of(((RangeGlobalStep) traversalEndStep).clone());
        if (traversalEndStep instanceof TailGlobalStep)
            this.tailGlobalStep = Optional.of(((TailGlobalStep) traversalEndStep).clone());
        if (traversalEndStep instanceof DedupGlobalStep)
            this.dedupGlobal = true;

    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP) || this.collectingBarrierStep.isPresent() || this.rangeGlobalStep.isPresent() || this.tailGlobalStep.isPresent() || this.dedupGlobal;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Comparable, Traverser<?>> emitter) {
        if (this.comparator.isPresent())
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(traverser, traverser)));
        else
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(emitter::emit));
    }

    @Override
    public Optional<Comparator<Comparable>> getMapKeySort() {
        return this.comparator;
    }

    @Override
    public void combine(final Comparable comparable, final Iterator<Traverser<?>> values, final ReduceEmitter<Comparable, Traverser<?>> emitter) {
        this.reduce(comparable, values, emitter);
    }

    @Override
    public void reduce(final Comparable comparable, final Iterator<Traverser<?>> values, final ReduceEmitter<Comparable, Traverser<?>> emitter) {
        final TraverserSet<?> traverserSet = new TraverserSet<>();
        while (values.hasNext()) {
            traverserSet.add((Traverser.Admin) values.next().asAdmin());
        }
        traverserSet.forEach(emitter::emit);
    }

    @Override
    public Iterator<Traverser<?>> generateFinalResult(final Iterator<KeyValue<Comparable, Traverser<?>>> keyValues) {
        if (this.collectingBarrierStep.isPresent()) {
            final TraverserSet<?> traverserSet = new TraverserSet<>();
            while (keyValues.hasNext()) {
                traverserSet.add((Traverser.Admin) keyValues.next().getValue().asAdmin());
            }
            this.collectingBarrierStep.get().barrierConsumer((TraverserSet) traverserSet);
            return (Iterator) traverserSet.iterator();
        } else if (this.rangeGlobalStep.isPresent()) {
            final RangeGlobalStep<?> rangeGlobalStep = this.rangeGlobalStep.get();
            rangeGlobalStep.setBypass(false);
            rangeGlobalStep.addStarts(IteratorUtils.map(keyValues, keyValue -> (Traverser) keyValue.getValue()));
            return (Iterator) rangeGlobalStep;
        } else if (this.tailGlobalStep.isPresent()) {
            final TailGlobalStep<?> tailGlobalStep = this.tailGlobalStep.get();
            tailGlobalStep.setBypass(false);
            tailGlobalStep.addStarts(IteratorUtils.map(keyValues, keyValue -> (Traverser) keyValue.getValue()));
            return (Iterator) tailGlobalStep;
        } else if (this.dedupGlobal) {
            return IteratorUtils.map(keyValues, keyValue -> {
                keyValue.getValue().asAdmin().setBulk(1l);
                return keyValue.getValue();
            });
        } else {
            return IteratorUtils.map(keyValues, KeyValue::getValue);
        }
    }

    @Override
    public String getMemoryKey() {
        return TRAVERSERS;
    }
}
