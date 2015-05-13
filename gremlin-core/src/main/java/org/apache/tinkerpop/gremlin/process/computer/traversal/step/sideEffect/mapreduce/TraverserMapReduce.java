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

    private TraverserMapReduce() {
    }

    public TraverserMapReduce(final Step traversalEndStep) {
        this.traversal = traversalEndStep.getTraversal();
        this.comparator = Optional.ofNullable(traversalEndStep instanceof OrderGlobalStep ? new ChainedComparator<Comparable>(((OrderGlobalStep) traversalEndStep).getComparators()) : null);
        if (!this.comparator.isPresent() && traversalEndStep instanceof CollectingBarrierStep)
            this.collectingBarrierStep = Optional.of((CollectingBarrierStep<?>) traversalEndStep);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversal(graph, configuration);
        final Step traversalEndStep = this.traversal.getEndStep().getPreviousStep(); // don't get the ComputerResultStep
        this.comparator = Optional.ofNullable(traversalEndStep instanceof OrderGlobalStep ? new ChainedComparator<Comparable>(((OrderGlobalStep) traversalEndStep).getComparators()) : null);
        if (!this.comparator.isPresent() && traversalEndStep instanceof CollectingBarrierStep)
            this.collectingBarrierStep = Optional.of((CollectingBarrierStep<?>) traversalEndStep);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP) || stage.equals(Stage.REDUCE) && this.collectingBarrierStep.isPresent();
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
    public void reduce(final Comparable comparable, final Iterator<Traverser<?>> values, final ReduceEmitter<Comparable, Traverser<?>> emitter) {
        final TraverserSet<?> traverserSet = new TraverserSet<>();
        while (values.hasNext()) {
            traverserSet.add((Traverser.Admin) values.next().asAdmin());
        }
        this.collectingBarrierStep.get().barrierConsumer((TraverserSet) traverserSet);
        traverserSet.forEach(emitter::emit);
    }

    @Override
    public Iterator<Traverser<?>> generateFinalResult(final Iterator<KeyValue<Comparable, Traverser<?>>> keyValues) {
        return IteratorUtils.map(keyValues, KeyValue::getValue);
    }


    @Override
    public String getMemoryKey() {
        return TRAVERSERS;
    }
}