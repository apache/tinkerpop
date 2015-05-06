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
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.ChainedComparator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMapReduce extends StaticMapReduce<Comparable, Object, Comparable, Object, Iterator<Object>> {

    public static final String TRAVERSERS = Graph.Hidden.hide("traversers");

    private Traversal.Admin<?, ?> traversal;
    private Optional<Comparator<Comparable>> comparator = Optional.empty();

    private TraverserMapReduce() {
    }

    public TraverserMapReduce(final Step traversalEndStep) {
        this.traversal = traversalEndStep.getTraversal();
        this.comparator = Optional.ofNullable(traversalEndStep instanceof OrderGlobalStep ? new ChainedComparator<Comparable>(((OrderGlobalStep) traversalEndStep).getComparators()) : null);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
        final Step endStep = this.traversal.getEndStep().getPreviousStep(); // don't get the ComputerResultStep
        this.comparator = Optional.ofNullable(endStep instanceof OrderGlobalStep ? new ChainedComparator<Comparable>(((OrderGlobalStep) endStep).getComparators()) : null);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Comparable, Object> emitter) {
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
    public Iterator<Object> generateFinalResult(final Iterator<KeyValue<Comparable, Object>> keyValues) {
        return IteratorUtils.map(keyValues, KeyValue::getValue);
    }


    @Override
    public String getMemoryKey() {
        return TRAVERSERS;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + TRAVERSERS).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this);
    }
}