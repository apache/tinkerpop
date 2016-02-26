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
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMapReduce implements MapReduce<Comparable, Traverser<?>, Comparable, Traverser<?>, Iterator<Traverser<?>>> {

    private Traversal.Admin<?, ?> traversal;
    private Comparator<Comparable> comparator = null;
    private CollectingBarrierStep<?> collectingBarrierStep = null;
    private boolean attachHaltedTraverser = false;

    private TraverserMapReduce() {
    }

    public TraverserMapReduce(final Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal;
        this.genericLoadState();
    }

    @Override
    public TraverserMapReduce clone() {
        try {
            final TraverserMapReduce clone = (TraverserMapReduce) super.clone();
            clone.traversal = this.traversal.clone();
            clone.genericLoadState();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversal(graph, configuration);
        this.genericLoadState();
    }

    private void genericLoadState() {
        final Step<?, ?> traversalEndStep = traversal.getEndStep();
        if (traversalEndStep instanceof CollectingBarrierStep) {
            this.collectingBarrierStep = ((CollectingBarrierStep<?>) traversalEndStep).clone();
            if (this.collectingBarrierStep instanceof TraversalParent) {
                this.attachHaltedTraverser = ((TraversalParent) this.collectingBarrierStep).getLocalChildren().stream().filter(TraversalHelper::isBeyondElementId).findAny().isPresent();
            }
        }
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP) || null != this.collectingBarrierStep;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Comparable, Traverser<?>> emitter) {
        vertex.<TraverserSet<Object>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> {
            if (this.attachHaltedTraverser && !(traverser.get() instanceof Edge)) {
                traverser = traverser.clone().asAdmin();
                traverser.attach(Attachable.Method.get(vertex));
            }
            if (null != this.comparator)    // TODO: I think we shouldn't ever single key it  -- always double emit to load balance the servers.
                emitter.emit(traverser, traverser);
            else
                emitter.emit(traverser);
        }));
    }

    @Override
    public Optional<Comparator<Comparable>> getMapKeySort() {
        return Optional.ofNullable(this.comparator);
    }

    @Override
    public void combine(final Comparable comparable, final Iterator<Traverser<?>> values, final ReduceEmitter<Comparable, Traverser<?>> emitter) {
        final TraverserSet<?> traverserSet = new TraverserSet<>();
        while (values.hasNext()) {
            traverserSet.add((Traverser.Admin) values.next().asAdmin());
        }
        IteratorUtils.removeOnNext(traverserSet.iterator()).forEachRemaining(traverser -> {
            if (null != this.comparator)
                emitter.emit(traverser, traverser);
            else
                emitter.emit(traverser);
        });
    }

    @Override
    public void reduce(final Comparable comparable, final Iterator<Traverser<?>> values, final ReduceEmitter<Comparable, Traverser<?>> emitter) {
        final TraverserSet<?> traverserSet = new TraverserSet<>();
        while (values.hasNext()) {
            traverserSet.add((Traverser.Admin) values.next().asAdmin());
        }
        IteratorUtils.removeOnNext(traverserSet.iterator()).forEachRemaining(emitter::emit);
    }

    @Override
    public Iterator<Traverser<?>> generateFinalResult(final Iterator<KeyValue<Comparable, Traverser<?>>> keyValues) {
        if (null != this.collectingBarrierStep) {
            final TraverserSet<?> traverserSet = new TraverserSet<>();
            while (keyValues.hasNext()) {
                traverserSet.add((Traverser.Admin) keyValues.next().getValue().asAdmin());
            }
            this.collectingBarrierStep.barrierConsumer((TraverserSet) traverserSet);
            return (Iterator) traverserSet.iterator();
        } else {
            return IteratorUtils.map(keyValues, KeyValue::getValue);
        }
    }

    @Override
    public String getMemoryKey() {
        return TraversalVertexProgram.HALTED_TRAVERSERS;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.traversal.toString());
    }
}
