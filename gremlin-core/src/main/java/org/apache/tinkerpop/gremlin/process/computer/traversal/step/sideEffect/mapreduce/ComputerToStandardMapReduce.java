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
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerToStandardMapReduce implements MapReduce<MapReduce.NullObject, Traverser<?>, MapReduce.NullObject, Traverser<?>, Iterator<Traverser<?>>> {

    private static final String VERTEX_TRAVERSAL = "gremlin.computerToStandardMapReduce.vertexTraversal";

    private Traversal.Admin<Vertex, ?> traversal;

    private ComputerToStandardMapReduce() {
    }

    public ComputerToStandardMapReduce(final Traversal.Admin<Vertex, ?> traversal) {
        this.traversal = traversal;
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.traversal = VertexProgramHelper.deserialize(configuration, VERTEX_TRAVERSAL);
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
        VertexProgramHelper.serialize(this.traversal, configuration, VERTEX_TRAVERSAL);
    }


    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP); // || this.access.equals(Access.HALTED);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Traverser<?>> emitter) {
        this.traversal.addStart(this.traversal.getTraverserGenerator().generate(vertex, this.traversal.getStartStep(), 1l));
        this.traversal.getEndStep().forEachRemaining(emitter::emit);
    }

    @Override
    public void combine(final NullObject key, final Iterator<Traverser<?>> values, final ReduceEmitter<NullObject, Traverser<?>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Traverser<?>> values, final ReduceEmitter<NullObject, Traverser<?>> emitter) {
        final TraverserSet<?> traverserSet = new TraverserSet<>();
        while (values.hasNext()) {
            traverserSet.add((Traverser.Admin) values.next().asAdmin());
        }
        IteratorUtils.removeOnNext(traverserSet.iterator()).forEachRemaining(emitter::emit);
    }

    @Override
    public Iterator<Traverser<?>> generateFinalResult(final Iterator<KeyValue<NullObject, Traverser<?>>> keyValues) {
        return IteratorUtils.map(keyValues, KeyValue::getValue);
    }

    @Override
    public String getMemoryKey() {
        return TraverserMapReduce.TRAVERSERS;
    }

    @Override
    public ComputerToStandardMapReduce clone() {
       try {
            final ComputerToStandardMapReduce clone = (ComputerToStandardMapReduce) super.clone();
            clone.traversal = this.traversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
