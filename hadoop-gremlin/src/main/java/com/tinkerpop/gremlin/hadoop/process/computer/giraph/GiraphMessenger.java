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
package com.tinkerpop.gremlin.hadoop.process.computer.giraph;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessenger<M> implements Messenger<M> {

    private GiraphComputeVertex giraphComputeVertex;
    private Iterable<ObjectWritable<M>> messages;

    public void setCurrentVertex(final GiraphComputeVertex giraphComputeVertex, final Iterable<ObjectWritable<M>> messages) {
        this.giraphComputeVertex = giraphComputeVertex;
        this.messages = messages;
    }

    @Override
    public Iterable<M> receiveMessages(final MessageScope messageScope) {
        return IteratorUtils.map(this.messages, ObjectWritable::get);
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal.Admin<Vertex, Edge> incidentTraversal = GiraphMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get(), this.giraphComputeVertex.getBaseVertex());
            final Direction direction = GiraphMessenger.getOppositeDirection(incidentTraversal);
            incidentTraversal.forEachRemaining(edge ->
                    this.giraphComputeVertex.sendMessage(
                            new LongWritable(Long.valueOf(edge.iterators().vertexIterator(direction).next().id().toString())),
                            new ObjectWritable<>(localMessageScope.getEdgeFunction().apply(message, edge))));
        } else {
            final MessageScope.Global globalMessageScope = (MessageScope.Global) messageScope;
            globalMessageScope.vertices().forEach(vertex ->
                    this.giraphComputeVertex.sendMessage(new LongWritable(Long.valueOf(vertex.id().toString())), new ObjectWritable<>(message)));
        }
    }

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(final Traversal<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.asAdmin().addStep(0, new StartStep<>(incidentTraversal.asAdmin(), vertex));
        return (T) incidentTraversal;
    }

    private static Direction getOppositeDirection(final Traversal.Admin<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
        return step.getDirection().opposite();
    }
}
