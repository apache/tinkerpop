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
package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMessenger<M> implements Messenger<M> {

    private GiraphVertex giraphVertex;
    private GiraphComputation giraphComputation;
    private Iterator<ObjectWritable<M>> messages;

    public GiraphMessenger(final GiraphVertex giraphVertex, final GiraphComputation giraphComputation, final Iterator<ObjectWritable<M>> messages) {
        this.giraphVertex = giraphVertex;
        this.giraphComputation = giraphComputation;
        this.messages = messages;
    }

    @Override
    public Iterator<M> receiveMessages() {
        return IteratorUtils.map(this.messages, ObjectWritable::get);
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal.Admin<Vertex, Edge> incidentTraversal = GiraphMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get().asAdmin(), this.giraphVertex.getValue().get());
            final Direction direction = GiraphMessenger.getOppositeDirection(incidentTraversal);
            incidentTraversal.forEachRemaining(edge ->
                    this.giraphComputation.sendMessage(
                            new ObjectWritable<>(edge.vertices(direction).next().id()),
                            new ObjectWritable<>(localMessageScope.getEdgeFunction().apply(message, edge))));
        } else {
            final MessageScope.Global globalMessageScope = (MessageScope.Global) messageScope;
            globalMessageScope.vertices().forEach(vertex ->
                    this.giraphComputation.sendMessage(new ObjectWritable<>(vertex.id()), new ObjectWritable<>(message)));
        }
    }

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(final Traversal.Admin<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.asAdmin().addStart(incidentTraversal.getTraverserGenerator().generate(vertex, incidentTraversal.getStartStep(), 1l));
        return (T) incidentTraversal;
    }

    private static Direction getOppositeDirection(final Traversal.Admin<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
        return step.getDirection().opposite();
    }
}
