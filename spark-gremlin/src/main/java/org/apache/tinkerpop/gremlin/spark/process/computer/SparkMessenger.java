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
package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkMessenger<M> implements Messenger<M> {

    private Vertex vertex;
    private Iterable<M> incomingMessages;
    private List<Tuple2<Object, M>> outgoingMessages = new ArrayList<>();

    public void setVertexAndIncomingMessages(final Vertex vertex, final Iterable<M> incomingMessages) {
        this.vertex = vertex;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = new ArrayList<>();
    }

    public List<Tuple2<Object, M>> getOutgoingMessages() {
        return this.outgoingMessages;
    }

    @Override
    public Iterator<M> receiveMessages() {
        return IteratorUtils.removeOnNext(this.incomingMessages.iterator());
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal.Admin<Vertex, Edge> incidentTraversal = SparkMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get().asAdmin(), this.vertex);
            final Direction direction = SparkMessenger.getOppositeDirection(incidentTraversal);
            incidentTraversal.forEachRemaining(edge -> this.outgoingMessages.add(new Tuple2<>(edge.vertices(direction).next().id(), message)));
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> this.outgoingMessages.add(new Tuple2<>(v.id(), message)));
        }
    }

    ///////////

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(final Traversal.Admin<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.asAdmin().addStart(incidentTraversal.getTraverserGenerator().generate(vertex, incidentTraversal.asAdmin().getStartStep(), 1l));
        return (T) incidentTraversal;
    }

    private static Direction getOppositeDirection(final Traversal.Admin<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
        return step.getDirection().opposite();
    }
}
