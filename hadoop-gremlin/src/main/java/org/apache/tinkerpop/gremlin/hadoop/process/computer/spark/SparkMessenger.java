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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkMessenger<M> implements Serializable, Messenger<M> {

    private Vertex vertex;
    private List<M> incoming;
    private Map<Object, List<M>> outgoing;

    public SparkMessenger() {

    }

    public SparkMessenger(final Vertex vertex) {
        this.vertex = vertex;
        this.incoming = new ArrayList<>();
        this.outgoing = this.vertex instanceof DetachedVertex ? null : new HashMap<>();
    }

    public SparkMessenger(final Vertex vertex, final List<M> incomingMessages) {
        this.vertex = vertex;
        this.incoming = incomingMessages;
    }

    public void clearIncomingMessages() {
        this.incoming.clear();
    }

    public void clearOutgoingMessages() {
        this.outgoing.clear();
    }

    public Vertex getVertex() {
        return this.vertex;
    }

    public void mergeInMessenger(final SparkMessenger<M> otherMessenger) {
        this.vertex = otherMessenger.vertex;
        this.outgoing = otherMessenger.outgoing;
    }

    public void addIncomingMessages(final SparkMessenger<M> otherMessenger, final Optional<MessageCombiner<M>> messageCombinerOptional) {
        if (messageCombinerOptional.isPresent()) {
            final MessageCombiner<M> messageCombiner = messageCombinerOptional.get();
            final M combinedMessage = Stream.concat(this.incoming.stream(), otherMessenger.incoming.stream()).reduce(messageCombiner::combine).get();
            this.incoming.clear();
            this.incoming.add(combinedMessage);
        } else {
            this.incoming.addAll(otherMessenger.incoming);
        }
    }

    public Set<Map.Entry<Object, List<M>>> getOutgoingMessages() {
        return this.outgoing.entrySet();
    }

    @Override
    public Iterable<M> receiveMessages(final MessageScope messageScope) {
        if (null == this.outgoing)
            throw new IllegalStateException("Message vertices can not receive messages");

        return null == this.incoming ? Collections.emptyList() : this.incoming;
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        if (null == this.outgoing)
            throw new IllegalStateException("Message vertices can not send messages");

        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal.Admin<Vertex, Edge> incidentTraversal = SparkMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get(), this.vertex);
            final Direction direction = SparkMessenger.getOppositeDirection(incidentTraversal);
            incidentTraversal.forEachRemaining(edge -> {
                final Object otherVertexId = edge.iterators().vertexIterator(direction).next().id();
                List<M> messages = this.outgoing.get(otherVertexId);
                if (null == messages) {
                    messages = new ArrayList<>();
                    this.outgoing.put(otherVertexId, messages);
                }
                messages.add(message);
            });
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> {
                List<M> messages = this.outgoing.get(v.id());
                if (null == messages) {
                    messages = new ArrayList<>();
                    this.outgoing.put(v.id(), messages);
                }
                messages.add(message);
            });
        }
    }

    ///////////

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(final Traversal<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.asAdmin().addStep(0, new StartStep<>(incidentTraversal.asAdmin(), vertex));
        return (T) incidentTraversal;
    }

    private static Direction getOppositeDirection(final Traversal.Admin<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
        return step.getDirection().opposite();
    }

}
