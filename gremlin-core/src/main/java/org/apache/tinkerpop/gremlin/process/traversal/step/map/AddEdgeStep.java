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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class AddEdgeStep<S> extends FlatMapStep<S, Edge> implements Scoping, Mutating<Event.EdgeAddedEvent> {

    private final Direction direction;
    private final String firstVertexKey;
    private final String edgeLabel;
    private final String secondVertexKey;
    private final Object[] propertyKeyValues;

    private CallbackRegistry<Event.EdgeAddedEvent> callbackRegistry;

    public AddEdgeStep(final Traversal.Admin traversal, final Direction direction, final String firstVertexKey, final String edgeLabel, final String secondVertexKey, final Object... propertyKeyValues) {
        super(traversal);
        this.direction = direction;
        this.firstVertexKey = firstVertexKey;
        this.edgeLabel = edgeLabel;
        this.secondVertexKey = secondVertexKey;
        this.propertyKeyValues = propertyKeyValues;
    }

    public Direction getDirection() {
        return this.direction;
    }

    public String getFirstVertexKey() {
        return this.firstVertexKey;
    }

    public String getEdgeLabel() {
        return this.edgeLabel;
    }

    public String getSecondVertexKey() {
        return this.secondVertexKey;
    }

    public Object[] getPropertyKeyValues() {
        return this.propertyKeyValues;
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<S> traverser) {
        final Object firstVertex = null == this.firstVertexKey ? (Vertex) traverser.get() : this.getScopeValue(Pop.last, this.firstVertexKey, traverser);
        final Object secondVertex = null == this.secondVertexKey ? (Vertex) traverser.get() : this.getScopeValue(Pop.last, this.secondVertexKey, traverser);
        final Object finalFirstVertex = firstVertex instanceof Iterable ? ((Iterable) firstVertex).iterator() : firstVertex;
        final Object finalSecondVertex = secondVertex instanceof Iterable ? ((Iterable) secondVertex).iterator() : secondVertex;

        final Iterator<Edge> edgeIterator;
        if (finalFirstVertex instanceof Iterator) {
            edgeIterator = IteratorUtils.map((Iterator<Vertex>) finalFirstVertex, vertex ->
                    this.direction.equals(Direction.OUT) ?
                            vertex.addEdge(this.edgeLabel, (Vertex) finalSecondVertex, this.propertyKeyValues) :
                            ((Vertex) finalSecondVertex).addEdge(this.edgeLabel, vertex, this.propertyKeyValues));
        } else if (finalSecondVertex instanceof Iterator) {
            edgeIterator = IteratorUtils.map((Iterator<Vertex>) finalSecondVertex, vertex ->
                    this.direction.equals(Direction.OUT) ?
                            ((Vertex) finalFirstVertex).addEdge(this.edgeLabel, vertex, this.propertyKeyValues) :
                            vertex.addEdge(this.edgeLabel, ((Vertex) finalFirstVertex), this.propertyKeyValues));
        } else {
            edgeIterator = IteratorUtils.of(this.direction.equals(Direction.OUT) ?
                    ((Vertex) firstVertex).addEdge(this.edgeLabel, (Vertex) secondVertex, this.propertyKeyValues) :
                    ((Vertex) secondVertex).addEdge(this.edgeLabel, (Vertex) firstVertex, this.propertyKeyValues));
        }

        return IteratorUtils.consume(edgeIterator, edge -> {
            if (callbackRegistry != null) {
                final Event.EdgeAddedEvent vae = new Event.EdgeAddedEvent(DetachedFactory.detach(edge, true));
                callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
            }
        });
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalHelper.getLabels(TraversalHelper.getRootTraversal(this.traversal)).stream().filter(this.getScopeKeys()::contains).findAny().isPresent() ?
                TYPICAL_GLOBAL_REQUIREMENTS :
                TYPICAL_LOCAL_REQUIREMENTS;
    }

    @Override
    public CallbackRegistry<Event.EdgeAddedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.direction.hashCode() ^ this.edgeLabel.hashCode();
        if (null != this.firstVertexKey)
            result ^= this.firstVertexKey.hashCode();
        if (null != this.secondVertexKey)
            result ^= this.secondVertexKey.hashCode();
        for (final Object object : this.propertyKeyValues) {
            result ^= object.hashCode();
        }
        return result;
    }

    @Override
    public Set<String> getScopeKeys() {
        final Set<String> keys = new HashSet<>();
        if (null != this.firstVertexKey)
            keys.add(this.firstVertexKey);
        if (null != this.secondVertexKey)
            keys.add(this.secondVertexKey);
        return keys;
    }
}
