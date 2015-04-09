/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceEdge extends ReferenceElement<Edge> implements Edge {

    private ReferenceEdge() {

    }

    public ReferenceEdge(final Edge edge) {
        super(edge);
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        final Iterator<Edge> edges = IteratorUtils.filter(hostVertex.edges(Direction.OUT), edge -> edge.id().equals(this.id));
        if (!edges.hasNext())
            throw Attachable.Exceptions.canNotAttachEdgeToHostVertex(this, hostVertex);
        return edges.next();
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        final Iterator<Edge> edges = hostGraph.edges(this.id);
        if (!edges.hasNext())
            throw Attachable.Exceptions.canNotAttachEdgeToHostGraph(this, hostGraph);
        return edges.next();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return Collections.emptyIterator();
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return "e[" + this.id + "]";
    }
}
