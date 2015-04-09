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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceEdge extends ReferenceElement<Edge> {

    private ReferenceEdge() {

    }

    public ReferenceEdge(final Edge edge) {
        super(edge);
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        final Iterator<Edge> edges = IteratorUtils.filter(hostVertex.edges(Direction.OUT), edge -> edge.id().equals(this.id));
        if (!edges.hasNext())
            throw new IllegalStateException("The reference edge could not be found incident to the provided vertex: " + this);
        return edges.next();
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        final Iterator<Edge> edges = hostGraph.edges(this.id);
        if (!edges.hasNext())
            throw new IllegalStateException("The reference edge could not be found in the provided graph: " + this);
        return edges.next();
    }

    @Override
    public String toString() {
        return "e*[" + this.id + "]";
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof ReferenceEdge)
            return this.id.equals(((ReferenceEdge) object).id);
        else
            return object instanceof Edge && this.id.equals(((Edge) object).id());
    }

}
