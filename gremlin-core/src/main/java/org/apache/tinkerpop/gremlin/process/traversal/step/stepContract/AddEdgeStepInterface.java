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
package org.apache.tinkerpop.gremlin.process.traversal.step.stepContract;

import java.util.List;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.FromToModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

public interface AddEdgeStepInterface<S> extends TraversalParent, Scoping, FromToModulating, AddElementStepInterface<S, Edge>, Writing<Event.EdgeAddedEvent> {
    Object getFrom();
    Object getTo();

    default Object getAdjacentVertex(Parameters parameters, String key) {
        Object vertex;
        List<Object> values = parameters.get(key, () -> null);
        vertex = values.isEmpty() ? null : values.get(0);
        if (vertex instanceof ConstantTraversal) {
            vertex = ((ConstantTraversal<?, ?>) vertex).next();
        }
        if (vertex != null && !(vertex instanceof Traversal) && !(vertex instanceof Vertex)) {
            vertex = new ReferenceVertex(vertex);
        }
        return vertex;
    }

    default Vertex getAdjacentVertex(Parameters parameters, String key, Traverser.Admin<S> traverser, String edgeLabel) {
        Object vertex;
        try {
            List<Object> values = parameters.get(traverser, key, traverser::get);
            vertex = values.isEmpty() ? null : values.get(0);
            if (vertex != null && !(vertex instanceof Vertex)) {
                vertex = new ReferenceVertex(vertex);
            }
        } catch (IllegalArgumentException e) { // as thrown by TraversalUtil.apply()
            throw new IllegalStateException(String.format(
                    "addE(%s) failed because the %s() traversal (which should give a Vertex) failed with: %s",
                    edgeLabel, key, e.getMessage()));
        }

        if (vertex == null)
            throw new IllegalStateException(String.format(
                    "The value given to addE(%s).%s() must resolve to a Vertex or the ID of a Vertex present in the graph, but null was specified instead", edgeLabel, key));
        return (Vertex) vertex;
    }

    default Object resolveVertexTraversal(Traversal.Admin<?,?> vertexTraversal) {
        return resolveVertexTraversal(vertexTraversal, null);
    }

    default Object resolveVertexTraversal(Traversal.Admin<?,?> vertexTraversal, Consumer<GValue<?>> gValueConsumer) {
        if (vertexTraversal instanceof GValueConstantTraversal) {
            if (gValueConsumer != null) {
                gValueConsumer.accept(((GValueConstantTraversal<?, ?>) vertexTraversal).getGValue());
            }
            return new ReferenceVertex(vertexTraversal.next());
        }
        // anything other than a GValueConstantTraversal is returned as-is
        return vertexTraversal;
    }
}
