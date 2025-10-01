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

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.FromToModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

public interface AddEdgeStepContract<S> extends AddElementStepContract<S, Edge>, FromToModulating, Writing<Event.EdgeAddedEvent> {

    /**
     * Concrete implementations of this contract that can be referenced as TinkerPop implementations.
     */
    List<Class<? extends Step>> CONCRETE_STEPS = List.of(AddEdgeStep.class, AddEdgeStepPlaceholder.class, AddEdgeStartStep.class, AddEdgeStartStepPlaceholder.class);

    /**
     * Gets the "from" vertex for the edge to be added. If the "from" vertex was added as a {@link Vertex}, ID, {@link GValue},
     * or {@link ConstantTraversal}, it is returned as a {@link ReferenceVertex}. Otherwise, it is returned in {@link Traversal} form.
     */
    Object getFrom();

    /**
     * Gets the "to" vertex for the edge to be added. If the "to" vertex was added as a {@link Vertex}, ID, {@link GValue},
     * or {@link ConstantTraversal}, it is returned as a {@link ReferenceVertex}. Otherwise, it is returned in {@link Traversal} form.
     */
    Object getTo();

    /**
     * Gets the "from" vertex for the edge to be added. If the "from" vertex was added as a {@link Vertex}, ID,
     * or {@link ConstantTraversal}, it is returned as a {@link ReferenceVertex}. If it was added as a {@link GValue}
     * containing a {@link Vertex} or ID, the {@link GValue} is returned. Otherwise, it is returned in {@link Traversal} form.
     */
    default Object getFromWithGValue() {
        Object from = getFrom();
        return from instanceof Traversal ? from : GValue.of(from);
    }

    /**
     * Gets the "from" vertex for the edge to be added. If the "from" vertex was added as a {@link Vertex}, ID,
     * or {@link ConstantTraversal}, it is returned as a {@link ReferenceVertex}. If it was added as a {@link GValue}
     * containing a {@link Vertex} or ID, the {@link GValue} is returned. Otherwise, it is returned in {@link Traversal} form.
     */
    default Object getToWithGValue() {
        Object to = getTo();
        return to instanceof Traversal ? to : GValue.of(to);
    }

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


}
