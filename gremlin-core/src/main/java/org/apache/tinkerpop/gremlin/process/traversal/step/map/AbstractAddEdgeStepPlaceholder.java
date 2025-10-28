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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public abstract class AbstractAddEdgeStepPlaceholder<S> extends AbstractAddElementStepPlaceholder<S, Edge, Event.EdgeAddedEvent> implements AddEdgeStepContract<S> {
    protected Traversal.Admin<?, ?> from;
    protected Traversal.Admin<?, ?> to;

    public AbstractAddEdgeStepPlaceholder(Traversal.Admin traversal, String label) {
        super(traversal, label);
    }

    public AbstractAddEdgeStepPlaceholder(Traversal.Admin traversal, GValue<String> label) {
        super(traversal, label);
    }

    public AbstractAddEdgeStepPlaceholder(Traversal.Admin traversal, Traversal.Admin<S, String> labelTraversal) {
        super(traversal, labelTraversal);
    }

    @Override
    protected String getDefaultLabel() {
        return Edge.DEFAULT_LABEL;
    }

    @Override
    public void addTo(final Traversal.Admin<?, ?> toObject) {
        addTraversal(toObject);
        if (toObject instanceof GValueConstantTraversal) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, ?>) toObject).getGValue());
        }
        this.to = toObject;
        this.integrateChild(this.to);
    }

    @Override
    public void addFrom(final Traversal.Admin<?, ?> fromObject) {
        addTraversal(fromObject);
        if (fromObject instanceof GValueConstantTraversal) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, ?>) fromObject).getGValue());
        }
        this.from = fromObject;
        this.integrateChild(this.from);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AbstractAddEdgeStepPlaceholder<?> that = (AbstractAddEdgeStepPlaceholder<?>) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), from, to);
    }

    @Override
    public boolean isParameterized() {
        if (super.isParameterized() ||
                (from instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) from).isParameterized()) ||
                (to instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) to).isParameterized())) {
            return true;
        }
        return false;
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        List<Traversal.Admin<?, ?>> childTraversals = super.getLocalChildren();
        if (from != null) {
            childTraversals.add(from instanceof GValueConstantTraversal ? ((GValueConstantTraversal<?, ?>) from).getConstantTraversal() : from);
        }
        if (to != null) {
            childTraversals.add(to instanceof GValueConstantTraversal ? ((GValueConstantTraversal<?, ?>) to).getConstantTraversal() : to);
        }
        return childTraversals;
    }

    @Override
    protected boolean supportsMultiProperties() {
        return false;
    }

    @Override
    public Object getFrom() {
        return resolveVertexTraversalAndPinVariables(from);
    }

    @Override
    public Object getFromWithGValue() {
        return resolveVertexTraversalAndPreserveGValue(from);
    }

    @Override
    public Object getTo() {
        return resolveVertexTraversalAndPinVariables(to);
    }

    @Override
    public Object getToWithGValue() {
        return resolveVertexTraversalAndPreserveGValue(to);
    }

    @Override
    public void updateVariable(String name, Object value) {
        super.updateVariable(name, value);
        if (from instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<S, String>) from).updateVariable(name, value);
        }
        if (to instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<S, String>) to).updateVariable(name, value);
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Collection<GValue<?>> gValues = super.getGValues();
        if (from instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) from).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<S, String>) from).getGValue());
        }
        if (to instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) to).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<S, String>) to).getGValue());
        }
        return gValues;
    }

    @Override
    public AbstractAddEdgeStepPlaceholder<S> clone() {
        final AbstractAddEdgeStepPlaceholder<S> clone = (AbstractAddEdgeStepPlaceholder) super.clone();
        if (from != null) {
            clone.addFrom(from.clone());
        }
        if (to != null) {
            clone.addTo(to.clone());
        }
        return clone;
    }

    /**
     * Attempts to extract a Vertex from a given Traversal. If the traversal is wrapping a GValue, that variable is pinned.
     * @param vertexTraversal
     * @return If vertexTraversal is a ConstantTraversal, returns a ReferenceVertex, otherwise returns vertexTraversal as-is
     */
    private Object resolveVertexTraversalAndPinVariables(Traversal.Admin<?,?> vertexTraversal) {
        if (vertexTraversal instanceof GValueConstantTraversal) {
            GValue<?> gValue = ((GValueConstantTraversal<?, ?>) vertexTraversal).getGValue();
            if (gValue.isVariable()) {
                traversal.getGValueManager().pinVariable(gValue.getName());
            }
            return new ReferenceVertex(vertexTraversal.next());
        }
        if (vertexTraversal instanceof ConstantTraversal) {
            return new ReferenceVertex(vertexTraversal.next());
        }
        // anything other than a ConstantTraversal is returned as-is
        return vertexTraversal;
    }

    /**
     * Attempts to extract a Vertex from a given Traversal. If the traversal is wrapping a GValue, the GValue is
     * returned without pinning the variable
     * @param vertexTraversal
     * @return If vertexTraversal is a ConstantTraversal, returns the constant vertex or id as a GValue, otherwise returns vertexTraversal as-is
     */
    private Object resolveVertexTraversalAndPreserveGValue(Traversal.Admin<?,?> vertexTraversal) {
        if (vertexTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, ?>) vertexTraversal).getGValue();
        }
        if (vertexTraversal instanceof ConstantTraversal) {
            return new ReferenceVertex(vertexTraversal.next());
        }
        // anything other than a ConstantTraversal is returned as-is
        return vertexTraversal;
    }
}
