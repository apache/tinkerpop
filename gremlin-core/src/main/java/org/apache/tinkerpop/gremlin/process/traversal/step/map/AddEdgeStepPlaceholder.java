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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class AddEdgeStepPlaceholder<S> extends AbstractAddElementStepPlaceholder<S, Edge, Event.EdgeAddedEvent>
        implements AddEdgeStepContract<S> {

    private Traversal.Admin<?,?> from;
    private Traversal.Admin<?,?> to;

    public AddEdgeStepPlaceholder(final Traversal.Admin traversal, final String edgeLabel) {
        super(traversal, edgeLabel == null ? Edge.DEFAULT_LABEL : edgeLabel);
    }

    public AddEdgeStepPlaceholder(final Traversal.Admin traversal, final GValue<String> edgeLabel) {
        super(traversal, edgeLabel == null || edgeLabel.isNull() ? GValue.of(Edge.DEFAULT_LABEL) : edgeLabel);
    }

    public AddEdgeStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> edgeLabelTraversal) {
        super(traversal, edgeLabelTraversal == null ?
                new ConstantTraversal<>(Edge.DEFAULT_LABEL) : edgeLabelTraversal);
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
    public List<Traversal.Admin<S, Edge>> getLocalChildren() {
        final List<Traversal.Admin<S, Edge>> childTraversals = super.getLocalChildren();
        if (from != null) childTraversals.add((Traversal.Admin) from);
        if (to != null) childTraversals.add((Traversal.Admin) to);
        return childTraversals;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        if (from != null) {
            hash ^= from.hashCode();
        }
        if (to != null) {
            hash ^= to.hashCode();
        }
        return hash;
    }

    @Override
    public AddEdgeStep<S> asConcreteStep() {
        AddEdgeStep<S> step = new AddEdgeStep<>(traversal, label instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) label).getConstantTraversal() : label);
        super.configureConcreteStep(step);
        if (from != null) {
            step.addFrom(from instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) from).getConstantTraversal() : from);
        }
        if (to != null) {
            step.addTo(to instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) to).getConstantTraversal() : to);
        }
        return step;
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
    protected boolean supportsMultiProperties() {
        return false;
    }

    @Override
    public Object getFrom() {
        return resolveVertexTraversal(from, gValue -> traversal.getGValueManager().pinVariable(gValue.getName()));
    }

    public Object getFromGValueSafe() {
        return resolveVertexTraversal(from);
    }

    @Override
    public Object getTo() {
        return resolveVertexTraversal(to, gValue -> traversal.getGValueManager().pinVariable(gValue.getName()));
    }

    public Object getToGValueSafe() {
        return resolveVertexTraversal(to);
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
    public AddEdgeStepPlaceholder<S> clone() {
        final AddEdgeStepPlaceholder<S> clone = (AddEdgeStepPlaceholder) super.clone();
        if (from != null){
            clone.from = from.clone();
        }
        if (to != null){
            clone.to = to.clone();
        }
        return clone;
    }
}
