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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddEdgeStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddEdgeStartStepPlaceholder<S> extends AbstractStep<S, Edge>
        implements AddEdgeStepInterface<S>, GValueHolder<S, Edge>, PropertyAdding {

    private Traversal.Admin<S,String> label;
    private Traversal.Admin<?,?> from;
    private Traversal.Admin<?,?> to;
    private Map<Object, List<Object>> properties = new HashMap<>();
    private GValue<Object> elementId;
    private Set<String> scopeKeys = new HashSet<>();

    public AddEdgeStartStepPlaceholder(final Traversal.Admin traversal, final String edgeLabel) {
        this(traversal, new ConstantTraversal<>(edgeLabel));
    }

    public AddEdgeStartStepPlaceholder(final Traversal.Admin traversal, final GValue<String> edgeLabel) {
        this(traversal.asAdmin(), new GValueConstantTraversal<>(edgeLabel));
    }

    public AddEdgeStartStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> edgeLabelTraversal) {
        super(traversal);
        this.label = edgeLabelTraversal;
        if (edgeLabelTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().track(((GValueConstantTraversal<S, String>) edgeLabelTraversal).getGValue());
        }
        addTraversal(edgeLabelTraversal);
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.scopeKeys;
    }

    private void addTraversal(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClassRecursively(Scoping.class, traversal).forEach(s -> scopeKeys.addAll(s.getScopeKeys()));
    }

    @Override
    public void addTo(final Traversal.Admin<?, ?> toObject) {
        addTraversal(toObject);
        if (toObject instanceof GValueConstantTraversal) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, ?>) toObject).getGValue());
        }
        this.to = toObject;
    }

    @Override
    public void addFrom(final Traversal.Admin<?, ?> fromObject) {
        addTraversal(fromObject);
        if (fromObject instanceof GValueConstantTraversal) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, ?>) fromObject).getGValue());
        }
        this.from = fromObject;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    protected Traverser.Admin<Edge> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("GraphStepGValueContract is not executable");
    }

    @Override
    public Step asConcreteStep() {

        AddEdgeStartStep step = new AddEdgeStartStep(traversal, label instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) label).getConstantTraversal() : label);

        if (from != null) {
            step.addFrom(from instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) from).getConstantTraversal() : from);
        }
        if (to != null) {
            step.addTo(to instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) to).getConstantTraversal() : to);
        }
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value);
            }
        }

        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        if ((label instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) label).isParameterized()) ||
                (from instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) from).isParameterized()) ||
                (to instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) to).isParameterized())) {
            return true;
        }
        for (List<Object> list : properties.values()) {
            if (GValue.containsVariables(list.toArray())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getLabel() {
        if (label instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, ?>) label).getGValue().getName());
        }
        return label.next();
    }

    public String getLabelGValueSafe() {
        return label.next();
    }

    private void setLabel(Object label) {// TODO this be public and added to step interface?
        if (getLabelGValueSafe().equals(Vertex.DEFAULT_LABEL)) {
            this.label = label instanceof GValue ? new GValueConstantTraversal<>((GValue) label) : new ConstantTraversal<>((String) label);
        }
    }

    @Override
    public void addProperty(Object key, Object value) {
        if (key instanceof GValue) {
            throw new IllegalArgumentException("GValue cannot be used as a property key");
        }
        if (value instanceof GValue) { //TODO could value come in as a traversal?
            traversal.getGValueManager().track((GValue<?>) value);
        }
        if (key == T.label) { //todo copy to similar steps
            setLabel(value);
            return;
        }
        if (key == T.id) {
            setElementId(value);
            return;
        }
        if (properties.containsKey(key)) {
            throw new IllegalArgumentException("Edges only support properties with single cardinality");
        }
        properties.put(key, Collections.singletonList(value));
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        for (List<Object> list : properties.values()) {
            for (Object value : list) {
                if (value instanceof GValue) {
                    traversal.getGValueManager().pinVariable(((GValue<?>) value).getName());
                }
            }
        }
        return properties;
    }

    public Map<Object, List<Object>> getPropertiesGValueSafe() {
        return properties;
    }

    @Override
    public void removeProperty(Object k) {
        properties.remove(k);
    }

    @Override
    public Vertex getFrom() {
        if (from instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, ?>) from).getGValue().getName());
        }
        return from == null ? null : (Vertex) from.next();
    }

    public Vertex getFromGValueSafe() {
        return from == null ? null : (Vertex) from.next();
    }

    @Override
    public Vertex getTo() {
        if (to instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, ?>) to).getGValue().getName());
        }
        return to == null ? null : (Vertex) to.next();
    }

    @Override
    public Object getElementId() {
        if (elementId == null) {
            return null;
        }
        this.traversal.getGValueManager().pinVariable(elementId.getName());
        return elementId.get();
    }

    public Object getElementIdGValueSafe() {
        if (elementId == null) {
            return null;
        }
        return elementId.get();
    }

    @Override
    public void setElementId(Object elementId) {
        this.elementId = elementId instanceof GValue ? (GValue<Object>) elementId : GValue.of(elementId);
        this.traversal.getGValueManager().track(this.elementId);
    }

    public Vertex getToGValueSafe() {
        return to == null ? null : (Vertex) to.next();
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (label instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<S, String>) label).updateVariable(name, value);
        }
        if (from instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<S, String>) from).updateVariable(name, value);
        }
        if (to instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<S, String>) to).updateVariable(name, value);
        }
        if (elementId != null && name.equals(elementId.getName())) {
            elementId = GValue.of(name, value); //TODO add to equivalent steps
        }
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object propertyVal : entry.getValue()) {
                if (propertyVal instanceof GValue && name.equals(((GValue<?>) propertyVal).getName())) {
                    properties.put(entry.getKey(), Collections.singletonList(GValue.of(name, value))); //TODO type check?
                }
            }
        }

    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Set<GValue<?>> gValues = new HashSet<>();
        if (label instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) label).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<S, String>) label).getGValue());
        }
        if (from instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) from).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<S, String>) from).getGValue());
        }
        if (to instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) to).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<S, String>) to).getGValue());
        }
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object propertyVal : entry.getValue()) {
                if (propertyVal instanceof GValue && ((GValue<?>) propertyVal).isVariable()) {
                    gValues.add((GValue<?>) propertyVal);
                }
            }
        }
        return gValues;
    }

    @Override
    public CallbackRegistry<Event.EdgeAddedEvent> getMutatingCallbackRegistry() {
        throw new IllegalStateException("Cannot get mutating CallbackRegistry on GValue placeholder step");
    }
}
