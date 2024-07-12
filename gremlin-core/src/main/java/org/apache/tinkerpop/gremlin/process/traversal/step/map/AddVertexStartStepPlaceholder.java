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
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddVertexStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddVertexStartStepPlaceholder extends AbstractStep<Vertex, Vertex>
        implements AddVertexStepInterface<Vertex>, GValueHolder<Vertex, Vertex> {

    private boolean userProvidedLabel;
    private Map<Object, List<Object>> properties = new HashMap<>();
    private Traversal.Admin<?,String> label;
    private GValue<Object> elementId;

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final String label) {
        this(traversal, null ==  label ? null : new ConstantTraversal<>(label));
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        this(traversal, null ==  label ? null : new GValueConstantTraversal<>(label));
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<?,String> vertexLabelTraversal) {
        super(traversal);
        this.label = null == vertexLabelTraversal ? new ConstantTraversal<>(Vertex.DEFAULT_LABEL) : vertexLabelTraversal;
        userProvidedLabel = vertexLabelTraversal != null;
        if (vertexLabelTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().track(((GValueConstantTraversal<?, String>) vertexLabelTraversal).getGValue());
        }
    }

    @Override
    public boolean hasUserProvidedLabel() {
        return userProvidedLabel;
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

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.label) ^ Objects.hashCode(this.properties);
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("GValuePlaceholder step is not executable");
    }

    @Override
    public AddVertexStartStepPlaceholder clone() {
        final AddVertexStartStepPlaceholder clone = (AddVertexStartStepPlaceholder) super.clone();
        clone.label = this.label.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        clone.properties = new HashMap<>(this.properties);
        return clone;
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
    public void addProperty(Object key, Object value) {
        if (key instanceof GValue) {
            throw new IllegalArgumentException("GValue cannot be used as a property key");
        }
        if (value instanceof GValue) { //TODO could value come in as a traversal?
            traversal.getGValueManager().track((GValue<?>) value);
        }
        List<Object> values = properties.get(key);
        if (values == null) {
            values = new ArrayList<>();
            properties.put(key, values);
        }
        values.add(value);
    }

    @Override
    public Step asConcreteStep() {
        AddVertexStartStep step = new AddVertexStartStep(traversal, label instanceof GValueConstantTraversal ? ((GValueConstantTraversal<?, String>) label).getConstantTraversal() : label);

        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value instanceof GValue ? ((GValue<?>) value).get() : value);
            }
        }

        return step;
    }

    @Override
    public boolean isParameterized() {
        if (label instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, String>) label).isParameterized()) {
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
    public void updateVariable(String name, Object value) {
        if (label instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<?, String>) label).updateVariable(name, value);
        }
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            List<Object> values = entry.getValue();
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) instanceof GValue && name.equals(((GValue<?>) values.get(i)).getName())) {
                    values.set(i, value);
                }
            }
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Set<GValue<?>> gValues = new HashSet<>();
        if (label instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, String>) label).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<?, String>) label).getGValue());
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
}
