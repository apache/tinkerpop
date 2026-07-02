/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddVertexStartStep extends AbstractStep<Vertex, Vertex> implements AddVertexStepContract<Vertex> {

    private Parameters internalParameters = new Parameters();
    private Parameters withConfiguration = new Parameters();
    private boolean first = true;
    private CallbackRegistry<Event.VertexAddedEvent> callbackRegistry;
    private boolean userProvidedLabel;
    private List<Traversal.Admin<?, ?>> labelTraversals;

    public AddVertexStartStep(final Traversal.Admin traversal, final String label) {
        super(traversal);
        if (label != null) {
            this.internalParameters.set(this, T.label, label);
        }
        userProvidedLabel = label != null;
    }

    public AddVertexStartStep(final Traversal.Admin traversal, final Traversal<?, ?> vertexLabelTraversal) {
        super(traversal);
        if (vertexLabelTraversal != null) {
            this.internalParameters.set(this, T.label, vertexLabelTraversal);
        }
        userProvidedLabel = vertexLabelTraversal != null;
    }

    public AddVertexStartStep(final Traversal.Admin traversal, final Set<String> labels) {
        super(traversal);
        if (labels != null && !labels.isEmpty()) {
            this.internalParameters.set(this, T.label, labels);
            userProvidedLabel = true;
        } else {
            userProvidedLabel = false;
        }
    }

    /**
     * Constructor for multiple label traversals. Each traversal is resolved to a single String
     * label at execution time.
     */
    public AddVertexStartStep(final Traversal.Admin traversal, final List<Traversal.Admin<?, ?>> labelTraversals) {
        super(traversal);
        if (labelTraversals != null && !labelTraversals.isEmpty()) {
            this.labelTraversals = new ArrayList<>(labelTraversals);
            for (final Traversal.Admin<?, ?> t : this.labelTraversals) {
                this.integrateChild(t);
            }
            userProvidedLabel = true;
        } else {
            userProvidedLabel = false;
        }
    }

    @Override
    public boolean hasUserProvidedLabel() {
        return userProvidedLabel;
    }

    @Override
    public Object getElementId() {
        List<Object> ids = this.internalParameters.get(T.id, null);
        return ids.isEmpty() ? null : ids.get(0);
    }

    @Override
    public void setElementId(Object elementId) {
        configureInternalParams(T.id, elementId);
    }

    @Override
    public Parameters getParameters() {
        return this.withConfiguration;
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.internalParameters.getReferencedLabels();
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        final List<Traversal.Admin<S, E>> children = new ArrayList<>(this.internalParameters.getTraversals());
        if (this.labelTraversals != null) {
            for (final Traversal.Admin<?, ?> t : this.labelTraversals) {
                children.add((Traversal.Admin<S, E>) t);
            }
        }
        return children;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }

    private void configureInternalParams(final Object... keyValues) {
        if (keyValues[0] == T.label && this.internalParameters.contains(T.label)) {
            if (this.internalParameters.contains(T.label, Vertex.DEFAULT_LABEL)) {
                this.internalParameters.remove(T.label);
                this.internalParameters.set(this, keyValues);
            } else {
                throw new IllegalArgumentException(String.format("Vertex T.label has already been set to [%s] and cannot be overridden with [%s]",
                        this.internalParameters.getRaw().get(T.label).get(0), keyValues[1]));
            }
        } else if (keyValues[0] == T.id && this.internalParameters.contains(T.id)) {
            throw new IllegalArgumentException(String.format("Vertex T.id has already been set to [%s] and cannot be overridden with [%s]",
                    this.internalParameters.getRaw().get(T.id).get(0), keyValues[1]));
        } else {
            this.internalParameters.set(this, keyValues);
        }
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        if (this.first) {
            this.first = false;
            final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();
            final Traverser.Admin<Vertex> dummyTraverser = generator.generate(false, (Step) this, 1L);
            final Object[] keyValues;
            if (this.labelTraversals != null) {
                // Multi-traversal: resolve each traversal to a single String label
                final Set<String> labels = new LinkedHashSet<>();
                for (final Traversal.Admin<?, ?> t : this.labelTraversals) {
                    final Object result = TraversalUtil.apply(dummyTraverser, (Traversal.Admin<Vertex, ?>) t);
                    if (result == null) {
                        throw new IllegalArgumentException("Label traversal must not produce null");
                    }
                    if (result instanceof Collection) {
                        throw new IllegalArgumentException("Label traversal must produce a scalar String when multiple traversals are provided, but got a Collection");
                    }
                    if (!(result instanceof String)) {
                        throw new IllegalArgumentException(String.format("Label traversal must produce a String, but got %s", result.getClass().getSimpleName()));
                    }
                    ElementHelper.validateLabel((String) result);
                    labels.add((String) result);
                }
                final Object[] otherKeyValues = this.internalParameters.getKeyValues(dummyTraverser);
                keyValues = new Object[otherKeyValues.length + 2];
                keyValues[0] = T.label;
                keyValues[1] = labels;
                System.arraycopy(otherKeyValues, 0, keyValues, 2, otherKeyValues.length);
            } else {
                keyValues = this.internalParameters.getKeyValues(dummyTraverser);
            }
            final Vertex vertex = this.getTraversal().getGraph().get().addVertex(keyValues);
            EventUtil.registerVertexCreation(callbackRegistry, getTraversal(), vertex);
            return generator.generate(vertex, this, 1L);
        } else
            throw FastNoSuchElementException.instance();
    }

    @Override
    public CallbackRegistry<Event.VertexAddedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode() ^ this.internalParameters.hashCode() ^ this.withConfiguration.hashCode();
        if (this.labelTraversals != null) {
            hash ^= this.labelTraversals.hashCode();
        }
        return hash;
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.internalParameters);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.internalParameters.getTraversals().forEach(this::integrateChild);
        this.withConfiguration.getTraversals().forEach(this::integrateChild);
        if (this.labelTraversals != null) {
            this.labelTraversals.forEach(this::integrateChild);
        }
    }

    @Override
    public AddVertexStartStep clone() {
        final AddVertexStartStep clone = (AddVertexStartStep) super.clone();
        clone.internalParameters = this.internalParameters.clone();
        clone.withConfiguration = this.withConfiguration.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        if (this.labelTraversals != null) {
            clone.labelTraversals = new ArrayList<>(this.labelTraversals.size());
            for (final Traversal.Admin<?, ?> t : this.labelTraversals) {
                clone.labelTraversals.add(t.clone());
            }
        }
        return clone;
    }

    @Override
    public Object getLabel() {
        Object label = internalParameters.get(T.label, () -> Vertex.DEFAULT_LABEL).get(0);
        if (label instanceof ConstantTraversal) {
            return ((ConstantTraversal<?, ?>) label).next();
        }
        return label;
    }

    @Override
    public void setLabel(Object label) {
        this.configure(T.label, label);
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        return Collections.unmodifiableMap(internalParameters.getRaw(T.id, T.label));
    }

    @Override
    public boolean removeProperty(Object k) {
        if (internalParameters.contains(k)) {
            internalParameters.remove(k);
            return true;
        }
        return false;
    }

    @Override
    public boolean removeElementId() {
        if (this.internalParameters.contains(T.id)) {
            this.internalParameters.remove(T.id);
            return true;
        }
        return false;
    }

    @Override
    public void addProperty(final Object key, final Object value) {
        configureInternalParams(key, value);
    }
}
