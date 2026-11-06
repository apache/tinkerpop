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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddVertexStep<S> extends ScalarMapStep<S, Vertex> implements AddVertexStepContract<S> {

    private Parameters internalParameters = new Parameters();
    private Parameters withConfiguration = new Parameters();
    private CallbackRegistry<Event.VertexAddedEvent> callbackRegistry;
    private boolean userProvidedLabel;
    private Object label;

    public AddVertexStep(final Traversal.Admin traversal, final String label) {
        super(traversal);
        setLabel(label);
    }

    public AddVertexStep(final Traversal.Admin traversal, final Traversal.Admin<S,?> vertexLabelTraversal) {
        super(traversal);
        setLabel(vertexLabelTraversal);
    }

    public AddVertexStep(final Traversal.Admin traversal, final Set<String> labels) {
        super(traversal);
        setLabel(labels);
    }

    /**
     * Constructor for multiple label traversals. Each traversal is resolved to a single String
     * label at execution time.
     */
    public AddVertexStep(final Traversal.Admin traversal, final List<Traversal.Admin<?, ?>> labelTraversals) {
        super(traversal);
        setLabel(labelTraversals);
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
        if (this.label instanceof Traversal.Admin) {
            children.add((Traversal.Admin<S, E>) this.label);
        } else if (this.label instanceof Collection) {
            for (final Object l : (Collection<?>) this.label) {
                if (l instanceof Traversal.Admin) {
                    children.add((Traversal.Admin<S, E>) l);
                }
            }
        }
        return children;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }

    private void configureInternalParams(final Object... keyValues) {
        if (keyValues[0] == T.label) {
            setLabel(keyValues[1]);
            return;
        }
        if (keyValues[0] == T.id && this.internalParameters.contains(T.id)) {
            throw new IllegalArgumentException(String.format("Vertex T.id has already been set to [%s] and cannot be overridden with [%s]",
                    this.internalParameters.getRaw().get(T.id).get(0), keyValues[1]));
        } else {
            this.internalParameters.set(this, keyValues);
        }
    }

    @Override
    protected Vertex map(final Traverser.Admin<S> traverser) {
        final Object[] otherKeyValues = this.internalParameters.getKeyValues(traverser);
        final Object[] keyValues;
        if (this.label instanceof Collection) {
            // Multi-value label: resolve each element to a single String label
            final Set<String> labels = resolveLabelCollection(traverser, (Collection<?>) this.label);
            keyValues = new Object[otherKeyValues.length + 2];
            keyValues[0] = T.label;
            keyValues[1] = labels;
            System.arraycopy(otherKeyValues, 0, keyValues, 2, otherKeyValues.length);
        } else if (this.label != null) {
            final Object resolvedLabel = this.label instanceof Traversal.Admin ?
                    TraversalUtil.apply(traverser, (Traversal.Admin<S, ?>) this.label) : this.label;
            keyValues = new Object[otherKeyValues.length + 2];
            keyValues[0] = T.label;
            keyValues[1] = resolvedLabel;
            System.arraycopy(otherKeyValues, 0, keyValues, 2, otherKeyValues.length);
        } else {
            keyValues = otherKeyValues;
        }
        final Vertex vertex = this.getTraversal().getGraph().get().addVertex(keyValues);
        EventUtil.registerVertexCreation(callbackRegistry, getTraversal(), vertex);
        return vertex;
    }

    private static <S> Set<String> resolveLabelCollection(final Traverser.Admin<S> traverser, final Collection<?> label) {
        final Set<String> labels = new LinkedHashSet<>();
        for (final Object l : label) {
            final Object result = l instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, ?>) l) : l;
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
        return labels;
    }

    @Override
    public CallbackRegistry<Event.VertexAddedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode() ^ this.internalParameters.hashCode() ^ this.withConfiguration.hashCode();
        if (this.label != null) {
            hash ^= this.label.hashCode();
        }
        return hash;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
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
        this.getLocalChildren().forEach(this::integrateChild);
    }

    @Override
    public AddVertexStep<S> clone() {
        final AddVertexStep<S> clone = (AddVertexStep<S>) super.clone();
        clone.internalParameters = this.internalParameters.clone();
        clone.withConfiguration = this.withConfiguration.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        if (this.label instanceof Traversal.Admin) {
            clone.label = ((Traversal.Admin<?, ?>) this.label).clone();
        } else if (this.label instanceof Collection) {
            final LinkedHashSet<Object> clonedLabels = new LinkedHashSet<>();
            for (final Object l : (Collection<?>) this.label) {
                clonedLabels.add(l instanceof Traversal.Admin ? ((Traversal.Admin<?, ?>) l).clone() : l);
            }
            clone.label = clonedLabels;
        } else {
            clone.label = this.label;
        }
        return clone;
    }

    @Override
    public Object getLabel() {
        if (this.label == null) {
            return Vertex.DEFAULT_LABEL;
        }
        if (this.label instanceof Collection) {
            final Set<Object> resolved = new LinkedHashSet<>();
            for (final Object l : (Collection<?>) this.label) {
                resolved.add(resolveStaticLabel(l));
            }
            return resolved;
        }
        return resolveStaticLabel(this.label);
    }

    /**
     * Resolves a single label element. Handles {@code ConstantTraversal}, which can be resolved to a
     * {@code String} without a live {@code Traverser}. Any other {@code Traversal} cannot be resolved
     * statically and is returned as-is.
     */
    private static Object resolveStaticLabel(final Object label) {
        if (label instanceof ConstantTraversal) {
            return ((ConstantTraversal<?, ?>) label).next();
        }
        return label;
    }

    @Override
    public void setLabel(Object label) {
        if (label == null) {
            this.label = null;
            userProvidedLabel = false;
            return;
        }
        if (userProvidedLabel) {
            throw new IllegalArgumentException(String.format("Vertex T.label has already been set to [%s] and cannot be overridden with [%s]",
                    this.label, label));
        }
        if (label instanceof Collection) {
            final Collection<?> labels = (Collection<?>) label;
            if (labels.isEmpty()) {
                this.label = null;
                userProvidedLabel = false;
                return;
            }
            this.label = new LinkedHashSet<>(labels);
            for (final Object l : labels) {
                if (l instanceof Traversal) {
                    this.integrateChild(((Traversal<?, ?>) l).asAdmin());
                }
            }
        } else if (label instanceof Traversal) {
            this.label = ((Traversal<S, String>) label).asAdmin();
            this.integrateChild((Traversal.Admin<?, ?>) this.label);
        } else {
            this.label = label;
        }
        userProvidedLabel = true;
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
