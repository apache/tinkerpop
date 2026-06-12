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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.keyed.KeyedProperty;
import org.apache.tinkerpop.gremlin.structure.util.keyed.KeyedVertexProperty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddPropertyStep<S extends Element> extends SideEffectStep<S> implements AddPropertyStepContract<S> {

    private Parameters internalParameters = new Parameters();
    private Parameters withConfiguration = new Parameters();
    private final VertexProperty.Cardinality cardinality;
    /**
     * Marks the single-argument {@code property(traversal)} form whose child traversal must produce a Map of
     * property key/value pairs. Dispatch is driven by this flag rather than by inspecting the property key.
     */
    private final boolean mapForm;
    private CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry;

    public AddPropertyStep(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        this(traversal, cardinality, keyObject, valueObject, false);
    }

    public AddPropertyStep(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject, final boolean mapForm) {
        super(traversal);
        this.internalParameters.set(this, T.key, keyObject, T.value, valueObject);
        this.cardinality = cardinality;
        this.mapForm = mapForm;
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
        return this.internalParameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Object k = this.internalParameters.get(traverser, T.key, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided key: " + this);
        }).get(0);

        // T identifies immutable components of elements. only property keys can be modified.
        if (k instanceof T)
            throw new IllegalStateException(String.format("T.%s is immutable on existing elements", ((T) k).name()));

        final String key = (String) k;

        // Check if the raw value is a traversal to enable multi-result handling.
        // Exclude ConstantTraversal which is used internally by TinkerPop to wrap literal values.
        final List<Object> rawValues = this.internalParameters.get(T.value, null);
        final boolean valueIsTraversal = !rawValues.isEmpty()
                && rawValues.get(0) instanceof Traversal.Admin
                && !(rawValues.get(0) instanceof ConstantTraversal);

        if (valueIsTraversal) {
            handleTraversalValue(traverser, key);
        } else {
            final Object value = this.internalParameters.get(traverser, T.value, () -> {
                throw new IllegalStateException("The AddPropertyStep does not have a provided value: " + this);
            }).get(0);
            final Object[] vertexPropertyKeyValues = this.internalParameters.getKeyValues(traverser, T.key, T.value);
            applyPropertyMutation(traverser, key, value, vertexPropertyKeyValues);
        }
    }

    /**
     * Handles the case where the value argument is a child traversal, supporting multi-result
     * resolution, Map-producing traversals, and cardinality-aware property setting.
     */
    private void handleTraversalValue(final Traverser.Admin<S> traverser, final String key) {
        final List<Object> rawValues = this.internalParameters.get(T.value, null);
        final Traversal.Admin<?, ?> valueTraversal = (Traversal.Admin<?, ?>) rawValues.get(0);

        // Collect all results from the child traversal
        final List<Object> results = new ArrayList<>();
        final Traverser.Admin<S> trav = traverser;
        final Iterator<?> itty = TraversalUtil.<S, Object>applyAll(trav, (Traversal.Admin<S, Object>) (Traversal.Admin) valueTraversal);
        while (itty.hasNext()) {
            results.add(itty.next());
        }

        // No-result case: skip mutation, pass element through unchanged
        if (results.isEmpty()) {
            return;
        }

        final Element element = traverser.get();
        final Object[] vertexPropertyKeyValues = this.internalParameters.getKeyValues(traverser, T.key, T.value);

        // Map-producing form: single-argument property(traversal) where the traversal produces a Map.
        // Only the dedicated map form expands a Map into per-entry properties; for the key+traversal form a
        // Map result is treated as an ordinary property value.
        if (this.mapForm) {
            if (results.size() == 1 && results.get(0) instanceof Map) {
                final Map<?, ?> map = (Map<?, ?>) results.get(0);
                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String)) {
                        throw new IllegalArgumentException(
                                "Property key must be a String but found " + entry.getKey().getClass().getSimpleName());
                    }
                    final String mapKey = (String) entry.getKey();
                    applyPropertyMutation(traverser, mapKey, entry.getValue(), vertexPropertyKeyValues);
                }
                return;
            }

            // The map form requires a Map result. Reject anything else rather than guessing.
            final Object result = results.get(0);
            throw new IllegalArgumentException(
                    "property(traversal) requires the traversal to produce a Map, but got: " +
                    (result == null ? "null" : result.getClass().getSimpleName()));
        }

        // Multi-result handling with cardinality awareness
        if (results.size() > 1) {
            // Determine effective cardinality
            final VertexProperty.Cardinality effectiveCard = this.cardinality != null
                    ? this.cardinality
                    : (element instanceof Vertex ? element.graph().features().vertex().getCardinality(key) : null);

            if (effectiveCard == VertexProperty.Cardinality.single) {
                throw new IllegalArgumentException(
                        "Single-cardinality property requires exactly one value, but traversal produced " +
                                results.size() + " results");
            }

            // For list/set cardinality with multiple results, add each as a separate property value
            if (effectiveCard == VertexProperty.Cardinality.list || effectiveCard == VertexProperty.Cardinality.set) {
                if (!(element instanceof Vertex)) {
                    throw new IllegalStateException(String.format(
                            "Property cardinality can only be set for a Vertex but the traversal encountered %s for key: %s",
                            element.getClass().getSimpleName(), key));
                }
                for (final Object v : results) {
                    applyPropertyMutation(traverser, key, v, vertexPropertyKeyValues);
                }
                return;
            }
        }

        // Single result or default cardinality: use the first result
        applyPropertyMutation(traverser, key, results.get(0), vertexPropertyKeyValues);
    }

    /**
     * Applies a single property mutation to the element, handling eventing and cardinality.
     */
    private void applyPropertyMutation(final Traverser.Admin<S> traverser, final String key,
                                       final Object value, final Object[] vertexPropertyKeyValues) {
        final Element element = traverser.get();

        // can't set cardinality if the element is something other than a vertex as only vertices can have
        // a cardinality of properties. if we don't throw an error here we end up with a confusing cast exception
        // which doesn't explain what went wrong
        if (this.cardinality != null && !(element instanceof Vertex))
            throw new IllegalStateException(String.format(
                    "Property cardinality can only be set for a Vertex but the traversal encountered %s for key: %s",
                    element.getClass().getSimpleName(), key));

        final Optional<EventStrategy> optEventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class);
        final boolean eventingIsConfigured = EventUtil.hasAnyCallbacks(callbackRegistry)
                && optEventStrategy.isPresent();
        final EventStrategy es = optEventStrategy.orElse(null);

        // find property to remove. only need to capture the removedProperty if eventing is configured
        final Property removedProperty = eventingIsConfigured ?
                captureRemovedProperty(element, key, value, es) :
                VertexProperty.empty();

        final VertexProperty.Cardinality card = this.cardinality != null
                ? this.cardinality
                : (element instanceof Vertex ? element.graph().features().vertex().getCardinality(key) : null);

        // update property
        if (element instanceof Vertex) {
            if (null != card) {
                ((Vertex) element).property(card, key, value, vertexPropertyKeyValues);
            } else if (vertexPropertyKeyValues.length > 0) {
                ((Vertex) element).property(key, value, vertexPropertyKeyValues);
            } else {
                ((Vertex) element).property(key, value);
            }
        } else if (element instanceof Edge) {
            element.property(key, value);
        } else if (element instanceof VertexProperty) {
            element.property(key, value);
        }

        // trigger event callbacks
        if (eventingIsConfigured) {
            EventUtil.registerPropertyChange(callbackRegistry, es, element, removedProperty, value, vertexPropertyKeyValues);
        }
    }

    private Property captureRemovedProperty(final Element element, final String key, final Object value,
                                            final EventStrategy es){
        Property removedProperty = VertexProperty.empty();

        if (element instanceof Vertex) {
            if (cardinality == VertexProperty.Cardinality.set) {
                final Iterator<? extends Property> properties = element.properties(key);
                while (properties.hasNext()) {
                    final Property property = properties.next();
                    if (Objects.equals(property.value(), value)) {
                        removedProperty = property;
                        break;
                    }
                }
            } else if (cardinality == VertexProperty.Cardinality.single) {
                removedProperty = element.property(key);
            }
        } else {
            removedProperty = element.property(key);
        }

        // detach removed property
        if (removedProperty.isPresent()) {
            removedProperty = es.detach(removedProperty);
        } else {
            removedProperty = element instanceof Vertex ? new KeyedVertexProperty(key) : new KeyedProperty(key);
        }

        return removedProperty;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode() ^ this.internalParameters.hashCode() ^ this.withConfiguration.hashCode() ^ Boolean.hashCode(this.mapForm);
        return (null != this.cardinality) ? (hash ^ cardinality.hashCode()) : hash;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.internalParameters.getTraversals().forEach(this::integrateChild);
        this.withConfiguration.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public Object getKey() {
        List<Object> keyParams = internalParameters.get(T.key, null);
        return keyParams.isEmpty() ? null : keyParams.get(0);
    }

    @Override
    public Object getValue() {
        List<Object> values = internalParameters.get(T.value, null);
        if (values.isEmpty()) {
            return null;
        }
        return values.get(0) instanceof ConstantTraversal ? ((ConstantTraversal<?, ?>) values.get(0)).next() : values.get(0);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.internalParameters);
    }

    @Override
    public AddPropertyStep<S> clone() {
        final AddPropertyStep<S> clone = (AddPropertyStep<S>) super.clone();
        clone.internalParameters = this.internalParameters.clone();
        clone.withConfiguration = this.withConfiguration.clone();
        return clone;
    }

    @Override
    public void addProperty(Object key, Object value) {
        internalParameters.set(this, key, value);
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        return internalParameters.getRaw(T.key, T.value);
    }

    @Override
    public boolean removeProperty(Object k) {
        if (internalParameters.contains(k)) {
            internalParameters.remove(k);
            return true;
        }
        return false;
    }
}
