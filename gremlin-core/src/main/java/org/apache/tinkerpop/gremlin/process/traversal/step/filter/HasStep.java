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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder<S, S>, Configuring, TraversalParent {

    private final Parameters parameters = new Parameters();
    private List<HasContainer> hasContainers;
    private List<Traversal.Admin<?, ?>> childTraversals = new ArrayList<>();

    public HasStep(final Traversal.Admin traversal, final HasContainer... hasContainers) {
        super(traversal);
        this.hasContainers = new ArrayList<>();
        for (final HasContainer hc : hasContainers) {
            this.hasContainers.add(hc);
            extractAndIntegrateTraversals(hc);
        }
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        // the generic S is defined as Element but Property can also be used with HasStep so this seems to cause
        // problems with some jdk versions.
        // Check Property BEFORE Element because VertexProperty implements both interfaces.
        // HasContainer.test(Property) correctly handles T.key and T.value accessors, whereas
        // HasContainer.test(Element) would fall through to element.properties("~key") which fails.
        if (traverser.get() instanceof Property) {
            return testAllWithTraversals(traverser, (Property) traverser.get());
        } else if (traverser.get() instanceof Element) {
            return testAllWithTraversals(traverser, (Element) traverser.get());
        } else {
            throw new IllegalStateException(String.format(
                    "Traverser to has() must be of type Property or Element, not %s",
                    traverser.get().getClass().getName()));
        }
    }

    /**
     * Tests all HasContainers against an Element, handling traversal-bearing containers by evaluating
     * child traversals against the current traverser.
     */
    private boolean testAllWithTraversals(final Traverser.Admin<S> traverser, final Element element) {
        for (final HasContainer hc : this.hasContainers) {
            if (!testHasContainer(traverser, hc, element)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Tests all HasContainers against a Property, handling traversal-bearing containers by evaluating
     * child traversals against the current traverser.
     */
    private boolean testAllWithTraversals(final Traverser.Admin<S> traverser, final Property property) {
        for (final HasContainer hc : this.hasContainers) {
            if (hc.hasTraversal()) {
                // For properties with traversals, we need to handle the traversal resolution
                if (hc.getTraversalValue() != null) {
                    final List<Object> results = collectTraversalResults(traverser, hc.getTraversalValue());
                    if (results.isEmpty()) return false;
                    // Get the property value to test against — takes first result, ignores extras
                    final Object propertyValue = getPropertyValueFromProperty(property, hc.getKey());
                    if (!P.eq(results.get(0)).test(propertyValue)) return false;
                } else if (hc.getPredicate() != null && hc.getPredicate().hasTraversal()) {
                    hc.getPredicate().resolve(traverser);
                    if (hc.getPredicate().isResolvedEmpty()) return false;
                    if (!hc.test(property)) return false;
                } else {
                    if (!hc.test(property)) return false;
                }
            } else {
                if (!hc.test(property)) return false;
            }
        }
        return true;
    }

    /**
     * Tests a single HasContainer against an Element, resolving traversals if present.
     */
    private boolean testHasContainer(final Traverser.Admin<S> traverser, final HasContainer hc, final Element element) {
        if (hc.hasTraversal()) {
            if (hc.getTraversalValue() != null) {
                // Direct traversal value: evaluate and use P.eq(first result) — takes first, ignores extras
                // Consistent with by(traversal) which also takes the first result silently.
                final List<Object> results = collectTraversalResults(traverser, hc.getTraversalValue());
                if (results.isEmpty()) return false;
                final Object propertyValue = getPropertyValue(element, hc.getKey());
                if (propertyValue == null) return false;
                return P.eq(results.get(0)).test(propertyValue);
            } else if (hc.getPredicate() != null && hc.getPredicate().hasTraversal()) {
                // Predicate with embedded traversal: resolve then test normally
                hc.getPredicate().resolve(traverser);
                if (hc.getPredicate().isResolvedEmpty()) return false;
                return hc.test(element);
            }
        }
        // No traversal: use existing test path
        return hc.test(element);
    }

    /**
     * Gets the property value from an Element for the given key, handling T.id and T.label accessors.
     */
    private Object getPropertyValue(final Element element, final String key) {
        if (key != null) {
            if (key.equals(T.id.getAccessor())) {
                return element.id();
            }
            if (key.equals(T.label.getAccessor())) {
                return element.label();
            }
        }
        final Iterator<? extends Property> itty = element.properties(key);
        try {
            if (itty.hasNext()) {
                return itty.next().value();
            }
        } finally {
            CloseableIterator.closeIterator(itty);
        }
        return null;
    }

    /**
     * Gets the property value from a Property for the given key, handling T.value and T.key accessors.
     */
    private Object getPropertyValueFromProperty(final Property property, final String key) {
        if (key != null) {
            if (key.equals(T.value.getAccessor())) {
                return property.value();
            }
            if (key.equals(T.key.getAccessor())) {
                return property.key();
            }
        }
        if (property instanceof Element) {
            return getPropertyValue((Element) property, key);
        }
        return null;
    }

    /**
     * Collects all results from a child traversal evaluated against the current traverser.
     * Uses the same prepare + iterate pattern as {@link TraversalUtil#applyAll(Traverser.Admin, Traversal.Admin)}
     * to avoid ambiguous overload resolution when S extends Element.
     */
    @SuppressWarnings("unchecked")
    private List<Object> collectTraversalResults(final Traverser.Admin<S> traverser,
                                                  final Traversal.Admin<?, ?> childTraversal) {
        final List<Object> results = new ArrayList<>();
        TraversalUtil.prepare(traverser, (Traversal.Admin) childTraversal);
        while (childTraversal.hasNext()) {
            results.add(childTraversal.next());
        }
        return results;
    }

    /**
     * Extracts child traversals from a HasContainer and integrates them as children of this step.
     * Handles direct traversal values on the container as well as traversals embedded within
     * predicates (including ConnectiveP which may have traversals on child predicates).
     */
    private void extractAndIntegrateTraversals(final HasContainer hc) {
        if (hc.getTraversalValue() != null) {
            this.childTraversals.add(hc.getTraversalValue());
            this.integrateChild(hc.getTraversalValue());
        }
        if (hc.getPredicate() != null && hc.getPredicate().hasTraversal()) {
            final List<Traversal.Admin<?, ?>> predicateTraversals = new ArrayList<>();
            P.collectTraversals(hc.getPredicate(), predicateTraversals);
            for (final Traversal.Admin<?, ?> t : predicateTraversals) {
                this.childTraversals.add(t);
                this.integrateChild(t);
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void removeHasContainer(final HasContainer hasContainer) {
        this.hasContainers.remove(hasContainer);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
        extractAndIntegrateTraversals(hasContainer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return (List) Collections.unmodifiableList(this.childTraversals);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public HasStep<S> clone() {
        final HasStep<S> clone = (HasStep<S>) super.clone();
        clone.hasContainers = new ArrayList<>();
        clone.childTraversals = new ArrayList<>();
        for (final HasContainer hasContainer : this.hasContainers) {
            final HasContainer clonedHc = hasContainer.clone();
            clone.hasContainers.add(clonedHc);
            if (clonedHc.getTraversalValue() != null) {
                clone.childTraversals.add(clonedHc.getTraversalValue());
            }
            if (clonedHc.getPredicate() != null && clonedHc.getPredicate().hasTraversal()) {
                P.collectTraversals(clonedHc.getPredicate(), clone.childTraversals);
            }
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        for (final Traversal.Admin<?, ?> childTraversal : this.childTraversals) {
            this.integrateChild(childTraversal);
        }
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final HasContainer hasContainer : this.hasContainers) {
            result ^= hasContainer.hashCode();
        }
        return result;
    }
}
