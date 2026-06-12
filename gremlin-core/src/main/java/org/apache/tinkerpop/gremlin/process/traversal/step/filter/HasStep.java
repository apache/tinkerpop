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
import org.apache.tinkerpop.gremlin.process.traversal.step.AcceptsChildPredicateTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder<S, S>, Configuring, TraversalParent, AcceptsChildPredicateTraversal {

    private final Parameters parameters = new Parameters();
    private List<HasContainer> hasContainers;
    private List<Traversal.Admin<?, ?>> childTraversals = new ArrayList<>();

    public HasStep(final Traversal.Admin traversal, final HasContainer... hasContainers) {
        super(traversal);
        this.hasContainers = new ArrayList<>();
        for (final HasContainer hc : hasContainers) {
            this.hasContainers.add(hc);
            collectChildTraversals(hc);
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
     * Tests all HasContainers against an Element. Traversal-bearing containers carry a {@code P} predicate
     * (e.g. {@code P.eq(traversal)}) whose child traversal is resolved against the current traverser before
     * the standard {@link HasContainer#test(Element)} is applied.
     */
    private boolean testAllWithTraversals(final Traverser.Admin<S> traverser, final Element element) {
        for (final HasContainer hc : this.hasContainers) {
            if (!resolvePredicate(hc, traverser) || !hc.test(element)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Tests all HasContainers against a Property. See {@link #testAllWithTraversals(Traverser.Admin, Element)}.
     */
    private boolean testAllWithTraversals(final Traverser.Admin<S> traverser, final Property property) {
        for (final HasContainer hc : this.hasContainers) {
            if (!resolvePredicate(hc, traverser) || !hc.test(property)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Resolves a container's predicate traversal (if any) against the current traverser, mutating the
     * predicate with the resolved value for this test cycle.
     *
     * @return {@code false} if the predicate resolved empty (no comparison value), meaning the element
     *         cannot match and the caller should short-circuit; {@code true} otherwise.
     */
    private boolean resolvePredicate(final HasContainer hc, final Traverser.Admin<S> traverser) {
        if (hc.hasTraversal()) {
            hc.getPredicate().resolve(traverser);
            return !hc.getPredicate().isResolvedEmpty();
        }
        return true;
    }

    /**
     * Collects the child traversals embedded within a HasContainer's predicate (including
     * {@link org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP}, which may carry traversals on
     * child predicates) into {@link #childTraversals}. Parenting is deferred to
     * {@link #setTraversal(Traversal.Admin)} which is the single integration point.
     */
    private void collectChildTraversals(final HasContainer hc) {
        if (hc.getPredicate().hasTraversal()) {
            P.collectTraversals(hc.getPredicate(), this.childTraversals);
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
        // A container may be added after this step has already been parented (e.g. by a strategy), so collect
        // its child traversals and integrate just those new ones immediately.
        final int before = this.childTraversals.size();
        collectChildTraversals(hasContainer);
        for (int i = before; i < this.childTraversals.size(); i++) {
            this.integrateChild(this.childTraversals.get(i));
        }
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
            if (clonedHc.getPredicate().hasTraversal()) {
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
