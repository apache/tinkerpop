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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Side-effect step that adds labels to the current element by calling {@link Element#addLabel(String, String...)}.
 * Providers that support label mutation must override {@code addLabel()} in their Element implementations.
 *
 * @since 4.0.0
 */
public class AddLabelStep<S extends Element> extends SideEffectStep<S>
        implements Mutating<Event.ElementLabelChangedEvent>, TraversalParent {

    private final String[] labels;
    private List<Traversal.Admin<S, ?>> labelTraversals;
    private CallbackRegistry<Event.ElementLabelChangedEvent> callbackRegistry;

    public AddLabelStep(final Traversal.Admin traversal, final String label, final String... moreLabels) {
        super(traversal);
        ElementHelper.validateLabel(label);
        for (final String l : moreLabels) {
            ElementHelper.validateLabel(l);
        }
        final List<String> allLabels = new ArrayList<>();
        allLabels.add(label);
        allLabels.addAll(Arrays.asList(moreLabels));
        this.labels = allLabels.toArray(new String[0]);
        this.labelTraversals = null;
    }

    /**
     * Constructor for one or more label traversals. If exactly one traversal is given and it produces a
     * {@link java.util.Collection}, the collection is unfolded into the set of labels added; otherwise each
     * traversal must resolve to a single {@link String} label.
     */
    public AddLabelStep(final Traversal.Admin traversal, final List<Traversal.Admin<S, ?>> labelTraversals) {
        super(traversal);
        this.labels = null;
        this.labelTraversals = new ArrayList<>(labelTraversals.size());
        for (final Traversal.Admin<S, ?> t : labelTraversals) {
            this.labelTraversals.add(this.integrateChild(t));
        }
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Element element = traverser.get();
        final Set<String> oldLabels = new LinkedHashSet<>(element.labels());

        if (this.labelTraversals != null) {
            final Set<String> collectedLabels = TraversalUtil.resolveStringArguments(traverser, this.labelTraversals);
            for (final String l : collectedLabels) {
                ElementHelper.validateLabel(l);
            }
            if (!collectedLabels.isEmpty()) {
                final List<String> asList = new ArrayList<>(collectedLabels);
                element.addLabel(asList.get(0), asList.subList(1, asList.size()).toArray(new String[0]));
            }
        } else {
            element.addLabel(this.labels[0],
                    Arrays.copyOfRange(this.labels, 1, this.labels.length));
        }

        // trigger event callbacks only if labels actually changed
        if (!oldLabels.equals(element.labels())) {
            EventUtil.registerLabelChange(callbackRegistry, getTraversal(), element, oldLabels, element.labels());
        }
    }

    @Override
    public CallbackRegistry<Event.ElementLabelChangedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.labels != null ? Arrays.asList(this.labels) : this.labelTraversals);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.labels != null) result ^= Arrays.hashCode(this.labels);
        if (this.labelTraversals != null) result ^= this.labelTraversals.hashCode();
        return result;
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return this.labelTraversals != null ? Collections.unmodifiableList(this.labelTraversals) : Collections.emptyList();
    }

    @Override
    public AddLabelStep<S> clone() {
        final AddLabelStep<S> clone = (AddLabelStep<S>) super.clone();
        if (this.labelTraversals != null) {
            clone.labelTraversals = new ArrayList<>(this.labelTraversals.size());
            for (final Traversal.Admin<S, ?> t : this.labelTraversals) {
                clone.labelTraversals.add(t.clone());
            }
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (this.labelTraversals != null) {
            this.labelTraversals.forEach(this::integrateChild);
        }
    }
}
