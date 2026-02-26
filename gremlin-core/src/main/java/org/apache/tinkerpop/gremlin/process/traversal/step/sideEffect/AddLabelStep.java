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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Side-effect step that adds labels to the current element by calling {@link Element#addLabel(String, String...)}.
 * Providers that support label mutation must override {@code addLabel()} in their Element implementations.
 *
 * @since 4.0.0
 */
public class AddLabelStep<S extends Element> extends SideEffectStep<S>
        implements Mutating<Event.ElementPropertyChangedEvent>, TraversalParent {

    private final String[] labels;
    private Traversal.Admin<S, String> labelTraversal;
    private CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry;

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
        this.labelTraversal = null;
    }

    public AddLabelStep(final Traversal.Admin traversal, final Traversal.Admin<S, String> labelTraversal) {
        super(traversal);
        this.labels = null;
        this.labelTraversal = this.integrateChild(labelTraversal);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Element element = traverser.get();

        if (this.labelTraversal != null) {
            final List<String> collectedLabels = new ArrayList<>();
            TraversalUtil.applyAll(traverser, this.labelTraversal)
                    .forEachRemaining(label -> {
                        ElementHelper.validateLabel(label);
                        collectedLabels.add(label);
                    });
            if (!collectedLabels.isEmpty()) {
                element.addLabel(collectedLabels.get(0),
                        collectedLabels.subList(1, collectedLabels.size()).toArray(new String[0]));
            }
        } else {
            element.addLabel(this.labels[0],
                    Arrays.copyOfRange(this.labels, 1, this.labels.length));
        }

        // trigger event callbacks
        final Optional<EventStrategy> optEventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class);
        if (EventUtil.hasAnyCallbacks(callbackRegistry) && optEventStrategy.isPresent()) {
            final EventStrategy es = optEventStrategy.get();
            EventUtil.registerPropertyChange(callbackRegistry, es, element, null, null, new Object[0]);
        }
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.labels != null ? Arrays.asList(this.labels) : this.labelTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.labels != null) result ^= Arrays.hashCode(this.labels);
        if (this.labelTraversal != null) result ^= this.labelTraversal.hashCode();
        return result;
    }

    @Override
    public List<Traversal.Admin<S, String>> getLocalChildren() {
        return this.labelTraversal != null ? Collections.singletonList(this.labelTraversal) : Collections.emptyList();
    }

    @Override
    public AddLabelStep<S> clone() {
        final AddLabelStep<S> clone = (AddLabelStep<S>) super.clone();
        if (this.labelTraversal != null)
            clone.labelTraversal = this.labelTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (this.labelTraversal != null)
            this.integrateChild(this.labelTraversal);
    }
}
