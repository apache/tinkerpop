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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Objects;

public abstract class AbstractAddVertexStepPlaceholder<S> extends AbstractAddElementStepPlaceholder<S, Vertex, Event.VertexAddedEvent>
        implements AddVertexStepContract<S>, GValueHolder<S, Vertex> {

    private boolean userProvidedLabel;

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final String label) {
        super(traversal, label);
    }

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal, label);
    }

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,?> vertexLabelTraversal) {
        super(traversal, vertexLabelTraversal);
    }

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final Collection<Object> labels) {
        super(traversal, labels);
    }

    @Override
    protected String getDefaultLabel() {
        return Vertex.DEFAULT_LABEL;
    }

    @Override
    public void setLabel(Object label) {
        // An explicit empty label collection means "no label provided": leave the label unset so vertex
        // creation applies the graph's LabelCardinality default (no labels under ZERO_OR_MORE), matching the
        // concrete step and graph.addVertex(). Flipping userProvidedLabel here would instead materialize the
        // default label.
        if (label instanceof Collection && ((Collection<?>) label).isEmpty()) {
            return;
        }
        super.setLabel(label);
        userProvidedLabel = true;
    }

    @Override
    public void addProperty(final Object key, final Object value) {
        // T.labels is a synonym for T.label on vertex creation (multi-label support)
        if (key == T.labels) {
            setLabel(value);
            return;
        }
        super.addProperty(key, value);
    }

    @Override
    public boolean hasUserProvidedLabel() {
        return userProvidedLabel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AbstractAddVertexStepPlaceholder<?> that = (AbstractAddVertexStepPlaceholder<?>) o;
        return userProvidedLabel == that.userProvidedLabel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userProvidedLabel);
    }

    @Override
    public AbstractAddVertexStepPlaceholder<S> clone() {
        final AbstractAddVertexStepPlaceholder<S> clone = (AbstractAddVertexStepPlaceholder<S>) super.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        return clone;
    }

    @Override
    protected boolean supportsMultiProperties() {
        return true;
    }

}
