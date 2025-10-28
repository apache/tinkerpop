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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Objects;

public abstract class AbstractAddVertexStepPlaceholder<S> extends AbstractAddElementStepPlaceholder<S, Vertex, Event.VertexAddedEvent>
        implements AddVertexStepContract<S>, GValueHolder<S, Vertex> {

    private boolean userProvidedLabel;

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final String label) {
        super(traversal, label);
        userProvidedLabel = label != null;
    }

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal, label);
        userProvidedLabel = label != null;
    }

    protected AbstractAddVertexStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> vertexLabelTraversal) {
        super(traversal, vertexLabelTraversal);
        userProvidedLabel = vertexLabelTraversal != null;
    }

    @Override
    protected String getDefaultLabel() {
        return Vertex.DEFAULT_LABEL;
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
