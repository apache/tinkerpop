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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddVertexStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class AddVertexStepPlaceholder<S> extends AbstractAddElementStepPlaceholder<S, Vertex, Event.VertexAddedEvent>
        implements AddVertexStepInterface<S>, GValueHolder<S, Vertex> {

    private boolean userProvidedLabel;

    public AddVertexStepPlaceholder(final Traversal.Admin traversal, final String label) {
        super(traversal, label == null ? Vertex.DEFAULT_LABEL : label);
        userProvidedLabel = label != null;
    }

    public AddVertexStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal, label == null ? GValue.of(Vertex.DEFAULT_LABEL) : label);
        userProvidedLabel = label != null;
    }

    public AddVertexStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> vertexLabelTraversal) {
        super(traversal, vertexLabelTraversal == null ?
                new ConstantTraversal<>(Vertex.DEFAULT_LABEL) : vertexLabelTraversal);
        userProvidedLabel = vertexLabelTraversal != null;
    }

    @Override
    public boolean hasUserProvidedLabel() {
        return userProvidedLabel;
    }

    @Override
    public AddVertexStepPlaceholder<S> clone() {
        final AddVertexStepPlaceholder<S> clone = (AddVertexStepPlaceholder<S>) super.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        return clone;
    }

    @Override
    protected boolean supportsMultiProperties() {
        return true;
    }

    @Override
    public AddVertexStep<S> asConcreteStep() {
        AddVertexStep<S> step = new AddVertexStep<>(traversal, label instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) label).getConstantTraversal() : label);
        super.configureConcreteStep(step);
        return step;
    }
}
