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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddVertexStep<S> extends ScalarMapStep<S, Vertex>
        implements Writing<Event.VertexAddedEvent>, TraversalParent, Scoping {

    private Parameters parameters = new Parameters();
    private CallbackRegistry<Event.VertexAddedEvent> callbackRegistry;
    private boolean userProvidedLabel;

    public AddVertexStep(final Traversal.Admin traversal, final String label) {
        super(traversal);
        this.parameters.set(this, T.label, null == label ? Vertex.DEFAULT_LABEL : label);
        userProvidedLabel = label != null;
    }
    public AddVertexStep(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal);
        this.parameters.set(this, T.label, null == label ? Vertex.DEFAULT_LABEL : label);
    }

    public AddVertexStep(final Traversal.Admin traversal, final Traversal.Admin<S,String> vertexLabelTraversal) {
        super(traversal);
        this.parameters.set(this, T.label, null == vertexLabelTraversal ? Vertex.DEFAULT_LABEL : vertexLabelTraversal);
        userProvidedLabel = vertexLabelTraversal != null;
    }

    public boolean hasUserProvidedLabel() {
        return userProvidedLabel;
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.parameters.getReferencedLabels();
    }

    @Override
    public HashSet<PopInstruction> getPopInstructions() {
        final HashSet<PopInstruction> popInstructions = new HashSet<>();
        popInstructions.addAll(Scoping.super.getPopInstructions());
        popInstructions.addAll(TraversalParent.super.getPopInstructions());
        return popInstructions;
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues[0] == T.label) userProvidedLabel = true;

        if (keyValues[0] == T.label && this.parameters.contains(T.label)) {
            if (this.parameters.contains(T.label, Vertex.DEFAULT_LABEL)) {
                this.parameters.remove(T.label);
                this.parameters.set(this, keyValues);
            } else {
                throw new IllegalArgumentException(String.format("Vertex T.label has already been set to [%s] and cannot be overridden with [%s]",
                        this.parameters.getRaw().get(T.label).get(0), keyValues[1]));
            }
        } else if (keyValues[0] == T.id && this.parameters.contains(T.id)) {
            throw new IllegalArgumentException(String.format("Vertex T.id has already been set to [%s] and cannot be overridden with [%s]",
                    this.parameters.getRaw().get(T.id).get(0), keyValues[1]));
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    protected Vertex map(final Traverser.Admin<S> traverser) {
        final Vertex vertex = this.getTraversal().getGraph().get().addVertex(this.parameters.getKeyValues(traverser));
        EventUtil.registerVertexCreation(callbackRegistry, getTraversal(), vertex);
        return vertex;
    }

    @Override
    public CallbackRegistry<Event.VertexAddedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parameters.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.parameters);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.parameters.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public AddVertexStep<S> clone() {
        final AddVertexStep<S> clone = (AddVertexStep<S>) super.clone();
        clone.parameters = this.parameters.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        return clone;
    }
}
