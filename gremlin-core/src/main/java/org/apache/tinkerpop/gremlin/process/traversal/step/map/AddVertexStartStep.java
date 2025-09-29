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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddVertexStartStep extends AbstractStep<Vertex, Vertex> implements AddVertexStepContract<Vertex> {

    private Parameters internalParameters = new Parameters();
    private Parameters withConfiguration = new Parameters();
    private boolean first = true;
    private CallbackRegistry<Event.VertexAddedEvent> callbackRegistry;
    private boolean userProvidedLabel;

    public AddVertexStartStep(final Traversal.Admin traversal, final String label) {
        super(traversal);
        this.internalParameters.set(this, T.label, null == label ? Vertex.DEFAULT_LABEL : label);
        userProvidedLabel = label != null;
    }

    public AddVertexStartStep(final Traversal.Admin traversal, final Traversal<?, String> vertexLabelTraversal) {
        super(traversal);
        this.internalParameters.set(this, T.label, null == vertexLabelTraversal ? Vertex.DEFAULT_LABEL : vertexLabelTraversal);
        userProvidedLabel = vertexLabelTraversal != null;
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
        return this.internalParameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }

    private void configureInternalParams(final Object... keyValues) {
        if (keyValues[0] == T.label && this.internalParameters.contains(T.label)) {
            if (this.internalParameters.contains(T.label, Vertex.DEFAULT_LABEL)) {
                this.internalParameters.remove(T.label);
                this.internalParameters.set(this, keyValues);
            } else {
                throw new IllegalArgumentException(String.format("Vertex T.label has already been set to [%s] and cannot be overridden with [%s]",
                        this.internalParameters.getRaw().get(T.label).get(0), keyValues[1]));
            }
        } else if (keyValues[0] == T.id && this.internalParameters.contains(T.id)) {
            throw new IllegalArgumentException(String.format("Vertex T.id has already been set to [%s] and cannot be overridden with [%s]",
                    this.internalParameters.getRaw().get(T.id).get(0), keyValues[1]));
        } else {
            this.internalParameters.set(this, keyValues);
        }
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        if (this.first) {
            this.first = false;
            final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();
            final Vertex vertex = this.getTraversal().getGraph().get().addVertex(this.internalParameters.getKeyValues(generator.generate(false, (Step) this, 1L)));
            EventUtil.registerVertexCreation(callbackRegistry, getTraversal(), vertex);
            return generator.generate(vertex, this, 1L);
        } else
            throw FastNoSuchElementException.instance();
    }

    @Override
    public CallbackRegistry<Event.VertexAddedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.internalParameters.hashCode() ^ this.withConfiguration.hashCode();
    }

    @Override
    public void reset() {
        super.reset();
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
    }

    @Override
    public AddVertexStartStep clone() {
        final AddVertexStartStep clone = (AddVertexStartStep) super.clone();
        clone.internalParameters = this.internalParameters.clone();
        clone.withConfiguration = this.withConfiguration.clone();
        clone.userProvidedLabel = this.userProvidedLabel;
        return clone;
    }

    @Override
    public Object getLabel() {
        Object label = internalParameters.get(T.label, () -> Vertex.DEFAULT_LABEL).get(0);
        if (label instanceof ConstantTraversal) {
            return ((ConstantTraversal<?, ?>) label).next();
        }
        return label;
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        return Collections.unmodifiableMap(internalParameters.getRaw());
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
