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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AddEdgeStep<S> extends ScalarMapStep<S, Edge> implements AddEdgeStepContract<S> {

    private static final String FROM = Graph.Hidden.hide("from");
    private static final String TO = Graph.Hidden.hide("to");

    private Parameters internalParameters = new Parameters();
    private Parameters withConfiguration = new Parameters();
    private CallbackRegistry<Event.EdgeAddedEvent> callbackRegistry;

    public AddEdgeStep(final Traversal.Admin traversal, final String edgeLabel) {
        super(traversal);
        this.internalParameters.set(this, T.label, edgeLabel);
    }

    public AddEdgeStep(final Traversal.Admin traversal, final Traversal.Admin<S,String> edgeLabelTraversal) {
        super(traversal);
        this.internalParameters.set(this, T.label, edgeLabelTraversal);
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.internalParameters.getTraversals();
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
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }

    private void configureInternalParams(final Object... keyValues) {
        this.internalParameters.set(this, keyValues);
    }

    @Override
    public void addTo(final Traversal.Admin<?, ?> toObject) {
        this.internalParameters.set(this, TO, toObject);
    }

    @Override
    public void addFrom(final Traversal.Admin<?, ?> fromObject) {
        this.internalParameters.set(this, FROM, fromObject);
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
    protected Edge map(final Traverser.Admin<S> traverser) {
        final String edgeLabel = this.internalParameters.get(traverser, T.label, () -> Edge.DEFAULT_LABEL).get(0);

        Vertex toVertex = getAdjacentVertex(this.internalParameters, TO, traverser, edgeLabel);
        Vertex fromVertex = getAdjacentVertex(this.internalParameters, FROM, traverser, edgeLabel);

        try {
            if (toVertex instanceof Attachable)
                toVertex = ((Attachable<Vertex>) toVertex)
                        .attach(Attachable.Method.get(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));
        }
        catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "The value given to addE(%s).to() must resolve to a Vertex or the ID of a Vertex present in the graph. The provided value does not match any vertices in the graph", edgeLabel));
        }

        try {
            if (fromVertex instanceof Attachable)
                fromVertex = ((Attachable<Vertex>) fromVertex)
                        .attach(Attachable.Method.get(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));
        }
        catch (Exception e) {
            throw new IllegalStateException(String.format(
                    "The value given to addE(%s).from() must resolve to a Vertex or the ID of a Vertex present in the graph. The provided value does not match any vertices in the graph", edgeLabel));
        }

        final Edge edge = fromVertex.addEdge(edgeLabel, toVertex, this.internalParameters.getKeyValues(traverser, TO, FROM, T.label));
        EventUtil.registerEdgeCreation(callbackRegistry, getTraversal(), edge);
        return edge;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public CallbackRegistry<Event.EdgeAddedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.internalParameters.hashCode() ^ this.withConfiguration.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.internalParameters.toString());
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.internalParameters.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public AddEdgeStep<S> clone() {
        final AddEdgeStep<S> clone = (AddEdgeStep<S>) super.clone();
        clone.internalParameters = this.internalParameters.clone();
        clone.withConfiguration = this.withConfiguration.clone();
        return clone;
    }

    @Override
    public Object getLabel() {
        Object label = internalParameters.get(T.label, () -> Edge.DEFAULT_LABEL).get(0);
        if (label instanceof ConstantTraversal) {
            return ((ConstantTraversal<?, ?>) label).next();
        }
        return label;
    }

    @Override
    public void setLabel(Object label) {
        this.configure(T.label, label);
    }

    @Override
    public void addProperty(Object key, Object value) {
        configureInternalParams(key, value);
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        return Collections.unmodifiableMap(internalParameters.getRaw(T.id, T.label, TO, FROM));
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
    public Object getFrom() {
        return getAdjacentVertex(this.internalParameters, FROM);
    }

    @Override
    public Object getTo() {
        return getAdjacentVertex(this.internalParameters, TO);
    }
}
