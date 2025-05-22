/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.FromToModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddEdgeStartStep extends AbstractStep<Edge, Edge>
        implements Writing<Event.EdgeAddedEvent>, TraversalParent, Scoping, FromToModulating {

    private static final String FROM = Graph.Hidden.hide("from");
    private static final String TO = Graph.Hidden.hide("to");

    private boolean first = true;
    private Parameters parameters = new Parameters();
    private CallbackRegistry<Event.EdgeAddedEvent> callbackRegistry;

    public AddEdgeStartStep(final Traversal.Admin traversal, final String edgeLabel) {
        super(traversal);
        this.parameters.set(this, T.label, edgeLabel);
    }

    public AddEdgeStartStep(final Traversal.Admin traversal, final Traversal<?, String> edgeLabelTraversal) {
        super(traversal);
        this.parameters.set(this, T.label, edgeLabelTraversal);
    }

    public AddEdgeStartStep(final Traversal.Admin traversal, final GValue<String> edgeLabel) {
        super(traversal);
        this.parameters.set(this, T.label, edgeLabel.get());
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
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
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    public void addTo(final Traversal.Admin<?, ?> toObject) {
        this.parameters.set(this, TO, toObject);
    }

    @Override
    public void addFrom(final Traversal.Admin<?, ?> fromObject) {
        this.parameters.set(this, FROM, fromObject);
    }

    @Override
    protected Traverser.Admin<Edge> processNextStart() {
        if (this.first) {
            this.first = false;
            final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();

            // a dead traverser to trigger the traversal
            final Traverser.Admin traverser = generator.generate(1, (Step) this, 1);

            final String edgeLabel = (String) this.parameters.get(traverser, T.label, () -> Edge.DEFAULT_LABEL).get(0);

            // FROM/TO must be set and must be vertices
            final Object theTo = this.parameters.get(traverser, TO, () -> null).get(0);
            if (!(theTo instanceof Vertex))
                throw new IllegalStateException(String.format(
                        "The value given to addE(%s).to() must resolve to a Vertex but %s was specified instead", edgeLabel,
                        null == theTo ? "null" : theTo.getClass().getSimpleName()));

            final Object theFrom = this.parameters.get(traverser, FROM, () -> null).get(0);
            if (!(theFrom instanceof Vertex))
                throw new IllegalStateException(String.format(
                        "The value given to addE(%s).from() must resolve to a Vertex but %s was specified instead", edgeLabel,
                        null == theFrom ? "null" : theFrom.getClass().getSimpleName()));

            Vertex toVertex = (Vertex) theTo;
            Vertex fromVertex = (Vertex) theFrom;

            if (toVertex instanceof Attachable)
                toVertex = ((Attachable<Vertex>) toVertex)
                        .attach(Attachable.Method.get(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));
            if (fromVertex instanceof Attachable)
                fromVertex = ((Attachable<Vertex>) fromVertex)
                        .attach(Attachable.Method.get(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));

            final Edge edge = fromVertex.addEdge(edgeLabel, toVertex, this.parameters.getKeyValues(traverser, TO, FROM, T.label));
            EventUtil.registerEdgeCreation(callbackRegistry, getTraversal(), edge);
            return generator.generate(edge, this, 1L);
        } else
            throw FastNoSuchElementException.instance();


    }

    @Override
    public CallbackRegistry<Event.EdgeAddedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parameters.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.parameters.toString());
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.parameters.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public AddEdgeStartStep clone() {
        final AddEdgeStartStep clone = (AddEdgeStartStep) super.clone();
        clone.parameters = this.parameters.clone();
        return clone;
    }

}
