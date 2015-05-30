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
package org.apache.tinkerpop.gremlin.neo4j.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public final class Neo4jGraphStep<S extends Element> extends GraphStep<S> {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public Neo4jGraphStep(final GraphStep<S> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        //No need to do anything if the first element is an Element, all elements are guaranteed to be an element and will be return as is
        if ((this.ids.length == 0 || !(this.ids[0] instanceof Element)))
            this.setIteratorSupplier(() -> (Iterator<S>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        return IteratorUtils.filter(this.getTraversal().getGraph().get().edges(this.ids), edge -> HasContainer.testAll((Edge) edge, this.hasContainers));
    }

    private Iterator<? extends Vertex> vertices() {
        final Neo4jGraph graph = (Neo4jGraph) this.getTraversal().getGraph().get();
        // ids are present, filter on them first
        if (this.ids != null && this.ids.length > 0)
            return IteratorUtils.filter(graph.vertices(this.ids), vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers));
        ////// do index lookups //////
        graph.tx().readWrite();
        // get a label being search on
        final Optional<String> label = this.hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> hasContainer.getPredicate().equals(Compare.eq))
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();
        if (label.isPresent()) {
            // find a vertex by label and key/value
            for (final HasContainer hasContainer : this.hasContainers) {
                if (hasContainer.getPredicate().equals(Compare.eq)) {
                    return IteratorUtils.filter(
                            IteratorUtils.map(
                                    graph.getBaseGraph().findNodes(label.get(), hasContainer.getKey(), hasContainer.getValue()).iterator(),
                                    node -> new Neo4jVertex(node, graph)),
                            vertex -> HasContainer.testAll(vertex, this.hasContainers));
                }
            }
        } else {
            // find a vertex by key/value
            for (final HasContainer hasContainer : this.hasContainers) {
                if (hasContainer.getPredicate().equals(Compare.eq)) {
                    return IteratorUtils.filter(
                            IteratorUtils.map(
                                    graph.getBaseGraph().findNodes(hasContainer.getKey(), hasContainer.getValue()).iterator(),
                                    node -> new Neo4jVertex(node, graph)),
                            vertex -> HasContainer.testAll(vertex, this.hasContainers));
                }
            }
        }
        if (label.isPresent()) {
            // find a vertex by label
            return IteratorUtils.filter(
                    IteratorUtils.map(
                            graph.getBaseGraph().findNodes(label.get()).iterator(),
                            node -> new Neo4jVertex(node, graph)),
                    vertex -> HasContainer.testAll(vertex, this.hasContainers));
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers));
        }
    }

    // TODO: move all this to the traits!

    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }
}
