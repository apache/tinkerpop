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
import org.apache.tinkerpop.gremlin.neo4j.structure.trait.MultiMetaNeo4jTrait;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.Neo4jNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
        // a label and a property
        final Pair<String, HasContainer> labelHasPair = this.getHasContainerForLabelIndex();
        if (null != labelHasPair)
            return this.getVerticesUsingLabelAndProperty(labelHasPair.getValue0(), labelHasPair.getValue1())
                    .filter(vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers)).iterator();
        // only labels
        final List<String> labels = this.getInternalLabels();
        if (null != labels)
            return this.getVerticesUsingOnlyLabels(labels).filter(vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers)).iterator();
        // linear scan
        return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers));
    }


    private Stream<Neo4jVertex> getVerticesUsingLabelAndProperty(final String label, final HasContainer hasContainer) {
        final Neo4jGraph graph = (Neo4jGraph) this.getTraversal().getGraph().get();
        final Iterable<Neo4jNode> iterator1 = graph.getBaseGraph().findNodes(label, hasContainer.getKey(), hasContainer.getValue());
        final Iterable<Neo4jNode> iterator2 = graph.getBaseGraph().findNodes(hasContainer.getKey(), T.value.getAccessor(), hasContainer.getValue());
        final Stream<Neo4jVertex> stream1 = IteratorUtils.stream(iterator1)
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .map(node -> new Neo4jVertex(node, graph));
        final Stream<Neo4jVertex> stream2 = IteratorUtils.stream(iterator2)
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .filter(node -> node.getProperty(T.key.getAccessor()).equals(hasContainer.getKey()))
                .map(node -> node.relationships(Neo4jDirection.INCOMING).iterator().next().start())
                .map(node -> new Neo4jVertex(node, graph));
        return Stream.concat(stream1, stream2);
    }

    private Stream<Neo4jVertex> getVerticesUsingOnlyLabels(final List<String> labels) {
        final Neo4jGraph graph = (Neo4jGraph) this.getTraversal().getGraph().get();
        final Predicate<Neo4jNode> nodePredicate = graph.getTrait().getNodePredicate();
        return labels.stream()
                .flatMap(label -> IteratorUtils.stream(graph.getBaseGraph().findNodes(label)))
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .filter(nodePredicate)
                .map(node -> new Neo4jVertex(node, graph));
    }

    private Pair<String, HasContainer> getHasContainerForLabelIndex() {
        final Neo4jGraph graph = (Neo4jGraph) this.getTraversal().getGraph().get();
        Neo4jGraphAPI baseGraph = graph.getBaseGraph();
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()) && hasContainer.getBiPredicate().equals(Compare.eq)) {
                if (baseGraph.hasSchemaIndex(
                        (String) hasContainer.getValue(), hasContainer.getKey())) {
                    return Pair.with((String) hasContainer.getValue(), hasContainer);
                }
            }
        }
        return null;
    }

    private List<String> getInternalLabels() {
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()) && hasContainer.getBiPredicate().equals(Compare.eq))
                return Arrays.asList(((String) hasContainer.getValue()));
            else if (hasContainer.getKey().equals(T.label.getAccessor()) && hasContainer.getBiPredicate().equals(Contains.within))
                return new ArrayList<>((Collection<String>) hasContainer.getValue());
        }
        return null;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.hasContainers) :
                    StringFactory.stepString(this, Arrays.toString(this.ids), this.hasContainers);
    }
}
