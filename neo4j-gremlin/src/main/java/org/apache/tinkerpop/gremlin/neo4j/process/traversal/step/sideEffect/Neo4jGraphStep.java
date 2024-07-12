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

import org.apache.tinkerpop.gremlin.neo4j.process.traversal.LabelP;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jStringSearchMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 * @deprecated See: https://tinkerpop.apache.org/docs/3.5.7/reference/#neo4j-gremlin
 */
@Deprecated
public final class Neo4jGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public Neo4jGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        if (null == this.ids)
            return Collections.emptyIterator();
        return IteratorUtils.filter(this.getTraversal().getGraph().get().edges(this.getIdsAsValues()), edge -> HasContainer.testAll(edge, this.hasContainers));
    }

    private Iterator<? extends Vertex> vertices() {
        if (null == this.ids)
            return Collections.emptyIterator();
        final Neo4jGraph graph = (Neo4jGraph) this.getTraversal().getGraph().get();

        // ids are present, filter on them first
        if (ids.length > 0)
            return IteratorUtils.filter(graph.vertices(this.getIdsAsValues()), vertex -> HasContainer.testAll(vertex, hasContainers));
        ////// do index lookups //////
        graph.tx().readWrite();
        // get a label being search on
        Optional<String> label = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey() != null && hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .filter(hasContainer -> hasContainer.getValue() != null)
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();
        if (!label.isPresent())
            label = hasContainers.stream()
                    .filter(hasContainer -> hasContainer.getKey() != null && hasContainer.getKey().equals(T.label.getAccessor()))
                    .filter(hasContainer -> hasContainer.getPredicate() instanceof LabelP)
                    .filter(hasContainer -> hasContainer.getValue() != null)
                    .map(hasContainer -> (String) hasContainer.getValue())
                    .findAny();

        if (label.isPresent()) {
            // find a vertex by label and key/value
            final String labelValue = label.get();
            final Neo4jGraphAPI baseGraph = graph.getBaseGraph();
            for (final HasContainer hasContainer : hasContainers) {
                final String key = hasContainer.getKey();
                final Object value = hasContainer.getValue();
                if (key != null && !key.equals(T.label.getAccessor()) && baseGraph.hasSchemaIndex(labelValue, key)) {
                    final BiPredicate<?, ?> predicate = hasContainer.getBiPredicate();
                    Iterable<Neo4jNode> nodes = null;
                    if (Compare.eq == predicate) {
                        nodes = baseGraph.findNodes(labelValue, key, value);
                    } else if (Text.containing == predicate) {
                        nodes = baseGraph.findNodes(labelValue, key, value.toString(), Neo4jStringSearchMode.CONTAINS);
                    } else if (Text.startingWith == predicate) {
                        nodes = baseGraph.findNodes(labelValue, key, value.toString(), Neo4jStringSearchMode.PREFIX);
                    } else if (Text.endingWith == predicate) {
                        nodes = baseGraph.findNodes(labelValue, key, value.toString(), Neo4jStringSearchMode.SUFFIX);
                    }
                    if (nodes != null) {
                        return IteratorUtils.stream(nodes)
                                .map(node -> (Vertex) new Neo4jVertex(node, graph))
                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
                    }
                }
            }

            // find a vertex by label
            return IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get()))
                    .map(node -> (Vertex) new Neo4jVertex(node, graph))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
