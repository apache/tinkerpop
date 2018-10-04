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

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * GraphFilter is used by {@link GraphComputer} implementations to prune the source graph data being loaded into the OLAP system.
 * There are two types of filters: a {@link Vertex} filter and an {@link Edge} filter.
 * The vertex filter is a {@link Traversal} that can only check the id, label, and properties of the vertex.
 * The edge filter is a {@link Traversal} that starts at the vertex are emits all legal incident edges.
 * If no vertex filter is provided, then no vertices are filtered. If no edge filter is provided, then no edges are filtered.
 * The use of a GraphFilter can greatly reduce the amount of data processed by the {@link GraphComputer}.
 * For instance, for {@code g.V().count()}, there is no reason to load edges, and thus, the edge filter can be {@code bothE().limit(0)}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilter implements Cloneable, Serializable {

    /**
     * A enum denoting whether a particular result will be allowed or not.
     * {@link Legal#YES} means that the specified element set will definitely not be removed by {@link GraphFilter}.
     * {@link Legal#MAYBE} means that the element set may or may not be filtered out by the {@link GraphFilter}.
     * {@link Legal#NO} means that the specified element set will definitely be removed by {@link GraphFilter}.
     */
    public enum Legal {
        YES, MAYBE, NO;

        /**
         * The enum is either {@link Legal#YES} or {@link Legal#MAYBE}.
         *
         * @return true if potentially legal.
         */
        public boolean positive() {
            return this != NO;
        }

        /**
         * The enum is {@link Legal#NO}.
         *
         * @return true if definitely not legal.
         */
        public boolean negative() {
            return this == NO;
        }
    }

    private Traversal.Admin<Vertex, Vertex> vertexFilter = null;
    private Traversal.Admin<Vertex, Edge> edgeFilter = null;
    private Map<Direction, Map<String, Legal>> edgeLegality = new EnumMap<>(Direction.class);
    private boolean allowNoEdges = false;

    public GraphFilter() {
        // no args constructor
    }

    public GraphFilter(final Computer computer) {
        if (null != computer.getVertices())
            this.setVertexFilter(computer.getVertices());
        if (null != computer.getEdges())
            this.setEdgeFilter(computer.getEdges());
    }

    /**
     * Set the filter for selecting vertices from the source graph.
     * The vertex filter can only access the vertex, its properties, and its properties properties.
     * Vertex filters can not access the incident edges of the vertex.
     *
     * @param vertexFilter The {@link Traversal} that will either let the vertex pass or not.
     */
    public void setVertexFilter(final Traversal<Vertex, Vertex> vertexFilter) {
        if (!TraversalHelper.isLocalProperties(vertexFilter.asAdmin()))
            throw GraphComputer.Exceptions.vertexFilterAccessesIncidentEdges(vertexFilter);
        this.vertexFilter = vertexFilter.asAdmin().clone();
    }

    /**
     * Set the filter for selecting edges from the source graph.
     * The edge filter can only access the local star graph (not adjacent vertices).
     *
     * @param edgeFilter The {@link Traversal} that will generate the legal incident edges of the vertex.
     */
    public void setEdgeFilter(final Traversal<Vertex, Edge> edgeFilter) {
        if (!TraversalHelper.isLocalStarGraph(edgeFilter.asAdmin()))
            throw GraphComputer.Exceptions.edgeFilterAccessesAdjacentVertices(edgeFilter);
        this.edgeFilter = edgeFilter.asAdmin().clone();
        ////
        this.edgeLegality = new EnumMap<>(Direction.class);
        this.edgeLegality.put(Direction.OUT, new HashMap<>());
        this.edgeLegality.put(Direction.IN, new HashMap<>());
        this.edgeLegality.put(Direction.BOTH, new HashMap<>());
        if (this.edgeFilter.getEndStep() instanceof RangeGlobalStep && 0 == ((RangeGlobalStep) this.edgeFilter.getEndStep()).getHighRange())
            this.allowNoEdges = true;
        ////
        if (this.edgeFilter.getStartStep() instanceof VertexStep) {
            final VertexStep step = (VertexStep) this.edgeFilter.getStartStep();
            final Map<String, Legal> map = this.edgeLegality.get(step.getDirection());
            if (step.returnsEdge()) {
                if (step.getEdgeLabels().length == 0)
                    map.put(null, 1 == this.edgeFilter.getSteps().size() ? Legal.YES : Legal.MAYBE);
                else {
                    for (final String label : step.getEdgeLabels()) {
                        map.put(label, 1 == this.edgeFilter.getSteps().size() ? Legal.YES : Legal.MAYBE);
                    }
                }
            }
        } else if (this.edgeFilter.getStartStep() instanceof UnionStep) {
            final UnionStep<?, ?> step = (UnionStep) this.edgeFilter.getStartStep();
            for (final Traversal.Admin<?, ?> union : step.getGlobalChildren()) {
                if (union.getStartStep() instanceof VertexStep) {
                    final VertexStep vertexStep = (VertexStep) union.getStartStep();
                    final Map<String, Legal> map = this.edgeLegality.get(vertexStep.getDirection());
                    if (vertexStep.returnsEdge()) {
                        if (vertexStep.getEdgeLabels().length == 0)
                            map.put(null, 2 == union.getSteps().size() ? Legal.YES : Legal.MAYBE);
                        else {
                            for (final String label : vertexStep.getEdgeLabels()) {
                                map.put(label, 2 == union.getSteps().size() ? Legal.YES : Legal.MAYBE);
                            }
                        }

                    }
                }
            }
        }
        final Map<String, Legal> outMap = this.edgeLegality.get(Direction.OUT);
        final Map<String, Legal> inMap = this.edgeLegality.get(Direction.IN);
        final Map<String, Legal> bothMap = this.edgeLegality.get(Direction.BOTH);
        for (final Map.Entry<String, Legal> entry : bothMap.entrySet()) {
            final Legal legal = inMap.get(entry.getKey());
            if (null == legal || legal.compareTo(entry.getValue()) > 0)
                inMap.put(entry.getKey(), entry.getValue());
        }
        for (final Map.Entry<String, Legal> entry : bothMap.entrySet()) {
            final Legal legal = outMap.get(entry.getKey());
            if (null == legal || legal.compareTo(entry.getValue()) > 0)
                outMap.put(entry.getKey(), entry.getValue());
        }
        for (final Map.Entry<String, Legal> entry : outMap.entrySet()) {
            final Legal legal = inMap.get(entry.getKey());
            if (null != legal)
                bothMap.put(entry.getKey(), legal.compareTo(entry.getValue()) > 0 ? legal : entry.getValue());
        }
        if (outMap.isEmpty() && inMap.isEmpty() && bothMap.isEmpty()) { // the edge filter could not be reasoned on
            outMap.put(null, Legal.MAYBE);
            inMap.put(null, Legal.MAYBE);
            bothMap.put(null, Legal.MAYBE);
        }
    }

    /**
     * Returns true if the provided vertex meets the vertex-filter criteria.
     * If no vertex filter is provided, then the vertex is considered legal.
     *
     * @param vertex the vertex to test for legality
     * @return whether the vertex is {@link Legal#YES}.
     */
    public boolean legalVertex(final Vertex vertex) {
        return null == this.vertexFilter || TraversalUtil.test(vertex, this.vertexFilter);
    }

    /**
     * Returns an iterator of legal edges incident to the provided vertex.
     * If no edge filter is provided, then all incident edges are returned.
     *
     * @param vertex the vertex whose legal edges are to be access.
     * @return an iterator of edges that are {@link Legal#YES}.
     */
    public Iterator<Edge> legalEdges(final Vertex vertex) {
        return null == this.edgeFilter ?
                vertex.edges(Direction.BOTH) :
                TraversalUtil.applyAll(vertex, this.edgeFilter);
    }

    /**
     * Get the vertex filter associated with this graph filter.
     *
     * @return the vertex filter or null if no vertex filter was provided.
     */
    public final Traversal.Admin<Vertex, Vertex> getVertexFilter() {
        return this.vertexFilter;
    }

    /**
     * Get the edge filter associated with this graph filter.
     *
     * @return the edge filter or null if no edge filter was provided.
     */
    public final Traversal.Admin<Vertex, Edge> getEdgeFilter() {
        return this.edgeFilter;
    }

    /**
     * Whether filters have been defined.
     *
     * @return true if either a vertex or edge filter has been provided.
     */
    public boolean hasFilter() {
        return this.vertexFilter != null || this.edgeFilter != null;
    }

    /**
     * Whether an edge filter has been defined.
     *
     * @return true if an edge filter was provided.
     */
    public boolean hasEdgeFilter() {
        return this.edgeFilter != null;
    }

    /**
     * Whether a vertex filter has been defined.
     *
     * @return true if a vertex filter was provided.
     */
    public boolean hasVertexFilter() {
        return this.vertexFilter != null;
    }

    /**
     * For a particular edge directionality, get all the {@link Legal#YES} or {@link Legal#MAYBE} edge labels.
     * If the label set contains {@code null}, then all edge labels for that direction are positively legal.
     *
     * @param direction the direction to get the positively legal edge labels for.
     * @return the set of positively legal edge labels for the direction.
     */
    public Set<String> getLegallyPositiveEdgeLabels(final Direction direction) {
        if (null == this.edgeFilter)
            return Collections.singleton(null);
        else if (this.allowNoEdges)
            return Collections.emptySet();
        else
            return this.edgeLegality.get(direction).containsKey(null) ?
                    Collections.singleton(null) :
                    this.edgeLegality.get(direction).entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().positive())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
    }

    /**
     * Get the legality of a particular edge direction and label.
     *
     * @param direction the direction of the edge.
     * @param label     the label of the edge.
     * @return the {@link Legal} of the arguments.
     */
    public Legal checkEdgeLegality(final Direction direction, final String label) {
        if (null == this.edgeFilter)
            return Legal.YES;
        if (this.checkEdgeLegality(direction).negative())
            return Legal.NO;
        final Map<String, Legal> legalMap = this.edgeLegality.get(direction);
        if (legalMap.containsKey(label))
            return legalMap.get(label);
        else if (legalMap.containsKey(null))
            return legalMap.get(null);
        else
            return Legal.NO;
    }

    /**
     * Get the legality of a particular edge direction.
     *
     * @param direction the direction of the edge.
     * @return the {@link Legal} of the edge direction.
     */
    public Legal checkEdgeLegality(final Direction direction) {
        if (null == this.edgeFilter)
            return Legal.YES;
        else if (this.allowNoEdges)
            return Legal.NO;
        return this.edgeLegality.get(direction).values()
                .stream()
                .reduce(Legal.NO, (a, b) -> a.compareTo(b) < 0 ? a : b);
    }

    @Override
    public int hashCode() {
        return (null == this.edgeFilter ? 111 : this.edgeFilter.hashCode()) ^ (null == this.vertexFilter ? 222 : this.vertexFilter.hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof GraphFilter))
            return false;
        else if (((GraphFilter) object).hasVertexFilter() && !((GraphFilter) object).getVertexFilter().equals(this.vertexFilter))
            return false;
        else if (((GraphFilter) object).hasEdgeFilter() && !((GraphFilter) object).getEdgeFilter().equals(this.edgeFilter))
            return false;
        else
            return true;
    }

    @Override
    public GraphFilter clone() {
        try {
            final GraphFilter clone = (GraphFilter) super.clone();
            if (null != this.vertexFilter)
                clone.vertexFilter = this.vertexFilter.clone();
            if (null != this.edgeFilter)
                clone.edgeFilter = this.edgeFilter.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        if (!this.hasFilter())
            return "graphfilter[none]";
        else if (this.hasVertexFilter() && this.hasEdgeFilter())
            return "graphfilter[" + this.vertexFilter + "," + this.edgeFilter + "]";
        else if (this.hasVertexFilter())
            return "graphfilter[" + this.vertexFilter + "]";
        else
            return "graphfilter[" + this.edgeFilter + "]";
    }
}