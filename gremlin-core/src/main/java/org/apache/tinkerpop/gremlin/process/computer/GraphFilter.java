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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

    public enum Legal {
        YES, NO, MAYBE;

        public boolean positive() {
            return this != NO;
        }

        public boolean negative() {
            return this == NO;
        }
    }

    private Traversal.Admin<Vertex, Vertex> vertexFilter = null;
    private Traversal.Admin<Vertex, Edge> edgeFilter = null;

    private boolean allowNoEdges = false;
    private Direction allowedEdgeDirection = Direction.BOTH;
    private Set<String> allowedEdgeLabels = new HashSet<>();
    //private boolean allowAllRemainingEdges = false;

    public void setVertexFilter(final Traversal<Vertex, Vertex> vertexFilter) {
        if (!TraversalHelper.isLocalVertex(vertexFilter.asAdmin()))
            throw GraphComputer.Exceptions.vertexFilterAccessesIncidentEdges(vertexFilter);
        this.vertexFilter = vertexFilter.asAdmin().clone();
    }

    public void setEdgeFilter(final Traversal<Vertex, Edge> edgeFilter) {
        if (!TraversalHelper.isLocalStarGraph(edgeFilter.asAdmin()))
            throw GraphComputer.Exceptions.edgeFilterAccessesAdjacentVertices(edgeFilter);
        this.edgeFilter = edgeFilter.asAdmin().clone();
        if (this.edgeFilter.getEndStep() instanceof RangeGlobalStep && 0 == ((RangeGlobalStep) this.edgeFilter.getEndStep()).getHighRange())
            this.allowNoEdges = true;
        else if (this.edgeFilter.getStartStep() instanceof VertexStep) {
            this.allowedEdgeLabels.clear();
            this.allowedEdgeLabels.addAll(Arrays.asList(((VertexStep) this.edgeFilter.getStartStep()).getEdgeLabels()));
            this.allowedEdgeDirection = ((VertexStep) this.edgeFilter.getStartStep()).getDirection();
            //this.allowAllRemainingEdges = 1 == this.edgeFilter.getSteps().size();
        }
    }

    /*public void compileFilters() {
        if (null != this.vertexFilter && !this.vertexFilter.isLocked())
            this.vertexFilter.applyStrategies();
        if (null != this.edgeFilter && !this.edgeFilter.isLocked())
            this.edgeFilter.applyStrategies();
    }*/

    public boolean legalVertex(final Vertex vertex) {
        return null == this.vertexFilter || TraversalUtil.test(vertex, this.vertexFilter);
    }

    public Iterator<Edge> legalEdges(final Vertex vertex) {
        return null == this.edgeFilter ?
                vertex.edges(Direction.BOTH) :
                TraversalUtil.applyAll(vertex, this.edgeFilter);
    }

    public final Traversal.Admin<Vertex, Vertex> getVertexFilter() {
        return this.vertexFilter;
    }

    public final Traversal.Admin<Vertex, Edge> getEdgeFilter() {
        return this.edgeFilter;
    }

    public boolean hasFilter() {
        return this.vertexFilter != null || this.edgeFilter != null;
    }

    public boolean hasEdgeFilter() {
        return this.edgeFilter != null;
    }

    public boolean hasVertexFilter() {
        return this.vertexFilter != null;
    }

    public Legal checkEdgeLegality(final Direction direction, final String label) {
        if (null == this.edgeFilter)
            return Legal.YES;
        else if (this.allowNoEdges)
            return Legal.NO;
        else if (!this.allowedEdgeDirection.equals(Direction.BOTH) && !this.allowedEdgeDirection.equals(direction))
            return Legal.NO;
        else if (!this.allowedEdgeLabels.isEmpty() && !this.allowedEdgeLabels.contains(label))
            return Legal.NO;
        else
            return Legal.MAYBE;
    }

    public Legal checkEdgeLegality(final Direction direction) {
        if (null == this.edgeFilter)
            return Legal.YES;
        else if (this.allowNoEdges)
            return Legal.NO;
        else if (!this.allowedEdgeDirection.equals(Direction.BOTH) && !this.allowedEdgeDirection.equals(direction))
            return Legal.NO;
        else
            return Legal.MAYBE;
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