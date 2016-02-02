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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilter implements Cloneable, Serializable {

    private Traversal.Admin<Vertex, Vertex> vertexFilter = null;
    private Traversal.Admin<Vertex, Edge> edgeFilter = null;

    // private boolean dropAllEdges (make this a fast one if the end is limit(0)?)
    protected Direction allowedEdgeDirection = Direction.BOTH;
    protected Set<String> allowedEdgeLabels = new HashSet<>();
    protected boolean allowAllRemainingEdges = false;

    public void setVertexFilter(final Traversal<Vertex, Vertex> vertexFilter) {
        this.vertexFilter = vertexFilter.asAdmin().clone();
    }

    public void setEdgeFilter(final Traversal<Vertex, Edge> edgeFilter) {
        this.edgeFilter = edgeFilter.asAdmin().clone();
        if (this.edgeFilter.getStartStep() instanceof VertexStep) {
            this.allowedEdgeLabels.addAll(Arrays.asList(((VertexStep) this.edgeFilter.getStartStep()).getEdgeLabels()));
            this.allowedEdgeDirection = ((VertexStep) this.edgeFilter.getStartStep()).getDirection();
            this.allowAllRemainingEdges = 1 == this.edgeFilter.getSteps().size();
        }
    }

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

    //////////////////////////////////////
    /////////////////////////////////////
    ////////////////////////////////////

    public StarGraph.StarVertex applyGraphFilter(final StarGraph.StarVertex vertex) {
        if (!this.hasFilter())
            return vertex;
        else if (null == vertex)
            return null;
        else if (this.legalVertex(vertex)) {
            if (this.hasEdgeFilter()) {
                if (!this.allowedEdgeDirection.equals(Direction.BOTH))
                    vertex.dropEdges(this.allowedEdgeDirection.opposite());
                if (!this.allowedEdgeLabels.isEmpty())
                    vertex.keepEdges(this.allowedEdgeDirection, this.allowedEdgeLabels);

                if (!this.allowAllRemainingEdges) {
                    final Map<String, List<Edge>> outEdges = new HashMap<>();
                    final Map<String, List<Edge>> inEdges = new HashMap<>();
                    this.legalEdges(vertex).forEachRemaining(edge -> {
                        if (edge instanceof StarGraph.StarOutEdge) {
                            List<Edge> edges = outEdges.get(edge.label());
                            if (null == edges) {
                                edges = new ArrayList<>();
                                outEdges.put(edge.label(), edges);
                            }
                            edges.add(edge);
                        } else {
                            List<Edge> edges = inEdges.get(edge.label());
                            if (null == edges) {
                                edges = new ArrayList<>();
                                inEdges.put(edge.label(), edges);
                            }
                            edges.add(edge);
                        }
                    });

                    if (outEdges.isEmpty())
                        vertex.dropEdges(Direction.OUT);
                    else
                        vertex.setEdges(Direction.OUT, outEdges);

                    if (inEdges.isEmpty())
                        vertex.dropEdges(Direction.IN);
                    else
                        vertex.setEdges(Direction.IN, inEdges);
                }
            }
            return vertex;
        } else {
            return null;
        }
    }
}
