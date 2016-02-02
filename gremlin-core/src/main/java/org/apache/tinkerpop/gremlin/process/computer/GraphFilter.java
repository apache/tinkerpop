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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilter implements Cloneable, Serializable {

    private Traversal.Admin<Vertex, Vertex> vertexFilter = null;
    private Traversal.Admin<Edge, Edge> edgeFilter = null;

    public void setVertexFilter(final Traversal<Vertex, Vertex> vertexFilter) {
        this.vertexFilter = vertexFilter.asAdmin().clone();
    }

    public void setEdgeFilter(final Traversal<Edge, Edge> edgeFilter) {
        this.edgeFilter = edgeFilter.asAdmin().clone();
    }

    public boolean hasVertexFilter() {
        return this.vertexFilter != null;
    }

    public final Traversal.Admin<Vertex, Vertex> getVertexFilter() {
        return this.vertexFilter;
    }

    public final Traversal.Admin<Edge, Edge> getEdgeFilter() {
        return this.edgeFilter;
    }

    public boolean hasEdgeFilter() {
        return this.edgeFilter != null;
    }

    public boolean hasFilter() {
        return this.vertexFilter != null || this.edgeFilter != null;
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
