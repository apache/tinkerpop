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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * An input graph class is {@code GraphFilterAware} if it can filter out vertices and edges as its loading the graph from the
 * source data. Any input class that is {@code GraphFilterAware} must be able to fully handle both vertex and edge filtering.
 * It is assumed that if the input class is {@code GraphFilterAware}, then the respective
 * {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} will not perform any filtering on the loaded graph.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphFilterAware {

    public void setVertexFilter(final Traversal<Vertex, Vertex> vertexFilter);

    public void setEdgeFilter(final Traversal<Edge, Edge> edgeFilter);

    public static Vertex applyVertexAndEdgeFilters(final Vertex vertex, final Traversal.Admin<Vertex, Vertex> vertexFilter, final Traversal.Admin<Edge, Edge> edgeFilter) {
        if (null == vertex)
            return null;
        else if (vertexFilter == null || TraversalUtil.test(vertex, vertexFilter)) {
            if (edgeFilter != null) {
                final Iterator<Edge> edgeIterator = vertex.edges(Direction.BOTH);
                while (edgeIterator.hasNext()) {
                    if (!TraversalUtil.test(edgeIterator.next(), edgeFilter))
                        edgeIterator.remove();
                }
            }
            return vertex;
        } else {
            return null;
        }
    }

    public static void storeVertexAndEdgeFilters(final Configuration apacheConfiguration, final org.apache.hadoop.conf.Configuration hadoopConfiguration, final Traversal.Admin<Vertex, Vertex> vertexFilter, final Traversal.Admin<Edge, Edge> edgeFilter) {
        if (null != vertexFilter) {
            VertexProgramHelper.serialize(vertexFilter, apacheConfiguration, Constants.GREMLIN_HADOOP_VERTEX_FILTER);
            hadoopConfiguration.set(Constants.GREMLIN_HADOOP_VERTEX_FILTER, apacheConfiguration.getString(Constants.GREMLIN_HADOOP_VERTEX_FILTER));
        }
        if (null != edgeFilter) {
            VertexProgramHelper.serialize(edgeFilter, apacheConfiguration, Constants.GREMLIN_HADOOP_EDGE_FILTER);
            hadoopConfiguration.set(Constants.GREMLIN_HADOOP_EDGE_FILTER, apacheConfiguration.getString(Constants.GREMLIN_HADOOP_EDGE_FILTER));
        }
    }
}
