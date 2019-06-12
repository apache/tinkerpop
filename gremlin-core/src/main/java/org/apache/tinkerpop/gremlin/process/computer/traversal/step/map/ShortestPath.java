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
package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Configuration options to be passed to the {@link GraphTraversal#with(String, Object)} step.
 */
public class ShortestPath {

    /**
     * Configures the traversal to use to filter the target vertices for all shortest paths.
     */
    public static final String target = Graph.Hidden.hide("tinkerpop.shortestPath.target");

    /**
     * Configures the direction or traversal to use to filter the edges traversed during the shortest path search phase.
     */
    public static final String edges = Graph.Hidden.hide("tinkerpop.shortestPath.edges");

    /**
     * Configures the edge property or traversal to use for shortest path distance calculations.
     */
    public static final String distance = Graph.Hidden.hide("tinkerpop.shortestPath.distance");

    /**
     * Configures the maximum distance for all shortest paths. Any path with a distance greater than the specified
     * value will not be returned.
     */
    public static final String maxDistance = Graph.Hidden.hide("tinkerpop.shortestPath.maxDistance");

    /**
     * Configures the inclusion of edges in the shortest path computation result.
     */
    public static final String includeEdges = Graph.Hidden.hide("tinkerpop.shortestPath.includeEdges");

    static boolean configure(final ShortestPathVertexProgramStep step, final String key, final Object value) {

        if (target.equals(key)) {
            if (value instanceof Traversal) {
                step.setTargetVertexFilter((Traversal) value);
                return true;
            }
            else throw new IllegalArgumentException("ShortestPath.target requires a Traversal as its argument");
        }
        else if (edges.equals(key)) {
            if (value instanceof Traversal) {
                step.setEdgeTraversal((Traversal) value);
                return true;
            }
            else if (value instanceof Direction) {
                step.setEdgeTraversal(__.toE((Direction) value));
                return true;
            }
            else throw new IllegalArgumentException(
                    "ShortestPath.edges requires a Traversal or a Direction as its argument");
        }
        else if (distance.equals(key)) {
            if (value instanceof Traversal) {
                step.setDistanceTraversal((Traversal) value);
                return true;
            }
            else if (value instanceof String) {
                // todo: new ElementValueTraversal((String) value)
                step.setDistanceTraversal(__.values((String) value));
                return true;
            }
            else throw new IllegalArgumentException(
                    "ShortestPath.distance requires a Traversal or a property name as its argument");
        }
        else if (maxDistance.equals(key)) {
            if (value instanceof Number) {
                step.setMaxDistance((Number) value);
                return true;
            }
            else throw new IllegalArgumentException("ShortestPath.maxDistance requires a Number as its argument");
        }
        else if (includeEdges.equals(key)) {
            if (value instanceof Boolean) {
                step.setIncludeEdges((Boolean) value);
                return true;
            }
            else throw new IllegalArgumentException("ShortestPath.includeEdges requires a Boolean as its argument");
        }
        return false;
    }
}
