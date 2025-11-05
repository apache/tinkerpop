/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tinkerpop.example.cmdb.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.tail;

public final class TraversalBuilders {
    private TraversalBuilders() {
    }

    /**
     * Blast radius paths starting from an incident
     */
    public static Function<GraphTraversalSource, GraphTraversal<?, Path>> incidentBlastRadius(String incidentId, int depth) {
        int d = Math.max(0, depth);
        return g -> g.V().has("incident", "incidentId", incidentId)
                .out("affects")
                .emit()
                .repeat(
                        __.union(
                                __.out("dependsOn"),
                                __.out("deployedAs").in("deploymentOf")
                        )
                )
                .times(d)
                .path().by(__.valueMap(true));
    }

    /**
     * Service dependencies to a given depth (valueMap for DTO-friendly mapping)
     */
    public static Function<GraphTraversalSource, GraphTraversal<?, Map<Object, Object>>> serviceDependencies(String serviceId, int depth) {
        int d = Math.max(1, depth);
        return g -> g.V().has("service", "serviceId", serviceId)
                .emit()
                .repeat(__.out("dependsOn")).times(d)
                .valueMap(true);
    }

    /**
     * Deployment to incident correlation timeline (ordered incidents)
     */
    public static Function<GraphTraversalSource, GraphTraversal<?, Map<Object, Object>>> deploymentTimeline(String deployId) {
        return g -> g.V().has("deployment", "deployId", deployId)
                .coalesce(
                        __.out("deployedTo").in("runsOn").in("deployedAs"),   // via hosts
                        __.out("deploymentOf").in("deployedAs")                 // via application when no deployedTo edges
                )
                .dedup()
                .in("affects").hasLabel("incident")
                .order().by("openedAt", Order.desc)
                .valueMap(true);
    }

    /**
     * Gets Service owners.
     */
    public static Function<GraphTraversalSource, GraphTraversal<?, Map<Object, Object>>> serviceOwners(String serviceId) {
        return g -> g.V().has("service", "serviceId", serviceId)
                .out("ownedBy")
                .valueMap(true);
    }

    public static Function<GraphTraversalSource, GraphTraversal<?, Vertex>> noneAndDiscard() {
        return g -> g.V().none();
    }

    public static Function<GraphTraversalSource, GraphTraversal<?, List<Object>>> firstAndLast() {
        return g -> g.V().hasLabel("team").values("name").
                split("").map(__.union(__.limit(Scope.local,1), tail(Scope.local,1)).fold());
    }

}
