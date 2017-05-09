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
package ${package};

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class SocialTraversalSourceDsl extends GraphTraversalSource {

    public SocialTraversalSourceDsl(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public SocialTraversalSourceDsl(final Graph graph) {
        super(graph);
    }

    public GraphTraversal<Vertex, Vertex> persons(String... names) {
        GraphTraversalSource clone = this.clone();
        clone.getBytecode().addStep(GraphTraversal.Symbols.V);
        clone.getBytecode().addStep(GraphTraversal.Symbols.hasLabel, "person");

        GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        traversal.addStep(new GraphStep<>(traversal, Vertex.class, true));
        TraversalHelper.addHasContainer(traversal, new HasContainer(T.label.getAccessor(), P.eq("person")));

        if (names.length > 0) {
            clone.getBytecode().addStep(GraphTraversal.Symbols.has, P.within(names));
            TraversalHelper.addHasContainer(traversal, new HasContainer("name", P.within(names)));
        }

        return traversal;
    }
}
