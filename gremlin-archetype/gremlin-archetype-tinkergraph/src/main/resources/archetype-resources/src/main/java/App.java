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

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import java.util.List;
import java.util.ArrayList;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class App {

    public static void main(String[] args) {
        // Create a new TinkerGraph and load some test data. The Graph instance is typically named "graph" as a
        // variable name. You will see this pattern consistently in TinkerPop documentation, the mailing list, etc.
        Graph graph = TinkerGraph.open();
        loadData(graph);

        // Create a GraphTraversalSource instance that is used to query the data in the Graph instance. This variable
        // is typically denoted as "g".  In TinkerPop documentation you can always count on references to "g" as
        // being a object of this type.
        GraphTraversalSource g = graph.traversal();

        Vertex fromNode = findByName(g, "marko");
        Vertex toNode = findByName(g, "peter");

        List list = calculateShortestPathBetween(g, fromNode, toNode);
        System.out.println(list.toString());
        System.exit(0);
    }

    public static Vertex findByName(GraphTraversalSource g, String name) {
        return g.V().has("name", name).next();
    }

    public static List calculateShortestPathBetween(GraphTraversalSource g, Vertex fromNode, Vertex toNode) {
        ArrayList list = new ArrayList();
        g.V(fromNode).repeat(both().simplePath()).until(is(toNode)).limit(1).path().fill(list);
        return list;
    }

    public static void loadData(Graph graph) {
        // see org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.generateModern()
        final Vertex marko = graph.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = graph.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = graph.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = graph.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = graph.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = graph.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);
    }
}