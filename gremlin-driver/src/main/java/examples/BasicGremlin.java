/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package examples;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class BasicGremlin {
    public static void main(String[] args) {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = traversal().withEmbedded(graph);

        // Basic Gremlin: adding and retrieving data
        Vertex v1 = g.addV("person").property("name","marko").next();
        Vertex v2 = g.addV("person").property("name","stephen").next();
        Vertex v3 = g.addV("person").property("name","vadas").next();

        // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
        // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
        g.V(v1).addE("knows").to(v2).property("weight",0.75).iterate();
        g.V(v1).addE("knows").to(v3).property("weight",0.75).iterate();

        // Retrieve the data from the "marko" vertex
        Object marko = g.V().has("person","name","marko").values("name").next();
        System.out.println("name: " + marko);

        // Find the "marko" vertex and then traverse to the people he "knows" and return their data
        List<Object> peopleMarkoKnows = g.V().has("person","name","marko").out("knows").values("name").toList();
        for (Object person : peopleMarkoKnows) {
            System.out.println("marko knows " + person);
        }
    }
}
