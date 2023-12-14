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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.structure.T.id;

public class ModernTraversals {
    public static void main(String[] args) {
        // Performs basic traversals on the Modern toy graph which can be created using TinkerFactory
        Graph modern = TinkerFactory.createModern();
        GraphTraversalSource g = traversal().withEmbedded(modern);

        List<Edge> e1 = g.V(1).bothE().toList(); // (1)
        List<Edge> e2 = g.V(1).bothE().where(otherV().hasId(2)).toList(); // (2)
        Vertex v1 = g.V(1).next();
        Vertex v2 = g.V(2).next();
        List<Edge> e3 = g.V(v1).bothE().where(otherV().is(v2)).toList(); // (3)
        List<Edge> e4 = g.V(v1).outE().where(inV().is(v2)).toList(); // (4)
        List<Edge> e5 = g.V(1).outE().where(inV().has(id, within(2,3))).toList(); // (5)
        List<Vertex> e6 = g.V(1).out().where(in().hasId(6)).toList(); // (6)

        System.out.println("1: " + e1.toString());
        System.out.println("2: " + e2.toString());
        System.out.println("3: " + e3.toString());
        System.out.println("4: " + e4.toString());
        System.out.println("5: " + e5.toString());
        System.out.println("6: " + e6.toString());

        /*
        1. There are three edges from the vertex with the identifier of "1".
        2. Filter those three edges using the where()-step using the identifier of the vertex returned by otherV() to
           ensure it matches on the vertex of concern, which is the one with an identifier of "2".
        3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
           identifiers.
        4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
           steps of outE() and inV() since the schema allows it.
        5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
           case, vertices with identifiers "2" and "3".
        6. There’s no reason why the same pattern of exclusion used for edges with where() can’t work for a vertex
           between two vertices.
        */
    }
}
