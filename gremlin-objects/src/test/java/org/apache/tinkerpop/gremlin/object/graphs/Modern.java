/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.object.graphs;

import org.apache.tinkerpop.gremlin.object.edges.Created;
import org.apache.tinkerpop.gremlin.object.edges.Knows;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.vertices.Software;
import org.apache.tinkerpop.gremlin.object.structure.Graph;

import lombok.Data;

/**
 * An object oriented representation of the {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory#createTheCrew}
 * graph.
 *
 * <p>
 * Note that the "person" and "software" vertices are defined using the {@link Person} and {@link
 * Software} classes, as opposed to outlining their properties in-line. The "knows" and "created"
 * edges are represented as the {@link Knows} and {@link Created} classes respectively.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
public class Modern {

  private final Graph graph;

  public Person marko, vadas, josh, peter;
  public Software lop, ripple, gremlin;

  public static Modern of(Graph graph) {
    Modern modern = new Modern(graph);
    modern.generateModern();
    return modern;
  }

  private void generateModern() {
    graph
        .addVertex(Person.of("marko", 29)).as(this::setMarko)
        .addVertex(Person.of("vadas", 27)).as(this::setVadas)
        .addVertex(Software.of("lop")).as("lop")
        .addVertex(Software.of("ripple")).as("ripple")
        .addVertex(Person.of("josh", 32)).as("josh")
        .addEdge(Created.of(1.0d), "ripple")
        .addEdge(Created.of(0.4d), "lop")
        .addVertex(Person.of("peter", 35)).as("peter")
        .addEdge(Created.of(0.2d), "lop");
    graph
        .addEdge(Knows.of(0.5d), marko, vadas)
        .addEdge(Knows.of(1.0d), marko, "josh")
        .addEdge(Created.of(0.4d), marko, "lop");

    josh = graph.get("josh", Person.class);
    peter = graph.get("peter", Person.class);

    lop = graph.get("lop", Software.class);
    ripple = graph.get("ripple", Software.class);
  }
}
