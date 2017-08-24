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

import org.apache.tinkerpop.gremlin.object.edges.Develops;
import org.apache.tinkerpop.gremlin.object.vertices.Location;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.vertices.Software;
import org.apache.tinkerpop.gremlin.object.edges.Traverses;
import org.apache.tinkerpop.gremlin.object.edges.Uses;
import org.apache.tinkerpop.gremlin.object.structure.Graph;

import lombok.Data;

import static org.apache.tinkerpop.gremlin.object.edges.Uses.Skill.Competent;
import static org.apache.tinkerpop.gremlin.object.edges.Uses.Skill.Expert;
import static org.apache.tinkerpop.gremlin.object.edges.Uses.Skill.Proficient;

/**
 * An object oriented representation of the {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory#createModern}
 * graph.
 *
 * <p>
 * Note that the "person" vertices are defined using the {@link Person} class, as opposed to
 * outlining them in-line. Edges such as "develops" and "uses" have classes of their own.
 *
 * <p>
 * Last, but not the least, the "locations" property on the {@link Person} vertex gets a class of
 * it's own. The value of "locations" is stored in {@link Location#name}, as it's qualified with
 * {@link org.apache.tinkerpop.gremlin.object.model.PropertyValue}. The rest of the fields in the
 * {@link Location} class, such as {@link Location#startTime} become its meta-properties.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
public class TheCrew {

  private final Graph graph;

  public Person marko, stephen, matthias, daniel;
  public Software tinkergraph, gremlin;

  public Develops markoDevelopsGremlin;

  public static TheCrew of(Graph graph) {
    TheCrew crew = new TheCrew(graph);
    crew.generateTheCrew();
    return crew;
  }

  public void generateTheCrew() {
    graph
        .addVertex(Software.of("tinkergraph")).as("tinkergraph")
        .addVertex(Software.of("gremlin")).as("gremlin")
        .addEdge(Traverses.of(), "tinkergraph");
    graph
        .addVertex(
            Person.of("marko",
                Location.of("san diego", 1997, 2001),
                Location.of("santa cruz", 2001, 2004),
                Location.of("brussels", 2004, 2005),
                Location.of("santa fe", 2005))).as("marko")
        .addEdge(Develops.of(2009), "gremlin").as(this::setMarkoDevelopsGremlin)
        .addEdge(Develops.of(2010), "tinkergraph")
        .addEdge(Uses.of(Proficient), "gremlin")
        .addEdge(Uses.of(Expert), "tinkergraph")
        .addVertex(
            Person.of("stephen",
                Location.of("centreville", 1990, 2000),
                Location.of("dulles", 2000, 2006),
                Location.of("purcellville", 2006))).as("stephen")
        .addEdge(Develops.of(2010), "gremlin")
        .addEdge(Develops.of(2011), "tinkergraph")
        .addEdge(Uses.of(Expert), "gremlin")
        .addEdge(Uses.of(Proficient), "tinkergraph")
        .addVertex(
            Person.of("matthias",
                Location.of("bremen", 2004, 2007),
                Location.of("baltimore", 2007, 2011),
                Location.of("oakland", 2011, 2014),
                Location.of("seattle", 2014))).as("matthias")
        .addEdge(Develops.of(2012), "gremlin")
        .addEdge(Uses.of(Competent), "gremlin")
        .addEdge(Uses.of(Competent), "tinkergraph")
        .addVertex(
            Person.of("daniel",
                Location.of("spremberg", 1982, 2005),
                Location.of("kaiserslautern", 2005, 2009),
                Location.of("aachen", 2009))).as("daniel")
        .addEdge(Uses.of(Expert), "gremlin")
        .addEdge(Uses.of(Competent), "tinkergraph");

    marko = graph.get("marko", Person.class);
    stephen = graph.get("stephen", Person.class);
    matthias = graph.get("matthias", Person.class);
    daniel = graph.get("daniel", Person.class);

    gremlin = graph.get("gremlin", Software.class);
    tinkergraph = graph.get("tinkergraph", Software.class);
  }

}
