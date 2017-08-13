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
package org.apache.tinkerpop.gremlin.object.structure;

import org.apache.tinkerpop.gremlin.object.graphs.Modern;
import org.apache.tinkerpop.gremlin.object.graphs.TheCrew;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.vertices.Software;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verify that the sub-graphs can be defined and composed as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SubGraphTest {

  private Graph graph;
  private Person person;
  private Software software;

  @Before
  public void setUp() {
    graph = mock(Graph.class);
    person = mock(Person.class);
    software = mock(Software.class);

    when(graph.addVertex(any())).thenReturn(graph);
    when(graph.addEdge(any(Edge.class), anyString())).thenReturn(graph);
    when(graph.addEdge(any(Edge.class), any(Vertex.class), any(Vertex.class))).thenReturn(graph);
    when(graph.addEdge(any(Edge.class), any(Vertex.class), anyString())).thenReturn(graph);
    when(graph.as(anyString())).thenReturn(graph);
    when(graph.as(any(Consumer.class))).thenReturn(graph);

    when(graph.get(eq("marko"), any(Class.class))).thenReturn(person);
    when(graph.get(eq("stephen"), any(Class.class))).thenReturn(person);
    when(graph.get(eq("matthias"), any(Class.class))).thenReturn(person);
    when(graph.get(eq("daniel"), any(Class.class))).thenReturn(person);

    when(graph.get(eq("gremlin"), any(Class.class))).thenReturn(software);
    when(graph.get(eq("tinkergraph"), any(Class.class))).thenReturn(software);
  }

  @Test
  public void testSubGraphChange() {
    SubGraph createCrew = graph ->
        TheCrew.of(graph).getGraph();

    createCrew.apply(graph);

    verify(graph, times(6)).addVertex(any(Vertex.class));
    verify(graph, times(14)).addEdge(any(Edge.class), anyString());
    verify(graph, times(6)).as(anyString());
    verify(graph, times(6)).get(anyString(), any(Class.class));
  }

  @Test
  public void testSubGraphCompose() {
    SubGraph createCrew = graph ->
        TheCrew.of(graph).getGraph();
    SubGraph createModern = graph ->
        Modern.of(graph).getGraph();

    createCrew.chain(createModern).apply(graph);

    verify(graph, times(12)).addVertex(any(Vertex.class));
    verify(graph, times(17)).addEdge(any(Edge.class), anyString());
    verify(graph, times(2)).addEdge(any(Edge.class), any(Vertex.class), anyString());
    verify(graph, times(1)).addEdge(any(Edge.class), any(Vertex.class), any(Vertex.class));
    verify(graph, times(10)).as(anyString());
    verify(graph, times(10)).get(anyString(), any(Class.class));
  }

}
