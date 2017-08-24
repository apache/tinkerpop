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

import org.apache.tinkerpop.gremlin.object.vertices.Location;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.apache.tinkerpop.gremlin.object.vertices.Location.year;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.REPLACE;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.list;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * * Assert that the vertex graph adds, updates and removes vertex as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings("unchecked")
public class VertexGraphTest extends ElementGraphTest<Person> {

  private VertexGraph vertexGraph;

  private org.apache.tinkerpop.gremlin.structure.Vertex marko, vadas;

  @Before
  public void setUp() {
    super.setUp();
    marko = mock(org.apache.tinkerpop.gremlin.structure.Vertex.class);
    vadas = mock(org.apache.tinkerpop.gremlin.structure.Vertex.class);
    when(marko.id()).thenReturn(1);
    when(vadas.id()).thenReturn(2);
    vertexGraph = new VertexGraph(graph, g);
    when(traversal.next()).thenReturn(
        mock(org.apache.tinkerpop.gremlin.structure.Vertex.class));
    when(traversal.property(any(), anyString(), anyVararg())).thenReturn(traversal);
  }

  @Override
  protected Person createElement() {
    return Person.of("marko", 29,
        Location.of("san diego", 1997, 2001),
        Location.of("santa cruz", 2001, 2004));
  }

  @Test
  public void testFindElement() {
    Person marko = createElement();

    vertexGraph.find(marko);

    InOrder inOrder = inOrder(traversal);
    inOrder.verify(traversal, times(1)).hasLabel(marko.label());
    inOrder.verify(traversal, times(1)).has("name", marko.name());
  }

  @Test
  public void testUpdateElement() {
    Person marko = createElement();

    vertexGraph.update(traversal, marko, Properties::of);

    InOrder inOrder = inOrder(traversal);
    inOrder.verify(traversal, times(1))
        .property(should.equals(REPLACE) ? single : list,
            "locations",
            "san diego",
            new Object[] {
                "startTime",
                year(1997),
                "endTime",
                year(2001)});
    inOrder.verify(traversal, times(1))
        .property(list,
            "locations",
            "santa cruz",
            new Object[] {
                "startTime",
                year(2001),
                "endTime",
                year(2004)
            }

        );
  }

  @Test
  public void testAddVertex() {
    Person marko = createElement();

    vertexGraph.addVertex(marko);

    InOrder inOrder = inOrder(g, traversal);

    switch (should) {
      case MERGE:
        inOrder.verify(g, times(1)).inject(1);
        inOrder.verify(traversal, times(1)).coalesce(traversal, traversal);
        break;
      case REPLACE:
        inOrder.verify(g, times(1)).inject(1);
        inOrder.verify(traversal, times(1)).coalesce(traversal, traversal, traversal);
        break;
      case INSERT:
        inOrder.verify(g, times(1)).V();
        inOrder.verify(traversal, times(1)).coalesce(traversal, traversal);
        break;
      case IGNORE:
        inOrder.verify(g, times(1)).inject(1);
        inOrder.verify(traversal, times(1)).coalesce(traversal, traversal);
        break;
      default:
        break;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddInvalidVertex() {
    Person marko = createElement();
    marko.name(null);

    vertexGraph.addVertex(marko);
  }

  @Test
  public void testRemoveVertex() {
    Person marko = createElement();

    vertexGraph.removeVertex(marko);

    InOrder inOrder = inOrder(g, traversal);
    inOrder.verify(g, times(1)).V();
    inOrder.verify(traversal, times(1)).hasLabel(marko.label());
    inOrder.verify(traversal, times(1)).has("name", marko.name());
    inOrder.verify(traversal, times(1)).drop();
    inOrder.verify(traversal, times(1)).toList();
  }

}
