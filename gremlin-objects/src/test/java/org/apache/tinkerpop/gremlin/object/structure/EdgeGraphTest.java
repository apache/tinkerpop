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

import org.apache.tinkerpop.gremlin.object.edges.Develops;
import org.apache.tinkerpop.gremlin.object.reflect.Label;
import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Assert that the edge graph adds, updates and removes edges as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings("unchecked")
public class EdgeGraphTest extends ElementGraphTest<Develops> {

  private EdgeGraph edgeGraph;

  private Query query;

  private Vertex marko, vadas;

  @Before
  public void setUp() {
    super.setUp();
    marko = mock(Vertex.class);
    vadas = mock(Vertex.class);
    when(marko.id()).thenReturn(1);
    when(vadas.id()).thenReturn(2);
    query = mock(Query.class);
    edgeGraph = new EdgeGraph(graph, query, g);
    when(traversal.next()).thenReturn(
        new DetachedEdge(null, Label.of(Develops.class), null,
            null, null, null, null));
  }

  @Override
  protected Develops createElement() {
    return Develops.of(2000);
  }

  @Test(expected = IllegalStateException.class)
  public void testAddEdgeWithMissingVertex() {
    Develops develops = createElement();
    edgeGraph.addEdge(develops, null, marko);
  }

  @Test(expected = IllegalStateException.class)
  public void testAddEdgeWithEmptyTraversal() {
    Develops develops = createElement();

    when(query.by((AnyTraversal) any())).thenReturn(query);
    when(query.by((SubTraversal) any())).thenReturn(query);
    when(query.list((Class) any())).thenReturn(null);
    edgeGraph.addEdge(develops, marko, g -> null);
  }

  @Test
  public void testFindElement() {
    Develops develops = createElement();

    edgeGraph.find(develops);

    InOrder inOrder = inOrder(traversal);
    inOrder.verify(traversal, times(1)).hasLabel(develops.label());
    inOrder.verify(traversal, times(1)).has("since", develops.since());
  }

  @Test
  public void testUpdateElement() {
    Develops develops = createElement();

    edgeGraph.update(traversal, develops, Properties::all);

    verify(traversal, times(1)).property("since", develops.since());
  }

  @Test
  public void testAddEdge() {
    Develops develops = createElement();

    edgeGraph.addEdge(develops, marko, vadas);

    InOrder inOrder = inOrder(g, traversal);

    switch (should) {
      case CREATE:
        inOrder.verify(g, times(1)).V();
        inOrder.verify(traversal, times(1)).hasId(marko.id());
        inOrder.verify(traversal, times(1)).as("from");
        inOrder.verify(traversal, times(1)).V();
        inOrder.verify(traversal, times(1)).hasId(vadas.id());
        inOrder.verify(traversal, times(1)).as("to");
        inOrder.verify(traversal, times(1)).addE(develops.label());
        inOrder.verify(traversal, times(1)).as("edge");
        inOrder.verify(traversal, times(1)).from("from");
        verify(traversal, times(1)).property("since", develops.since());
        break;
      case MERGE:
        inOrder.verify(g, times(1)).inject(1);
        inOrder.verify(traversal, times(1)).coalesce(traversal, traversal);
        break;
      case REPLACE:
        inOrder.verify(g, times(1)).inject(1);
        inOrder.verify(traversal, times(1)).coalesce(traversal, traversal, traversal);
        break;
      case INSERT:
        // coalesce and complete the insert
        inOrder.verify(g, times(1)).inject(1);

        // try to find the edge, and fail if found
        inOrder.verify(traversal, times(1)).hasId(marko.id());
        inOrder.verify(traversal, times(1)).as("from");
        inOrder.verify(traversal, times(1)).out(develops.label());
        inOrder.verify(traversal, times(1)).hasId(vadas.id());
        inOrder.verify(traversal, times(1)).as("to");
        inOrder.verify(traversal, times(1)).inE(develops.label());
        inOrder.verify(traversal, times(1)).as("edge");
        inOrder.verify(traversal, times(1)).choose(__.value());

        // try to insert the edge
        inOrder.verify(g, times(1)).V();
        inOrder.verify(traversal, times(1)).hasId(marko.id());
        inOrder.verify(traversal, times(1)).as("from");
        inOrder.verify(traversal, times(1)).V();
        inOrder.verify(traversal, times(1)).hasId(vadas.id());
        inOrder.verify(traversal, times(1)).as("to");
        inOrder.verify(traversal, times(1)).addE(develops.label());
        inOrder.verify(traversal, times(1)).as("edge");
        inOrder.verify(traversal, times(1)).from("from");
        verify(traversal, times(1)).property("since", develops.since());

        // coalesce the find and insert
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

  @Test
  public void testRemoveEdge() {
    Develops develops = createElement();
    develops.setFromId(marko.id());
    develops.setToId(vadas.id());

    edgeGraph.removeEdge(develops);

    InOrder inOrder = inOrder(g, traversal);
    inOrder.verify(g, times(1)).V();
    inOrder.verify(traversal, times(1)).hasId(marko.id());
    inOrder.verify(traversal, times(1)).as("from");
    inOrder.verify(traversal, times(1)).out(develops.label());
    inOrder.verify(traversal, times(1)).hasId(vadas.id());
    inOrder.verify(traversal, times(1)).as("to");
    inOrder.verify(traversal, times(1)).inE(develops.label());
    inOrder.verify(traversal, times(1)).as("edge");
    inOrder.verify(traversal, times(1)).select("edge");
    inOrder.verify(traversal, times(1)).toList();
  }
}
