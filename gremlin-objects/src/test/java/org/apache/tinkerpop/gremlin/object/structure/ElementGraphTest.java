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
import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.CREATE;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.IGNORE;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.INSERT;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.MERGE;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.REPLACE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 *  Assert that the element graph finds and updates elements as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@RunWith(Parameterized.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class ElementGraphTest<O extends Element> extends GraphTest {

  @Parameterized.Parameter
  public Graph.Should should;
  protected GraphTraversalSource g;
  protected GraphTraversal traversal;
  private ElementGraph elementGraph;

  public ElementGraphTest() {
    super(mock(Graph.class));
  }

  @Parameterized.Parameters(name = "should({0})")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {CREATE}, {MERGE}, {REPLACE}, {IGNORE}, {INSERT}
    });
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    super.setUp();

    when(graph.should()).thenReturn(should);

    traversal = mock(GraphTraversal.class);
    when(traversal.V()).thenReturn(traversal);
    when(traversal.has(anyString(), anyString())).thenReturn(traversal);
    when(traversal.has(anyVararg())).thenReturn(traversal);
    when(traversal.hasLabel(anyString())).thenReturn(traversal);
    when(traversal.hasId(anyString())).thenReturn(traversal);
    when(traversal.as(anyString())).thenReturn(traversal);
    when(traversal.out(anyString())).thenReturn(traversal);
    when(traversal.inE(anyString())).thenReturn(traversal);
    when(traversal.addE(anyString())).thenReturn(traversal);
    when(traversal.addV(anyString())).thenReturn(traversal);
    when(traversal.from(anyString())).thenReturn(traversal);
    when(traversal.coalesce(anyVararg())).thenReturn(traversal);
    when(traversal.select(anyString())).thenReturn(traversal);
    when(traversal.property(anyString(), any())).thenReturn(traversal);
    when(traversal.property(any(), anyString(), any(), anyVararg())).thenReturn(traversal);
    when(traversal.properties(anyVararg())).thenReturn(traversal);
    when(traversal.drop()).thenReturn(traversal);
    when(traversal.choose(__.value())).thenReturn(traversal);

    g = mock(GraphTraversalSource.class);
    when(g.V()).thenReturn(traversal);
    when(g.addV(anyString())).thenReturn(traversal);
    when(g.inject(anyVararg())).thenReturn(traversal);

    elementGraph = new ElementGraph(graph, g);
  }

  protected O createElement() {
    return (O) Location.of("San Francisco", 2000);
  }

  @Test
  public void testFindElement() {
    Location location = (Location) createElement();

    elementGraph.find(location);

    InOrder inOrder = inOrder(traversal);
    inOrder.verify(traversal, times(1)).hasLabel(location.label());
    inOrder.verify(traversal, times(1)).has("name", location.getName());
    inOrder.verify(traversal, times(1)).has("startTime", location.getStartTime());
  }

  @Test
  public void testUpdateElement() {
    Location location = (Location) createElement();

    elementGraph.update(traversal, location, Properties::all);

    InOrder inOrder = inOrder(traversal);
    inOrder.verify(traversal, times(1)).property("name", "San Francisco");
    inOrder.verify(traversal, times(1)).property("startTime", location.getStartTime());
  }
}
