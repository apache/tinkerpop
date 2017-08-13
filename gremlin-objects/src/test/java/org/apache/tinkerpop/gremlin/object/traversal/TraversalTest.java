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
package org.apache.tinkerpop.gremlin.object.traversal;

import org.apache.tinkerpop.gremlin.object.model.PrimaryKey;
import org.apache.tinkerpop.gremlin.object.structure.Connection;
import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.junit.Before;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An abstraction that serves as the base for all test cases under the library package. It provides
 * a mocked {@link GraphTraversal}, with certain {@link org.mockito.Mockito#when}s applied.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings("unchecked")
public abstract class TraversalTest<O> {

  protected GraphTraversalSource g;
  protected GraphTraversal<Element, O> traversal;

  protected City sanFrancisco;

  @Before
  public void setUp() {
    sanFrancisco = City.of("San Francisco");
    traversal = (GraphTraversal<Element, O>) mock(GraphTraversal.class);
    when(traversal.property(anyString(), any())).thenReturn(traversal);
    when(traversal.hasLabel(anyString())).thenReturn(traversal);
    when(traversal.count()).thenReturn((GraphTraversal) traversal);
    g = mock(GraphTraversalSource.class);
    when(g.V()).thenReturn((GraphTraversal) traversal);
    when(g.addV(anyString())).thenReturn((GraphTraversal) traversal);
  }

  protected GraphTraversal<Element, O> traverse(AnyTraversal anyTraversal) {
    return (GraphTraversal<Element, O>) anyTraversal.apply(g);
  }

  @SuppressWarnings("rawtypes")
  protected GraphTraversal<Element, O> traverse(SubTraversal subTraversal) {
    return (GraphTraversal<Element, O>) subTraversal.apply(traversal);
  }

  @Data
  @Builder
  @AllArgsConstructor
  @EqualsAndHashCode(callSuper = true)
  public static class City extends Vertex {

    @PrimaryKey
    private String name;
    private int population;

    public static City of(String name) {
      return City.builder().name(name).build();
    }
  }

  @Data
  @Builder
  @EqualsAndHashCode(callSuper = true)
  @RequiredArgsConstructor(staticName = "of")
  public static class Highway extends Edge {

    @PrimaryKey
    private final String name;

    @Override
    protected List<Connection> connections() {
      return Connection.list(City.class, City.class);
    }
  }

}
