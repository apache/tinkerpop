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

import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.model.PrimaryKey;
import org.apache.tinkerpop.gremlin.object.provider.GraphSystem;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasKeys;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.function.BulkSetSupplier;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verify that the {@link ObjectQuery} executes queries and returns objects as expected. The
 * provider of the {@link GraphSystem} is mocked, as are the gremlin {@link Vertex}s.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Slf4j
@NoArgsConstructor
@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ObjectQueryTest {

  protected GraphSystem system;

  protected ObjectQuery query;

  protected GraphTraversalSource g;

  protected GraphTraversal traversal;

  protected Person marko;

  protected Vertex vertex;

  @Before
  public void setUp() {
    marko = Person.of("marko");
    vertex = new DetachedVertex(
        1, "person", new HashMap<String, Object>() {
          {
            put("name", Arrays.asList(new HashMap() {
              {
                put("value", "marko");
              }
            }));
          }
        });

    system = mock(GraphSystem.class);
    g = mock(GraphTraversalSource.class);
    traversal = mock(GraphTraversal.class);

    when(g.V()).thenReturn(traversal);
    when(system.g()).thenReturn(g);

    query = new ObjectQuery(system) {};
  }

  @Test
  public void testQueryByNativeTraversal() {
    when(system.execute(anyString())).thenReturn((List) Arrays.asList(vertex));

    Person actual = query
        .by("g.V().has('person', 'name', 'marko')")
        .one(Person.class);

    assertEquals(marko, actual);
  }

  @Test
  @SuppressWarnings("cast")
  public void testQueryByAnyTraversal() {
    when(traversal.hasLabel(anyString())).thenReturn(traversal);
    when(traversal.has(anyString(), (Object) any())).thenReturn(traversal);
    BulkSet<Vertex> bulkSet = BulkSetSupplier.<Vertex>instance().get();
    bulkSet.add(vertex);
    when(traversal.toBulkSet()).thenReturn(bulkSet);

    Person actual = query
        .by(g -> g.V().hasLabel(marko.label()).has("name", marko.name()))
        .one(Person.class);

    assertEquals(marko, actual);
  }

  @Test
  @SuppressWarnings("cast")
  public void testQueryBySubTraversals() {
    when(traversal.hasLabel(anyString())).thenReturn(traversal);
    when(traversal.has(anyString(), (Object) any())).thenReturn(traversal);
    BulkSet<Vertex> bulkSet = BulkSetSupplier.<Vertex>instance().get();
    bulkSet.add(vertex);
    when(traversal.toBulkSet()).thenReturn(bulkSet);

    List<Person> actuals = query
        .by(HasKeys.of(marko, PrimaryKey.class))
        .list(Person.class);

    assertEquals(Arrays.asList(marko), actuals);
  }
}
