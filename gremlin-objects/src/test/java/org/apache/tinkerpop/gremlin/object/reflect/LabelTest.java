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
package org.apache.tinkerpop.gremlin.object.reflect;

import org.apache.tinkerpop.gremlin.object.edges.Develops;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.object.reflect.Label.of;
import static org.junit.Assert.assertEquals;

/**
 * Assert that {@link Label} determines the label of the element as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class LabelTest extends ReflectTest {

  @Test
  public void testVertexLabelWithAlias() {
    assertEquals("person", of(Person.class));
    assertEquals("person", of(marko));
  }

  @Test
  public void testEdgeLabelWithoutAlias() {
    assertEquals("develops", of(Develops.class));
    assertEquals("develops", of(develops));
  }

  @Test
  public void testVertexLabelWithoutAlias() {
    assertEquals("City", of(City.class));
  }

  private static class City extends Vertex {

  }
}
