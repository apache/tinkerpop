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
import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasKeys;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.PriorityQueue;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.is;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isCollection;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isEdge;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isElement;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isFunctional;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isList;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isSet;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isVertex;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Assert that the {@link Classes#is*} methods behave as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class ClassesTest extends ReflectTest {

  @Test
  public void testIsElement() {
    assertTrue(isElement(Person.class));
    assertTrue(isElement(Develops.class));
  }

  @Test
  public void testIsVertex() {
    assertTrue(isVertex(Person.class));
    assertFalse(isVertex(Develops.class));
  }

  @Test
  public void testIsEdge() {
    assertFalse(isEdge(Person.class));
    assertTrue(isEdge(Develops.class));
  }

  @Test
  public void testIsList() {
    assertFalse(isList(Person.class));
    assertTrue(isList(Arrays.asList(develops)));
  }

  @Test
  public void testIsSet() {
    assertTrue(isSet(new HashSet<>()));
    assertFalse(isSet(Person.class));
  }

  @Test
  public void testIsCollection() {
    assertTrue(isCollection(new ArrayList<>()));
    assertFalse(isCollection(new PriorityQueue<>()));
    assertFalse(isCollection(Person.class));
  }

  @Test
  public void testIsFunctional() {
    assertTrue(isFunctional(HasKeys.class));
    assertFalse(isFunctional(Person.class));
  }

  @Test
  public void testIsSomething() {
    assertTrue(is(Develops.class, Edge.class));
    assertTrue(is(develops, Edge.class));
    assertTrue(is(Develops.class, develops));
    assertFalse(is(Develops.class, Vertex.class));
    assertFalse(is(develops, Vertex.class));
    assertFalse(is(Develops.class, marko));
  }

}
