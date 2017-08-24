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
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.tinkerpop.gremlin.object.reflect.Parser.as;
import static org.apache.tinkerpop.gremlin.object.reflect.Parser.isPropertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.field;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.MERGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Assert that {@link Parser} materializes {@link org.apache.tinkerpop.gremlin.object.structure.Element}
 * objects from underlying {@link org.apache.tinkerpop.gremlin.structure.Element} structures as
 * expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings({"unchecked", "serial"})
public class ParserTest extends ReflectTest {

  @Test
  public void testIsPropertyValue() {
    assertTrue(isPropertyValue(field(location, "name")));
    assertFalse(isPropertyValue(field(marko, "age")));
  }

  @Test
  public void testAsEnum() {
    assertEquals(MERGE, as("MERGE", Should.class));
  }

  @Test
  public void testAsItself() {
    assertEquals(MERGE, as(MERGE, Should.class));
  }

  @Test(expected = ClassCastException.class)
  public void testAsUnhandled() {
    assertEquals(Collections.emptyList(), as(new ArrayList<>(), Person.class));
  }

  @Test
  public void testAsVertex() {
    assertEquals(marko, as(new DetachedVertex(
        1, "person", new HashMap<String, Object>() {
          {
            put("name", Arrays.asList(new HashMap<String, Object>() {
              {
                put("value", "marko");
              }
            }));
            put("age", Arrays.asList(new HashMap<String, Object>() {
              {
                put("value", 29);
              }
            }));
          }
        }), Person.class));
  }

  @Test
  public void testAsEdge() {
    assertEquals(develops, as(new DetachedEdge(
        null, Label.of(Develops.class), new HashMap<String, Object>() {
          {
            put("since", develops.since());
          }
        }, null, null, null, null), Develops.class));
  }
}
