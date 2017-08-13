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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.object.vertices.Location.year;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.all;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.id;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.list;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.names;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.of;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.values;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Assert that {@link Properties} lists the key and value of properties of any given kind.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class PropertiesTest extends ReflectTest {

  @Test
  public void testPropertyId() {
    assertArrayEquals(new Object[] {"name", "marko", "age", 29}, id(marko));
    assertArrayEquals(new Object[] {}, id(develops));
  }

  @Test
  public void testPropertyAll() {
    assertArrayEquals(new Object[] {"name", "marko", "age", 29}, all(marko));
    assertArrayEquals(new Object[] {"since", year(2000)}, all(develops));
  }

  @Test
  public void testPropertyOf() {
    assertArrayEquals(new Object[] {}, of(marko));
    assertArrayEquals(new Object[] {"since", year(2000)}, of(develops));
  }

  @Test
  public void testPropertyNames() {
    assertEquals(Arrays.asList("name", "age", "locations", "titles"), names(marko));
    assertEquals(Arrays.asList("since"), names(develops));
  }

  @Test
  public void testPropertyValues() {
    assertEquals(Arrays.asList("san diego", "startTime", year(2000)), values(location));
    assertEquals(Arrays.asList("since", year(2000)), values(develops));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPropertyLists() {
    Property<Instant> since = list(of(develops)).get(0);
    assertEquals("since", since.key());
    assertEquals(year(2000), since.value());
  }
}
