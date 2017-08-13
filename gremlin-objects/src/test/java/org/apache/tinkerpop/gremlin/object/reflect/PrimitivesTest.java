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

import org.apache.tinkerpop.gremlin.object.structure.Graph;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.object.vertices.Location.year;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isPrimitive;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isTimeType;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.toTimeType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Assert that {@link Primitives} identifies the registered primitive types properly. It also tests
 * that time properties can be converted between compatible types.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class PrimitivesTest extends ReflectTest {

  private Instant now;

  @Before
  public void setUp() {
    now = year(2017);
  }

  @Test
  public void testIsPrimitiveType() {
    assertTrue(isPrimitive(String.class));
    assertTrue(isPrimitive(Graph.Should.class));
    assertFalse(isPrimitive(Set.class));
  }

  @Test
  public void testIsTimeType() {
    assertTrue(isTimeType(Instant.class));
    assertFalse(isTimeType(Integer.class));
  }

  @Test
  public void testToDateTime() {
    assertEquals(Date.from(now), toTimeType(now, Date.class));
    assertEquals(Date.from(now), toTimeType(now.toEpochMilli(), Date.class));
  }

  @Test
  public void testToInstantTime() {
    assertEquals(now, toTimeType(Date.from(now), Instant.class));
    assertEquals(now, toTimeType(now.toEpochMilli(), Instant.class));
  }

  @Test
  public void testToEpochTime() {
    assertEquals(Long.valueOf(now.toEpochMilli()), toTimeType(now, Long.class));
    assertEquals(Long.valueOf(now.toEpochMilli()), toTimeType(Date.from(now), Long.class));
  }
}
