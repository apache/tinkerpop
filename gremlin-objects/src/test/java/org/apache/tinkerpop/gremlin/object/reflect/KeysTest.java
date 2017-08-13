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
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;

import static org.apache.tinkerpop.gremlin.object.reflect.Fields.field;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.hasPrimaryKeys;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.id;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.isOrderingKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.isPrimaryKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.orderingKeyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.primaryKeyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.primaryKeyNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Assert that {@link Keys} finds the fields annotated
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class KeysTest extends ReflectTest {

  private Field personName;
  private Field personAge;
  private Field personTitles;

  @Before
  public void setUp() {
    super.setUp();
    personName = field(Person.class, "name");
    personAge = field(Person.class, "age");
    personTitles = field(Person.class, "titles");
  }

  @Test
  public void testIsPrimaryKey() {
    assertTrue(isPrimaryKey(personName));
    assertFalse(isPrimaryKey(personAge));
    assertFalse(isPrimaryKey(personTitles));
  }

  @Test
  public void testIsOrderingKey() {
    assertFalse(isOrderingKey(personName));
    assertTrue(isOrderingKey(personAge));
    assertFalse(isOrderingKey(personTitles));
  }

  @Test
  public void testPrimaryKeyFields() {
    assertEquals(Arrays.asList(personName), primaryKeyFields(Person.class));
    assertTrue(primaryKeyFields(Develops.class).isEmpty());
  }

  @Test
  public void testHasPrimaryKey() {
    assertTrue(hasPrimaryKeys(Person.class));
    assertFalse(hasPrimaryKeys(Develops.class));
  }

  @Test
  public void testOrderingKeyFields() {
    assertEquals(Arrays.asList(personAge), orderingKeyFields(Person.class));
    assertTrue(orderingKeyFields(Develops.class).isEmpty());
  }

  @Test
  public void testPrimaryKeyNames() {
    assertEquals(Arrays.asList("name"), primaryKeyNames(Person.class));
    assertTrue(primaryKeyNames(Develops.class).isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullPrimaryKey() {
    marko.setName(null);
    id(marko);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullOrderingKey() {
    Primitives.allowDefaultKeys = false;
    marko.setAge(0);
    id(marko);
  }

  @Test
  @SuppressWarnings("serial")
  public void testIdIsInternalOrGenerated() {
    assertEquals(new HashMap<String, Object>() {
      {
        put(T.label.getAccessor(), marko.label());
        put("name", "marko");
        put("age", 29);
      }
    }, id(marko));
  }

}
