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

import org.apache.tinkerpop.gremlin.object.model.Alias;
import org.apache.tinkerpop.gremlin.object.model.DefaultValue;
import org.apache.tinkerpop.gremlin.object.model.OrderingKey;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.model.PrimaryKey;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static org.apache.tinkerpop.gremlin.object.reflect.Fields.alias;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.field;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.fields;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.has;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Assert that {@link Fields} finds the object fields corresponding to element properties as
 * expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class FieldsTest extends ReflectTest {

  @Test
  public void testHasAnnotation() {
    assertTrue(has(name, PrimaryKey.class));
    assertFalse(has(name, OrderingKey.class));
  }

  @Test
  public void testAliasProperty() {
    assertEquals("person", alias(Person.class, Alias::label));
    assertNull(alias(locations, Alias::key));
  }

  @Test
  public void testPropertyKey() {
    assertEquals("name", propertyKey(name));
  }

  @Test
  public void testPropertyValue() {
    assertEquals("marko", propertyValue(name, marko));
  }

  @Test
  public void testGetField() {
    assertEquals("age", field(marko, "age").getName());
  }

  @Test
  public void testGetAllFields() {
    assertEquals(
        Arrays.asList("name", "age", "locations", "titles"),
        fields(marko).stream().map(Field::getName).collect(Collectors.toList()));
  }

  @Test
  public void testFieldCacheEnabled() {
    assertEquals(fields(marko), fields(marko));
    List<Field> first = fields(marko);
    List<Field> second = fields(marko);
    assertEquals(first.size(), second.size());
    for (int index = 0; index < first.size(); index++) {
      assertTrue(first.get(index) == second.get(index));
    }
  }

  @Test
  public void testFieldCacheDisabled() {
    try {
      Fields.elementCacheSize = 0;
      List<Field> first = fields(marko);
      List<Field> second = fields(marko);
      assertEquals(first.size(), second.size());
      for (int index = 0; index < first.size(); index++) {
        assertFalse(first.get(index) == second.get(index));
      }
    } finally {
      Fields.elementCacheSize = 100L;
    }
  }

  @Test
  public void testGetPrimaryKeyFields() {
    assertEquals(
        Arrays.asList("name"),
        fields(marko, Keys::isPrimaryKey).stream()
            .map(Field::getName).collect(Collectors.toList()));
  }

  @Test
  public void testGetNonKeyFields() {
    assertEquals(
        Arrays.asList("locations", "titles"),
        fields(marko, field -> !Keys.isKey(field)).stream()
            .map(Field::getName).collect(Collectors.toList()));
  }

  @Test
  public void testGetDefaultValue() {
    DefaultValues defaultValues = new DefaultValues();
    Function<String, Object> valueOf = fieldName ->
        propertyValue(field(defaultValues, fieldName), defaultValues);
    assertEquals("default", valueOf.apply("stringField"));
    assertEquals(10, valueOf.apply("intField"));
    assertEquals(20L, valueOf.apply("longField"));
    assertEquals(30.2D, valueOf.apply("doubleField"));
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  private static class DefaultValues extends Vertex {

    @DefaultValue("default")
    private String stringField;
    @DefaultValue("10")
    private int intField;
    @DefaultValue("20")
    private Long longField;
    @DefaultValue("30.2")
    private Double doubleField;
  }
}
