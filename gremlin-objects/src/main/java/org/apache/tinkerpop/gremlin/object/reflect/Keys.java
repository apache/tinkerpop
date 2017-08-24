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

import org.apache.tinkerpop.gremlin.object.model.OrderingKey;
import org.apache.tinkerpop.gremlin.object.model.PrimaryKey;
import org.apache.tinkerpop.gremlin.object.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isVertex;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.fields;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.has;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isMissing;

/**
 * {@link Keys} helps find fields in the object that denote special properties in the underlying
 * graph element, such as those marked as a {@link PrimaryKey} or {@link OrderingKey}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public final class Keys {

  private Keys() {}

  /**
   * Is the given field annotated with either {@link PrimaryKey} or {@link OrderingKey}?
   */
  public static boolean isKey(Field field) {
    return isPrimaryKey(field) || isOrderingKey(field);
  }

  public static boolean isPrimaryKey(Field field) {
    return has(field, PrimaryKey.class);
  }

  public static boolean isOrderingKey(Field field) {
    return has(field, OrderingKey.class);
  }

  /**
   * Find fields annotated with either {@link PrimaryKey} or {@link OrderingKey} in the element.
   */
  public static List<Field> keyFields(Element element) {
    return keyFields(element.getClass());
  }

  /**
   * Find fields annotated with either {@link PrimaryKey} or {@link OrderingKey} in the given type.
   */
  public static List<Field> keyFields(Class<? extends Element> elementType) {
    return fields(elementType).stream().filter(Keys::isKey)
        .collect(Collectors.toList());
  }

  public static List<Field> primaryKeyFields(Element element) {
    return primaryKeyFields(element.getClass());
  }

  public static List<Field> primaryKeyFields(Class<? extends Element> elementType) {
    return fields(elementType).stream().filter(Keys::isPrimaryKey)
        .collect(Collectors.toList());
  }

  public static boolean hasPrimaryKeys(Element element) {
    return hasPrimaryKeys(element.getClass());
  }

  public static boolean hasPrimaryKeys(Class<? extends Element> elementType) {
    return !primaryKeyFields(elementType).isEmpty();
  }

  public static List<Field> orderingKeyFields(Element element) {
    return orderingKeyFields(element.getClass());
  }

  public static List<Field> orderingKeyFields(Class<? extends Element> elementType) {
    return fields(elementType).stream().filter(Keys::isOrderingKey)
        .collect(Collectors.toList());
  }

  /**
   * Get the names of the fields annotated with {@link PrimaryKey} in the given element
   */
  public static List<String> primaryKeyNames(Element element) {
    return primaryKeyNames(element);
  }

  public static List<String> primaryKeyNames(Class<? extends Element> elementType) {
    return primaryKeyFields(elementType).stream()
        .map(Fields::propertyKey)
        .collect(Collectors.toList());
  }

  /**
   * Supply an id for the given element, in terms of a {@link Map} that contains its label, and all
   * of it's {@link PrimaryKey} and {@link OrderingKey}s.
   */
  @SneakyThrows
  @SuppressWarnings("PMD.ShortMethodName")
  public static <E extends Element> Object id(E element) {
    if (element == null) {
      return null;
    }
    Object elementId = element.id();
    if (elementId != null) {
      return elementId;
    }
    Map<String, Object> mappedId = new HashMap<>();
    if (isVertex(element)) {
      mappedId.put(T.label.getAccessor(), element.label());
    }
    Consumer<Field> addKey = field -> {
      Object propertyValue = propertyValue(field, element);
      if (isMissing(propertyValue)) {
        throw Element.Exceptions.requiredKeysMissing(element.getClass(), propertyKey(field));
      }
      mappedId.put(propertyKey(field), propertyValue);
    };
    primaryKeyFields(element).forEach(addKey);
    orderingKeyFields(element).forEach(addKey);
    return mappedId.isEmpty() ? null : mappedId;
  }

}
