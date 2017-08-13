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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.structure.Element;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isElement;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isList;
import static org.apache.tinkerpop.gremlin.object.reflect.Parser.isPropertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.fields;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.isSimple;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.isKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isMissing;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isPrimitive;

/**
 * {@link Properties} help outline the properties of an object element, that match a specific
 * predicate. When creating elements, one is interested in {@link #all} the properties, including
 * the immutable primary and ordering keys. However, when updating elements, one should only change
 * the properties {@link #of} the element that are mutable.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@SuppressWarnings({"rawtypes", "PMD.TooManyStaticImports", "PMD.AvoidDuplicateLiterals"})
public final class Properties {

  private Properties() {}

  /**
   * Get the id of the given element, as a key value list of it's key fields.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static <E extends Element> Object[] id(E element) {
    return of(element, Keys::isKey);
  }

  /**
   * Get the key value array of all fields of the given element.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static <E extends Element> Object[] all(E element) {
    return of(element, field -> true);
  }

  /**
   * Get the key value array of fields of the element that match the given predicate, including the
   * given system properties.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static <E extends Element> Object[] all(E element, Predicate<Field> predicate, T... ts) {
    List<Object> properties = new ArrayList<>();
    for (T t : ts) {
      switch (t) {
        case id:
          if (element.id() != null) {
            properties.add(T.id);
            properties.add(element.id());
          }
          break;
        case label:
          properties.add(T.label);
          properties.add(element.label());
          break;
        default:
          break;
      }
    }
    properties.addAll(some(element, predicate));
    return properties.toArray(new Object[] {});
  }

  /**
   * Get the key value array of non-key fields of the given vertex.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static <V extends Vertex> Object[] of(V vertex) {
    return of(vertex, field -> !isKey(field));
  }

  /**
   * Get the key value array of all fields of the given edge.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static <E extends Edge> Object[] of(E edge) {
    return of(edge, field -> true);
  }

  /**
   * Get the key value array of matching fields of the given element.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static <E extends Element> Object[] of(E element, Predicate<Field> predicate) {
    return some(element, predicate).toArray(new Object[] {});
  }

  /**
   * Get the key value array of simple fields of the given element, including given system ones.
   */
  public static <E extends Element> Object[] simple(E element, T... ts) {
    return all(element, Fields::isSimple, T.id, T.label);
  }

  /**
   * Get the key value array of the complex fields of the given element.
   */
  public static <E extends Element> Object[] complex(E element) {
    return all(element, field -> !isSimple(field));
  }

  /**
   * Given an {@link Element}, {@link #some} filter's its fields using the given predicate, creates
   * a property key and value for each field, flattens them, and returns it as a {@link List}.
   *
   * @throws IllegalArgumentException, if it finds a property that doesn't have a value, and that
   *                                   property corresponds to a {@link org.apache.tinkerpop.gremlin.object.model.PrimaryKey}
   *                                   or {@link org.apache.tinkerpop.gremlin.object.model.OrderingKey}
   *                                   field.
   */
  @SneakyThrows
  public static <E extends Element> List<Object> some(E element, Predicate<Field> predicate) {
    List<Object> properties = new ArrayList<>();
    for (Field field : fields(element, predicate)) {
      Object propertyValue = FieldUtils.readField(field, element);
      if (isMissing(propertyValue)) {
        if (isKey(field)) {
          throw Element.Exceptions.requiredKeysMissing(element.getClass(), propertyKey(field));
        }
        continue;
      }
      String propertyName = propertyKey(field);
      properties.add(propertyName);
      if (isPrimitive(field)) {
        if (field.getType().isEnum()) {
          properties.add(((Enum) propertyValue).name());
        } else {
          properties.add(propertyValue);
        }
      } else {
        properties.add(propertyValue);
      }
    }
    return properties;
  }

  /**
   * Get the names of the keys of the given element.
   */
  public static <E extends Element> List<String> names(E element) {
    return names(element, field -> true);
  }

  /**
   * Get the names of the element matching the given field predicate.
   */
  public static <E extends Element> List<String> names(E element, Predicate<Field> predicate) {
    List<String> names = new ArrayList<>();

    for (Field field : fields(element)) {
      if (!predicate.test(field)) {
        continue;
      }
      names.add(propertyKey(field));
    }
    return names;
  }

  /**
   * Get the list of non-null property key values in the given object.
   */
  @SneakyThrows
  @SuppressWarnings("unchecked")
  public static List<Object> values(Object object) {
    List<Object> properties = new ArrayList<>();
    Class<?> objectClass = object.getClass();
    if (isPrimitive(objectClass)) {
      properties.add(object);
    } else if (isElement(objectClass)) {
      for (Field field : fields((Class<? extends Element>) objectClass)) {
        Object value = propertyValue(field, object);
        if (isMissing(value)) {
          continue;
        }
        String propertyName = propertyKey(field);
        // add property name iff this field is not the meta-properties value field
        if (isPropertyValue(field)) {
          properties.add(0, value);
        } else {
          properties.add(propertyName);
          properties.add(value);
        }
      }
    }
    return properties;
  }

  /**
   * Convert the key value array into a list of {@link Property}s.
   */
  public static List<Property> list(Object... objects) {
    List<Property> properties = new ArrayList<>();
    for (int i = 0; i < objects.length; i = i + 2) {
      String key = (String) objects[i];
      Object value = objects[i + 1];
      properties.add(new DetachedProperty<>(key, value));
    }
    return properties;
  }

  /**
   * Get the keys of the element that have missing values.
   */
  @SneakyThrows
  public static <E extends Element> List<String> nullKeys(E element) {
    List<String> keys = new ArrayList<>();
    for (Field field : fields(element)) {
      Object propertyValue = FieldUtils.readField(field, element);
      if (isMissing(propertyValue)) {
        String propertyName = propertyKey(field);
        keys.add(propertyName);
      }
    }
    return keys;
  }

  /**
   * Get the keys of the element that are lists.
   */
  @SneakyThrows
  public static <E extends Element> List<String> listKeys(E element) {
    List<String> keys = new ArrayList<>();
    for (Field field : fields(element)) {
      Class fieldType = field.getType();
      if (isList(fieldType)) {
        String propertyName = propertyKey(field);
        keys.add(propertyName);
      }
    }
    return keys;
  }
}
