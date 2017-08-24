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

import org.apache.tinkerpop.gremlin.object.model.PropertyValue;
import org.apache.tinkerpop.gremlin.object.structure.Element;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.is;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isCollection;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.newCollection;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.sortCollection;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.fields;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.has;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.listType;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isPrimitive;

/**
 * The {@link Parser}'s job is to take an element returned by a traversal and convert it into its
 * corresponding class' object, through the {@link #as(Object, Class)} method.
 *
 * <p>
 * It specifies a {@link ElementParser} interface, which has similar {@code #as} method. A default
 * implementation of that contract is defined in {@link GremlinElementParser}.
 *
 * <p>
 * Some graph systems may complete a traversal as an iterator of implementation-defined elements. To
 * support such non-standard elements, the graph system may register a custom {@link ElementParser}
 * with this class during it's construction phase.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings({"rawtypes", "unchecked", "PMD.TooManyStaticImports"})
public final class Parser {

  private static final List<ElementParser<?>> ELEMENT_PARSERS = new ArrayList<>();

  static {
    registerElementParser(GremlinElementParser.getInstance());
  }

  private Parser() {}

  /**
   * Register a {@link ElementParser} that takes a element from a result set, and maps it to an
   * object.
   */
  public static void registerElementParser(ElementParser<?> elementParser) {
    ELEMENT_PARSERS.add(elementParser);
  }

  /**
   * Does the given field hold the value of a vertex property, that has meta-properties of it's
   * own?
   */
  public static boolean isPropertyValue(Field field) {
    return has(field, PropertyValue.class);
  }

  @SneakyThrows
  @SuppressWarnings({"PMD.ShortMethodName", "rawtypes"})
  public static <T> T as(Object element, Class<T> objectClass) {
    Class<?> elementType = element.getClass();
    if (is(elementType, objectClass)) {
      return (T) element;
    }
    for (ElementParser elementParser : ELEMENT_PARSERS) {
      if (is(elementType, elementParser.elementType())) {
        return (T) elementParser.as(element, objectClass);
      }
    }
    if (objectClass.isEnum()) {
      return (T) Enum.valueOf((Class<Enum>) objectClass, (String) element);
    }
    throw Query.Exceptions.invalidObjectType(element, objectClass);
  }

  /**
   * The {@link ElementParser} takes an abstract element and constructs an instance of the given
   * object type.
   */
  public interface ElementParser<E> {

    Class<E> elementType();

    @SuppressWarnings("PMD.ShortMethodName")
    <O> O as(E element, Class<O> objectType);
  }

  /**
   * This is an implementation of {@link ElementParser} baaed on the {@link
   * org.apache.tinkerpop.gremlin.structure.Element} type.
   */
  public static class GremlinElementParser
      implements ElementParser<org.apache.tinkerpop.gremlin.structure.Element> {

    private static final GremlinElementParser INSTANCE = new GremlinElementParser();

    public static GremlinElementParser getInstance() {
      return INSTANCE;
    }

    @Override
    public Class<org.apache.tinkerpop.gremlin.structure.Element> elementType() {
      return org.apache.tinkerpop.gremlin.structure.Element.class;
    }

    @SneakyThrows
    @Override
    @SuppressWarnings({"PMD.ShortMethodName"})
    public <T> T as(org.apache.tinkerpop.gremlin.structure.Element element, Class<T> objectType) {
      Element instance = (Element) objectType.newInstance();
      Object elementId = element.id();
      instance.setUserSuppliedId(elementId);
      instance.setDelegate(element);
      for (Field field : fields(instance)) {
        String propertyName = propertyKey(field);
        Class propertyType = field.getType();

        List<Property> properties = properties(element, propertyName);
        if (properties.isEmpty()) {
          continue;
        }
        if (isCollection(propertyType)) {
          Collection collection = (Collection) field.get(instance);
          if (collection == null) {
            collection = newCollection(propertyType);
            field.set(instance, collection);
          }
          for (Property property : properties) {
            Class elementType = listType(field);
            collection.add(value(elementType, property));
          }
          sortCollection(propertyType, collection);
        } else {
          Property property = properties.get(0);
          field.set(instance, value(propertyType, property));
        }
      }
      return (T) instance;
    }

    private List<Property> properties(org.apache.tinkerpop.gremlin.structure.Element element,
        String propertyName) {
      List<Property> properties = new ArrayList<>();
      Object elementId = element.id();
      if (elementId instanceof Map) {
        Object idValue = ((Map) elementId).get(propertyName);
        if (idValue != null) {
          properties.add(new DetachedProperty(propertyName, idValue));
        }
      }
      Iterator<? extends Property> iterator = element.properties(propertyName);
      if (iterator != null) {
        iterator.forEachRemaining(property -> {
          if (property.isPresent()) {
            properties.add(property);
          }
        });
      }
      return properties;
    }

    @SneakyThrows
    private Object value(Class propertyType, Property property) {
      if (isPrimitive(propertyType)) {
        return value(propertyType, property.value());
      }
      Element propertyValue = (Element) propertyType.newInstance();
      VertexProperty vertexProperty = (VertexProperty) property;
      for (Field field : fields(propertyValue)) {
        if (isPropertyValue(field)) {
          Object value = vertexProperty.value();
          if (value instanceof Object[]) {
            Object[] values = (Object[]) value;
            field.set(propertyValue, value(field.getType(), values[0]));
          } else {
            field.set(propertyValue, value(field.getType(), value));
          }
        } else {
          Object value = vertexProperty.value();
          if (value instanceof Object[]) {
            Object[] values = (Object[]) value;
            String fieldName = propertyKey(field);
            for (int i = 1; i < values.length; i = i + 2) {
              if (fieldName.equals(values[i])) {
                field.set(propertyValue, value(field.getType(), values[i + 1]));
              }
            }
          } else {
            Iterator values = vertexProperty.values(propertyKey(field));
            if (values.hasNext()) {
              field.set(propertyValue, value(field.getType(), values.next()));
            }
          }
        }
      }
      return propertyValue;
    }

    private Object value(Class propertyType, Object propertyValue) {
      Object modifiedValue = propertyValue;
      if (Primitives.isTimeType(propertyType)) {
        modifiedValue = Primitives.toTimeType(propertyValue, propertyType);
      } else if (propertyType.isEnum()) {
        modifiedValue = Enum.valueOf((Class<Enum>) propertyType, (String) propertyValue);
      }
      return modifiedValue;
    }
  }
}
