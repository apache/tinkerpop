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
package org.apache.tinkerpop.gremlin.object.structure;

import org.apache.tinkerpop.gremlin.object.model.Alias;
import org.apache.tinkerpop.gremlin.object.reflect.Keys;
import org.apache.tinkerpop.gremlin.object.reflect.Label;
import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.apache.tinkerpop.gremlin.object.traversal.ElementTo;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;
import org.apache.tinkerpop.gremlin.object.traversal.library.Has;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.name;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.elementCacheSize;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.field;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.fields;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.keyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.primaryKeyFields;

/**
 * The element that {@code gremlin-core} uses to represent a vertex and edge is much like a property
 * map, which can be hard to read and write. The {@link Element} class allows for representations of
 * the underlying  element as first-class objects, whose fields correspond to its properties.
 *
 * <p>
 * Both the {@link Vertex} and {@link Edge} classes inherit from the {@link Element} class. It
 * contains sub-traversals such as {@link #withLabel} and {@link #s}, that are applicable to all
 * elements. In addition, it implements the {@link Comparable} interface, ordering elements by their
 * {@code PrimaryKey}s and {@code OrderingKey}s first.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@ToString
@NoArgsConstructor
@SuppressWarnings({"rawtypes", "PMD.TooManyStaticImports"})
public class Element implements Comparable<Element> {

  public static final long serialVersionUID = 1L;
  public final ElementTo.Element withLabel = traversal -> traversal.hasLabel(label());
  @Setter
  protected org.apache.tinkerpop.gremlin.structure.Element delegate;
  @Getter
  @Setter
  @Alias(key = "id")
  private Object userSuppliedId;
  public final ElementTo.Element withId = traversal -> traversal.hasId(id());

  public Element(Element other) {
    this.setUserSuppliedId(other.id());
  }

  /**
   * Compose the given {@link SubTraversal} in the given order.
   */
  @SuppressWarnings("unchecked")
  protected static <E> ElementTo<E> compose(SubTraversal<?, ?>... subTraversals) {
    return traversal -> {
      for (SubTraversal subTraversal : subTraversals) {
        traversal = (GraphTraversal<
            org.apache.tinkerpop.gremlin.structure.Element,
            org.apache.tinkerpop.gremlin.structure.Element>) subTraversal.apply(traversal);
      }
      return (GraphTraversal) traversal;
    };
  }

  /**
   * This is final since you can override the label through the {@link Alias} annotation
   */
  public final String label() {
    return Label.of(getClass());
  }

  /**
   * If the graph supports user supplied ids, then we will derive the id based on the primary and
   * ordering key fields of the element, ergo this is final.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public final Object id() {
    if (userSuppliedId != null) {
      return userSuppliedId;
    }
    if (delegate != null) {
      return delegate.id();
    }
    return null;
  }

  /**
   * Generate a {@link SubTraversal} that matches the value of the given key in this element.
   */
  @SneakyThrows
  @SuppressWarnings({"PMD.ShortMethodName"})
  public ElementTo.Element s(String key) {
    Field propertyField = field(getClass(), key);
    return Has.of(label(), propertyKey(propertyField), propertyValue(propertyField, this));
  }

  /**
   * @throws {@link Exceptions#requiredKeysMissing(Class, String)} if a key is missing
   */
  public void validate() {
    Keys.id(this);
  }

  /**
   * Changes the maximum number of elements for which reflection metadata will be cached. If the
   * limit is hit, the least recently used element is kicked out.
   */
  public static void cache(long numberOfElements) {
    elementCacheSize = numberOfElements;
  }

  /**
   * Classes that extend the {@link Element} should use the {@code @EqualsAndHashCode(of={},
   * callSuper=true)} annotation, so that we can ignore any fields marked as {@code Hidden}.
   *
   * By including {@code of={}} in that annotation, the derived class essentially excludes all of
   * it's fields, so as to avoid double-checking each field. If {@code of={}} is not present, and if
   * the derived class contains any instance-specific {@link SubTraversal}s or {@code
   * AnyTraversal}s, then they will need to either be marked as {@code transient} or start with a
   * {@code $} symbol, so that the {@code lombok} processor can ignore those lambda objects.
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Element)) {
      return false;
    }
    Element that = (Element) other;
    if (this.id() != null && that.id() != null && !this.id().equals(that.id())) {
      return false;
    }
    return compareTo(that) == 0;
  }

  @Override
  @SneakyThrows
  public int hashCode() {
    List<Field> primaryKeyFields = primaryKeyFields(getClass());
    int hashCode = 0;
    for (Field primaryKeyField : primaryKeyFields) {
      Object value = primaryKeyField.get(this);
      if (value == null) {
        continue;
      }
      hashCode += value.hashCode();
    }
    return hashCode;
  }

  /**
   * Compare against the given element, using the values of the fields, in {@link
   * org.apache.tinkerpop.gremlin.object.reflect.Fields#BY_KEYS} order.
   */
  @Override
  public int compareTo(Element that) {
    return valuesOf(fields(getClass())).compare(this, that);
  }

  /**
   * Check if this element exists in the given collection. If it defines {@code PrimaryKey}s or
   * {@code OrderingKey}s, then those are compared. If not, then all fields are considered.
   */
  public boolean existsIn(Collection<? extends Element> elements) {
    final List<Field> fields = keyFields(getClass());
    if (fields.isEmpty()) {
      fields.addAll(fields(getClass()));
    }
    return elements.stream()
        .anyMatch(element -> valuesOf(fields).compare(element, this) == 0);
  }

  @SuppressWarnings("unchecked")
  public static final Comparator<Element> valuesOf(List<Field> fields) {
    return (left, right) -> {
      if (!left.getClass().equals(right.getClass())) {
        return -1;
      }
      for (Field field : fields) {
        try {
          Object thisValue = field.get(left);
          Object thatValue = field.get(right);

          if (thisValue == null) {
            if (thatValue != null) {
              return -1;
            }
          } else {
            if (thatValue == null) {
              return 1;
            }
            int difference;
            if (thisValue instanceof Comparable) {
              difference = ((Comparable) thisValue).compareTo(thatValue);
            } else {
              difference = thisValue.hashCode() - thatValue.hashCode();
            }
            if (difference != 0) {
              return difference;
            }
          }
        } catch (IllegalAccessException iae) {
          return -1;
        }
      }
      return 0;
    };
  }

  public static class Exceptions {

    protected Exceptions() {
      // The protected access modifier allows for sub-classing.
    }

    public static IllegalStateException elementAlreadyExists(Element element) {
      return new IllegalStateException(
          String.format("The element '%s' with the id '%s' already exists",
              element.label(), Properties.id(element)));
    }

    public static IllegalStateException removingDetachedElement(Element element) {
      return new IllegalStateException(
          String.format("Cannot remove detached element: %s", element));
    }

    public static IllegalArgumentException requiredKeysMissing(Class<? extends Element> elementType,
        String fieldName) {
      return new IllegalArgumentException(
          String.format("The mandatory key '%s' for element '%s' is null",
              fieldName, name(elementType)));
    }

    public static IllegalArgumentException invalidAnnotationType(Class<?> type) {
      return new IllegalArgumentException(
          String.format("The annotation type '%s' is not supported", name(type)));
    }

    public static InstantiationException invalidCollectionType(Class<?> type) {
      return new InstantiationException(
          String.format("The collection type '%s' is not supported", name(type)));
    }

    public static InstantiationException unknownElementType(Class<?> type) {
      return new InstantiationException(
          String.format("The collection type '%s' is not supported", name(type)));
    }

    public static ClassCastException invalidTimeType(Class targetClass, Object timeValue) {
      return new ClassCastException(
          String.format("The time value '%s' is not of the desired '%s' type", targetClass,
              timeValue));
    }
  }
}
