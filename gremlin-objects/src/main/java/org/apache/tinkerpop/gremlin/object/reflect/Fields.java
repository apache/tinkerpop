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
import org.apache.tinkerpop.gremlin.object.model.Hidden;
import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.structure.Element;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.lang.reflect.Modifier.isStatic;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.reflect.FieldUtils.getAllFields;
import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isFunctional;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isList;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.isKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.isOrderingKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.isPrimaryKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.asPrimitiveType;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isPrimitive;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isPrimitiveDefault;

/**
 * {@link Fields} defines ways to get the key and value of a field that represents a property,
 * retrieve fields for certain kinds of properties, and see if it has object specific annotations.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@SuppressWarnings({"unchecked", "rawtypes", "PMD.TooManyStaticImports"})
public final class Fields {

  // How many element types do we want to cache?
  public static long elementCacheSize = 10L;

  // A LRU cache of element types to their fields.
  private static final Map<Pair<Class, Predicate<Field>>, Map<String, Field>> ELEMENT_CACHE =
      new LinkedHashMap() {
        public static final long serialVersionUID = 1L;

        @Override
        public Object get(Object key) {
          return elementCacheSize > 0L ? super.get(key) : null;
        }

        @Override
        public Object computeIfAbsent(Object key, Function mappingFunction) {
          return elementCacheSize > 0L ? super.computeIfAbsent(key, mappingFunction)
              : mappingFunction.apply(key);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return super.size() > elementCacheSize;
        }
      };

  public static final Comparator<Field> BY_NAME = (left, right) ->
      left.getName().compareTo(right.getName());

  /**
   * A comparator, that considers the primary keys first, ordering keys next, and then the rest.
   */
  public static final Comparator<Field> BY_KEYS = (left, right) -> {
    if (isPrimaryKey(left)) {
      if (isPrimaryKey(right)) {
        return BY_NAME.compare(left, right);
      } else {
        return -1;
      }
    } else if (isOrderingKey(left)) {
      if (isPrimaryKey(right)) {
        return 1;
      } else if (isOrderingKey(right)) {
        return BY_NAME.compare(left, right);
      } else {
        return -1;
      }
    } else {
      if (isKey(right)) {
        return 1;
      } else {
        return BY_NAME.compare(left, right);
      }
    }
  };

  private static final Set<String> SYSTEM_PROPERTIES = new HashSet<>();
  private static final Set<Class> SYSTEM_CLASSES = new HashSet<>();

  static {
    SYSTEM_PROPERTIES.add(T.id.getAccessor());
    SYSTEM_PROPERTIES.add(T.label.getAccessor());

    SYSTEM_CLASSES.add(Element.class);
    SYSTEM_CLASSES.add(Vertex.class);
    SYSTEM_CLASSES.add(Edge.class);
  }

  private Fields() {}

  /**
   * Does the field have the given annotation?
   */
  public static boolean has(Field field, Class<? extends Annotation> annotationType) {
    return field.getAnnotation(annotationType) != null;
  }

  /**
   * If the given field or class have an {@link Alias}, what is it's value?
   */
  public static String alias(AnnotatedElement element, Function<Alias, String> property) {
    Alias alias = element.getAnnotation(Alias.class);
    if (alias == null) {
      return null;
    }
    String value = property.apply(alias);
    return isNotEmpty(value) ? value : null;
  }

  /**
   * What is the property key corresponding to this field?
   */
  public static String propertyKey(Field field) {
    String key = alias(field, Alias::key);
    return key != null ? key : field.getName();
  }

  /**
   * What is the property value corresponding to this field?
   */
  @SneakyThrows
  public static Object propertyValue(Field field, Object instance) {
    Object value = field.get(instance);
    if (isPrimitiveDefault(field, value)) {
      DefaultValue defaultValue = field.getAnnotation(DefaultValue.class);
      if (defaultValue != null) {
        return asPrimitiveType(field, defaultValue.value());
      }
    }
    return value;
  }

  /**
   * Find the field of the given name in the given element.
   */
  public static <E extends Element> Field field(E element, String fieldName) {
    return field(element.getClass(), fieldName);
  }

  /**
   * Find the field of the given name from the given class.
   */

  public static <E extends Element> Field field(Class<E> elementType, String fieldName) {
    Field matchingField = null;
    Map<String, Field> fieldCache = ELEMENT_CACHE.get(elementType);
    if (fieldCache != null) {
      matchingField = fieldCache.get(fieldName);
    }
    if (matchingField == null) {
      matchingField = getField(elementType, fieldName, true);
    }
    return matchingField;
  }

  /**
   * Find the fields of the element.
   */
  public static <E extends Element> List<Field> fields(E element) {
    return fields(element.getClass());
  }

  /**
   * Find the fields of the element that satisfy the given predicate.
   */
  public static <E extends Element> List<Field> fields(E element, Predicate<Field> predicate) {
    return fields(element.getClass(), predicate);
  }

  /**
   * Find the fields of the given element class.
   */
  public static <E extends Element> List<Field> fields(Class<E> elementType) {
    return fields(elementType, field -> true);
  }

  private static <E extends Element> Map<String, Field> fieldsByName(Pair<Class, Predicate<Field>>
      predicatedType) {
    Class<E> elementType = predicatedType.getValue0();
    Predicate<Field> predicate = predicatedType.getValue1();
    List<Field> allFields = Arrays.asList(getAllFields(elementType));
    allFields.forEach(field -> {
      field.setAccessible(true);
    });
    Map<String, Field> fieldMap = allFields.stream()
        .filter(field -> predicate.test(field) && !isHiddenField(field))
        .collect(Collectors.toMap(Field::getName, Function.identity()));
    return fieldMap;
  }

  /**
   * Find the fields of the given element type, and field predicate.
   */
  public static <E extends Element> List<Field> fields(Class<E> elementType,
      Predicate<Field> predicate) {
    Pair<Class, Predicate<Field>> predicatedType = Pair.with(elementType, predicate);
    Map<String, Field> cachedFields =
        ELEMENT_CACHE.computeIfAbsent(
            predicatedType, Fields::fieldsByName);
    List<Field> matchingFields = cachedFields.values().stream()
        .collect(Collectors.toList());
    Collections.sort(matchingFields, BY_KEYS);
    return matchingFields;
  }

  /**
   * Get the type of the list field.
   */
  public static Class<?> listType(Field field) {
    return (Class) ((ParameterizedType) field.getAnnotatedType().getType())
        .getActualTypeArguments()[0];
  }

  /**
   * Does this field to a simple property with no meta-properties?
   */
  public static boolean isSimple(Field field) {
    return isPrimitive(field) || (isList(field) && isPrimitive(listType(field)));
  }

  /**
   * Is this field to be hidden from the graph?
   */
  private static boolean isHiddenField(Field field) {
    if (field.isSynthetic() || has(field, Hidden.class) || isStatic(field.getModifiers())) {
      return true;
    }
    return SYSTEM_PROPERTIES.contains(propertyKey(field)) ||
        SYSTEM_CLASSES.contains(field.getDeclaringClass()) ||
        isFunctional(field.getType());
  }
}
