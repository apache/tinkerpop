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

import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.structure.Element;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import lombok.SneakyThrows;

/**
 * {@link Classes} helps you introspect classes easily.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public final class Classes {

  private Classes() {}

  /**
   * Does the given class inherits from {@link Element}?
   */
  public static boolean isElement(Class<?> type) {
    return is(type, Element.class);
  }

  /**
   * Is the given object a {@link Vertex}?
   */
  public static boolean isVertex(Object object) {
    return object != null && isVertex(object.getClass());
  }

  /**
   * Is the given class a {@link Vertex}?
   */
  public static boolean isVertex(Class<?> type) {
    return is(type, Vertex.class);
  }

  /**
   * Is the given object an {@link Edge}?
   */
  public static boolean isEdge(Object object) {
    return object != null && isEdge(object.getClass());
  }

  /**
   * Is the given type an {@link Edge}?
   */
  public static boolean isEdge(Class<?> type) {
    return is(type, Edge.class);
  }

  /**
   * Is the given object a {@link List}?
   */
  public static boolean isList(Object object) {
    return object != null && isList(object.getClass());
  }

  /**
   * Is the given class a {@link List}?
   */
  public static boolean isList(Class<?> type) {
    return is(type, List.class);
  }

  /**
   * Is the given object a {@link Set}?
   */
  public static boolean isSet(Object object) {
    return object != null && isSet(object.getClass());
  }

  /**
   * Is the given class a {@link Set}?
   */
  public static boolean isSet(Class<?> type) {
    return is(type, Set.class);
  }

  /**
   * Is the given object a {@link List} or a {@link Set}?
   */
  public static boolean isCollection(Object object) {
    return object != null && isCollection(object.getClass());
  }

  /**
   * Is the given class a {@link List} or a {@link Set}?
   */
  public static boolean isCollection(Class<?> type) {
    return isList(type) || isSet(type);
  }

  /**
   * Is the given class a {@link Function}?
   */
  public static boolean isFunctional(Class<?> type) {
    return is(type, Function.class);
  }

  /**
   * Is the given class that of the given object?
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static boolean is(Class<?> type, Object that) {
    return that != null && is(type, that.getClass());
  }

  /**
   * Is the given object of the given class?
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static boolean is(Object object, Class<?> that) {
    return object != null && is(object.getClass(), that);
  }

  /**
   * Is the type class assignable from that class?
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  public static boolean is(Class<?> type, Class<?> that) {
    return that.isAssignableFrom(type);
  }

  /**
   * Sort the collection, if it's a list.
   */
  @SuppressWarnings("unchecked")
  public static void sortCollection(Class<?> clazz, Collection<?> collection) {
    if (isList(clazz)) {
      Collections.sort((List) collection);
    }
  }

  /**
   * Create a new collection of the given type.
   */
  @SneakyThrows
  @SuppressWarnings("rawtypes")
  public static Collection newCollection(Class<?> clazz) {
    if (isList(clazz)) {
      return new ArrayList<>();
    }
    if (isSet(clazz)) {
      return new HashSet<>();
    }
    throw Element.Exceptions.invalidCollectionType(clazz);
  }

  /**
   * Return the simple name of the given class.
   */
  public static String name(Class<?> type) {
    return type.getSimpleName();
  }
}
