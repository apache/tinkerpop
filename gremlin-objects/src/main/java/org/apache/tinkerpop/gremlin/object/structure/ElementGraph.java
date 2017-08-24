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

import org.apache.tinkerpop.gremlin.object.reflect.Keys;
import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.apache.tinkerpop.gremlin.object.reflect.UpdateBy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isCollection;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isList;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isSet;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.list;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.values;
import static org.apache.tinkerpop.gremlin.object.reflect.UpdateBy.Update;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.REPLACE;
import static org.apache.tinkerpop.gremlin.object.structure.HasFeature.supportsGraphAdd;
import static org.apache.tinkerpop.gremlin.object.structure.HasFeature.supportsUserSuppliedIds;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

/**
 * The {@link ElementGraph} provides ways to find and update gremlin {@link
 * org.apache.tinkerpop.gremlin.structure.Element}s.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@AllArgsConstructor
@SuppressWarnings({"rawtypes", "PMD.TooManyStaticImports"})
public class ElementGraph {

  protected Graph graph;
  protected GraphTraversalSource g;

  /**
   * What should we do if the element we are trying to add already exists?
   */
  protected Graph.Should should() {
    return graph.should();
  }

  /**
   * Does the gremlin graph API support add elements (vertices/edges)?
   */
  protected <E extends Element> boolean useGraph(E element) {
    return graph.verify(supportsGraphAdd(element));
  }

  /**
   * Find the given element using it's id, if it has one, or all of it's properties.
   */
  protected <E extends Element> GraphTraversal find(E element) {
    GraphTraversal traversal = g.V();
    if (element.id() != null) {
      traversal = traversal.hasId(element.id());
    } else {
      traversal = traversal.hasLabel(element.label());
      Object[] properties = Properties.id(element);
      if (properties == null || properties.length == 0) {
        properties = Properties.all(element);
      }
      for (Property property : list(properties)) {
        traversal = traversal.has(property.key(), property.value());
      }
    }
    return traversal;
  }

  /**
   * Update the element, using a traversal, and the properties provided by the lister.
   */
  protected <E extends Element> GraphTraversal update(
      GraphTraversal traversal, E element, Function<E, Object[]> lister) {
    return update(traversal, UpdateBy.TRAVERSAL, element, lister);
  }

  /**
   * Update the attached element, using it's own methods, and the listed properties.
   */
  protected <E extends Element> org.apache.tinkerpop.gremlin.structure.Element update(
      org.apache.tinkerpop.gremlin.structure.Element delegate, E element,
      Function<E, Object[]> lister) {
    return update(delegate, UpdateBy.ELEMENT, element, lister);
  }

  /**
   * Generate the list of {@link Update}s corresponding to the listed properties. Then, apply then
   * against either the given {@link GraphTraversal} or {@link Element}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected <E extends Element, M> M update(M updatable, UpdateBy updateBy, E element,
      Function<E, Object[]> lister) {
    Object[] properties = lister.apply(element);
    updates(properties).forEach(
        mutation -> updateBy.updater().property(updatable, mutation));
    return updatable;
  }

  /**
   * For single-valued properties, create a <key, value> update. For multi- or vertex-properties
   * with meta-properties, create a <key, value, values> update.
   */
  protected List<Update> updates(Object... properties) {
    List<Update> updates = new ArrayList<>();
    for (Property property : list(properties)) {
      String key = property.key();
      Object value = property.value();
      Class<?> valueClass = value.getClass();
      if (isCollection(valueClass)) {
        Collection collection = (Collection) value;
        Cardinality cardinality = null;
        for (Object item : collection) {
          cardinality = collectionCardinality(valueClass, cardinality);
          append(updates, cardinality, key, item);
        }
      } else {
        append(updates, VertexProperty.Cardinality.single, key, value);
      }
    }
    return updates;
  }

  /**
   * What {@link Cardinality} should we use for the given type of value, depending on what the
   * previous update's {@link Cardinality}, and what {@link #should()}  is.
   *
   * In particular, if we're in the replace mode, we want the first update on a given list or set to
   * have a {@link Cardinality#single} value.
   */
  @SneakyThrows
  private Cardinality collectionCardinality(Class<?> valueType, Cardinality lastOne) {
    if (lastOne == null) {
      if (should().equals(REPLACE)) {
        return Cardinality.single;
      }
    } else {
      if (!Cardinality.single.equals(lastOne)) {
        return lastOne;
      }
    }
    if (isList(valueType)) {
      return Cardinality.list;
    }
    if (isSet(valueType)) {
      return Cardinality.set;
    }
    throw Element.Exceptions.invalidCollectionType(valueType);
  }

  /**
   * Append an update with key, value and possibly an array of values, depending on whether it's a
   * single-, multi-, or complex-property (that has meta-properties).
   */
  protected void append(List<Update> updates, Cardinality cardinality,
      String key, Object value) {
    List<Object> values = values(value);
    switch (values.size()) {
      case 0:
        break;
      case 1:
        // this is a single valued property.
        updates.add(Update.of(key, values.get(0)));
        break;
      default:
        // this is either a multi- or meta-property
        updates.add(Update.of(cardinality, key, values.remove(0),
            values.toArray(new Object[] {})));
        break;
    }
  }

  /**
   * If the underlying graph supports user supplied ids, supply one.
   */
  protected Object maybeSupplyId(Element element) {
    if (!graph.verify(supportsUserSuppliedIds(element))) {
      return null;
    }
    element.setUserSuppliedId(Keys.id(element));
    return element.id();
  }

  /**
   * Force the traversal to fail.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected GraphTraversal fail(GraphTraversal traversal) {
    traversal.choose(__.value());
    return traversal;
  }

  /**
   * Complete the traversal by returning the next (and only) element.
   */
  protected <E> E complete(GraphTraversal<?, E> traversal) {
    return traversal.next();
  }
}
