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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

/**
 * The {@link UpdateBy} class updates the property of an {@link Element}, using either the {@link
 * Element#property(String, Object)} methods or the {@link GraphTraversal#property} methods.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings({"rawtypes"})
public enum UpdateBy {
  /**
   * Defines an {@link Updater} that relies on the {@link Element#property} method.
   */
  ELEMENT(new ElementBasedUpdater()),
  /**
   * Defines an {@link Updater} that relies on the {@link GraphTraversal#property} method.
   */
  TRAVERSAL(new TraversalBasedUpdater());

  private Updater updater;

  UpdateBy(Updater updater) {
    this.updater = updater;
  }

  public Updater updater() {
    return updater;
  }

  /**
   * A function that given a property update, applies it on the given updatable object.
   */
  @FunctionalInterface
  @SuppressWarnings("PMD.UnusedModifier")
  public interface Updater<U> {

    U property(U updatable, Update update);
  }

  /**
   * An abstraction that captures an update on a property.
   */
  @Data
  @AllArgsConstructor
  @RequiredArgsConstructor(staticName = "of")
  public static class Update {

    private final String key;
    private final Object value;
    private Cardinality cardinality;
    private Object[] keyValues;

    @SuppressWarnings("PMD.ShortMethodName")
    public static Update of(Cardinality cardinality, String key, Object value,
        Object... keyValues) {
      return new Update(key, value, cardinality, keyValues);
    }
  }

  /**
   * A property updater that goes through the {@link Element#property} methods.
   */
  static class ElementBasedUpdater implements Updater<Element> {

    @Override
    public Element property(Element element, Update update) {
      if (update.getKeyValues() != null) {
        ((Vertex) element).property(update.getCardinality(),
            update.getKey(),
            update.getValue(),
            update.getKeyValues());
      } else {
        element.property(update.getKey(), update.getValue());
      }
      return element;
    }
  }

  /**
   * A property updater that goes through the {@link GraphTraversal#property} methods.
   */
  static class TraversalBasedUpdater implements Updater<GraphTraversal> {

    @Override
    public GraphTraversal property(GraphTraversal traversal, Update update) {
      if (update.getCardinality() != null) {
        traversal.property(update.getCardinality(),
            update.getKey(),
            update.getValue(),
            update.getKeyValues());
      } else {
        traversal.property(update.getKey(),
            update.getValue());
      }
      return traversal;
    }
  }
}
