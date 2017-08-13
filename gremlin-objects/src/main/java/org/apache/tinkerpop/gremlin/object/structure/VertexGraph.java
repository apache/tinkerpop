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

import org.apache.tinkerpop.gremlin.object.reflect.Parser;
import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.apache.tinkerpop.gremlin.object.reflect.Keys.hasPrimaryKeys;
import static org.apache.tinkerpop.gremlin.object.reflect.Properties.simple;

/**
 * The {@link VertexGraph} performs add and remove vertex operations on the underlying {@link
 * GraphTraversal} or {@link org.apache.tinkerpop.gremlin.structure.Graph}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@SuppressWarnings({"rawtypes", "unchecked"})
public class VertexGraph extends ElementGraph {

  @Getter
  private Vertex vertex;
  private Graph.O observers = Graph.O.get();
  private Map<Object, org.apache.tinkerpop.gremlin.structure.Vertex> delegates = new HashMap<>();

  public VertexGraph(Graph graph, GraphTraversalSource g) {
    super(graph, g);
  }

  public <V extends Vertex> org.apache.tinkerpop.gremlin.structure.Vertex delegate(V vertex) {
    return vertex.id() != null ? delegates.get(vertex.id()) : null;
  }

  /**
   * Add the given vertex.
   *
   * Based on the value of {@link #should()}, it will either:
   * <p/>
   * <ul>
   *   <li>Create the vertex with no checks.</li>
   *   <li>Merge the vertex with any existing one.</li>
   *   <li>Replace an existing vertex, if present, else insert.</li>
   *   <li>Insert the vertex, only if it doesn't exist.</li>
   *   <li>Ignore the addition if it already exists.</li>
   * </ul>
   */
  public <V extends Vertex> Vertex addVertex(V vertex) {
    vertex.validate();
    org.apache.tinkerpop.gremlin.structure.Vertex delegate;
    switch (should()) {
      case CREATE:
        delegate = complete(create(vertex));
        break;
      case MERGE:
        delegate = complete(g.inject(1).coalesce(
            update(vertex), create(vertex)));
        break;
      case REPLACE:
        delegate = complete(g.inject(1).coalesce(
            clear(vertex), update(vertex), create(vertex)));
        break;
      case INSERT:
        delegate = insert(vertex);
        break;
      case IGNORE:
      default:
        delegate = complete(g.inject(1).coalesce(find(vertex), create(vertex)));
        break;
    }
    vertex.setDelegate(delegate);
    vertex.setUserSuppliedId(delegate.id());
    delegates.put(vertex.id(), delegate);
    this.vertex = Parser.as(delegate, vertex.getClass());
    observers.notifyAll(observer -> observer.vertexAdded(vertex, this.vertex));
    return this.vertex;
  }

  /**
   * Remove the given vertex.
   */
  public <V extends Vertex> Vertex removeVertex(V vertex) {
    GraphTraversal traversal = find(vertex);
    log.info("Executing 'remove vertex' traversal {} ", traversal);
    traversal.drop().toList();
    if (vertex.id() != null) {
      delegates.remove(vertex);
    }
    observers.notifyAll(observer -> observer.vertexRemoved(vertex));
    return vertex;
  }

  protected <V extends Vertex> GraphTraversal clear(V vertex) {
    GraphTraversal traversal = find(vertex);
    List<String> clearKeys = Properties.nullKeys(vertex);
    clearKeys.addAll(Properties.listKeys(vertex));
    traversal.properties(clearKeys.toArray(new String[] {})).drop();
    return traversal;
  }

  protected <V extends Vertex> GraphTraversal update(V vertex) {
    GraphTraversal traversal = find(vertex);
    return update(traversal, vertex, Properties::of);
  }

  protected <V extends Vertex> GraphTraversal check(V vertex) {
    return fail(find(vertex));
  }

  protected <V extends Vertex> GraphTraversal create(V vertex) {
    GraphTraversal traversal = g
        .addV(vertex.label());
    Object vertexId = maybeSupplyId(vertex);
    if (vertexId != null) {
      traversal.property(T.id, vertexId);
    }
    return update(traversal, vertex, Properties::all);
  }

  protected org.apache.tinkerpop.gremlin.structure.Vertex insert(Vertex vertex) {
    try {
      org.apache.tinkerpop.gremlin.structure.Vertex delegate;
      if (useGraph(vertex)) {
        Object vertexId = maybeSupplyId(vertex);
        delegate = g.getGraph().addVertex(
            vertexId != null ? simple(vertex, T.id, T.label) : simple(vertex, T.label));
        update(delegate, vertex, Properties::complex);
      } else {
        delegate = complete(hasPrimaryKeys(vertex) ? g.inject(1).coalesce(
            check(vertex), create(vertex)) : create(vertex));
      }
      return delegate;
    } catch (Exception e) {
      log.warn("The vertex {} already exists", vertex);
      throw Element.Exceptions.elementAlreadyExists(vertex);
    }
  }

  @Override
  protected org.apache.tinkerpop.gremlin.structure.Vertex complete(GraphTraversal traversal) {
    log.info("Executing '{} vertex' traversal {} ", should().label(), traversal);
    return (org.apache.tinkerpop.gremlin.structure.Vertex) super.complete(traversal);
  }

  public void reset() {
    this.delegates.clear();
    this.vertex = null;
  }

}
