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
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * The {@link EdgeGraph} performs add and remove edge operations on the underlying {@link
 * GraphTraversal} or {@link org.apache.tinkerpop.gremlin.structure.Graph}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@SuppressWarnings("rawtypes")
public class EdgeGraph extends ElementGraph {

  private final Query query;
  private Graph.O observers = Graph.O.get();

  private Edge edge;
  private Map<Object, org.apache.tinkerpop.gremlin.structure.Edge> delegates = new HashMap<>();

  public EdgeGraph(Graph graph, Query query, GraphTraversalSource g) {
    super(graph, g);
    this.query = query;
  }

  public org.apache.tinkerpop.gremlin.structure.Edge delegate(Edge edge) {
    return edge.id() != null ? this.delegates.get(edge) : null;
  }

  /**
   * Add the given edge, from the given gremlin vertex, to the vertices selected by the given {@link
   * AnyTraversal}.
   */
  public <E extends Edge> Edge addEdge(E edge, org.apache.tinkerpop.gremlin.structure.Vertex from,
      AnyTraversal anyTraversal) {
    if (from == null) {
      throw Edge.Exceptions.missingEdgeVertex(Direction.OUT, edge, from);
    }
    List<Vertex> tos = query.by(anyTraversal).list(Vertex.class);
    if (tos == null || tos.isEmpty()) {
      throw Edge.Exceptions.missingEdgeVertex(Direction.IN, edge, anyTraversal);
    }
    tos.forEach(to -> {
      edge.setTo(to);
      addEdge(edge, from, to.delegate());
    });
    return edge;
  }

  /**
   * Add the given edge, from the given gremlin vertex, to the vertices selected by the given {@link
   * SubTraversal}s.
   */
  public <E extends Edge> Edge addEdge(E edge, org.apache.tinkerpop.gremlin.structure.Vertex from,
      SubTraversal... subTraversals) {
    if (from == null) {
      throw Edge.Exceptions.missingEdgeVertex(Direction.OUT, edge, from);
    }
    List<Vertex> tos = query.by(subTraversals).list(Vertex.class);
    if (tos == null || tos.isEmpty()) {
      throw Edge.Exceptions.missingEdgeVertex(Direction.IN, edge, subTraversals);
    }
    tos.forEach(to -> {
      edge.setTo(to);
      addEdge(edge, from, to.delegate());
    });
    return edge;
  }

  /**
   * Add the given edge from and to the given gremlin vertices.
   *
   * Based on the value of {@link #should()}, it will either: <ol> <li>Create the edge with no
   * checks.</li> <li>Merge the edge with any existing one.</li> <li>Replace the existing edge, if
   * present, otherwise insert.</li> <li>Insert the edge, only if it doesn't exist.</li> <li>Ignore
   * the addition if one already exists.</li> </ol>
   */
  @SuppressWarnings("unchecked")
  <E extends Edge> Edge addEdge(E edge, org.apache.tinkerpop.gremlin.structure.Vertex fromVertex,
      org.apache.tinkerpop.gremlin.structure.Vertex toVertex) {
    if (fromVertex == null) {
      throw Edge.Exceptions.missingEdgeVertex(Direction.OUT, edge);
    }
    if (toVertex == null) {
      throw Edge.Exceptions.missingEdgeVertex(Direction.IN, edge);
    }
    org.apache.tinkerpop.gremlin.structure.Element delegate;
    Object fromId = fromVertex.id();
    Object toId = toVertex.id();
    switch (should()) {
      case CREATE:
        delegate = complete(create(edge, fromId, toId));
        break;
      case MERGE:
        delegate = complete(g.inject(1).coalesce(
            update(edge, fromId, toId), create(edge, fromId, toId)));
        break;
      case REPLACE:
        delegate = complete(g.inject(1).coalesce(
            clear(edge, fromId, toId), update(edge, fromId, toId), create(edge, fromId, toId)));
        break;
      case INSERT:
        delegate = insert(edge, fromVertex, toVertex);
        break;
      case IGNORE:
      default:
        delegate = complete(g.inject(1).coalesce(
            find(edge, fromId, toId), create(edge, fromId, toId)));
        break;
    }
    edge.setDelegate(delegate);
    edge.setFromId(fromId);
    edge.setToId(toId);
    delegates.put(edge.id(), (org.apache.tinkerpop.gremlin.structure.Edge) delegate);
    this.edge = Parser.as(delegate, edge.getClass());
    this.edge.setFrom(edge.getFrom());
    this.edge.setTo(edge.getTo());
    this.edge.setFromId(edge.getFromId());
    this.edge.setToId(edge.getToId());
    observers.notifyAll(observer -> observer.edgeAdded(edge, this.edge));
    return this.edge;
  }

  /**
   * Remove the given edge.
   */
  public <E extends Edge> Edge removeEdge(E edge) {
    if (edge.getFromId() == null || edge.getToId() == null) {
      throw Element.Exceptions.removingDetachedElement(edge);
    }
    GraphTraversal traversal = find(edge, edge.getFromId(), edge.getToId());
    log.info("Executing 'remove edge' traversal {}", traversal);
    traversal.select("edge").drop().toList();
    if (edge.id() != null) {
      delegates.remove(edge);
    }
    observers.notifyAll(observer -> observer.edgeRemoved(edge));
    return edge;
  }

  <E extends Edge> GraphTraversal clear(E edge, Object fromId, Object toId) {
    GraphTraversal traversal = find(edge, fromId, toId);
    List<String> clearKeys = Properties.nullKeys(edge);
    traversal.properties(clearKeys.toArray(new String[] {})).drop();
    return traversal;
  }

  <E extends Edge> GraphTraversal update(E edge, Object fromId, Object toId) {
    GraphTraversal traversal = find(edge, fromId, toId);
    return update(traversal, edge, Properties::of);
  }

  <E extends Edge> GraphTraversal check(E edge, Object fromId, Object toId) {
    return fail(find(edge, fromId, toId));
  }

  <E extends Edge> GraphTraversal create(E edge, Object fromId, Object toId) {
    GraphTraversal traversal = g
        .V().hasId(fromId).as("from")
        .V().hasId(toId).as("to")
        .addE(edge.label()).as("edge").from("from");
    Object edgeId = maybeSupplyId(edge);
    if (edgeId != null) {
      traversal.property(T.id, edgeId);
    }
    return update(traversal, edge, Properties::all);
  }

  @SuppressWarnings("unchecked")
  protected org.apache.tinkerpop.gremlin.structure.Edge insert(
      Edge edge, org.apache.tinkerpop.gremlin.structure.Vertex fromVertex,
      org.apache.tinkerpop.gremlin.structure.Vertex toVertex) {
    try {
      if (useGraph(edge)) {
        return fromVertex.addEdge(edge.label(), toVertex, Properties.of(edge));
      } else {
        Object fromId = fromVertex.id();
        Object toId = toVertex.id();
        return complete(g.inject(1).coalesce(
            check(edge, fromId, toId), create(edge, fromId, toId)));
      }
    } catch (Exception e) {
      log.warn("An {} edge between these two vertices already exists", edge, fromVertex, toVertex);
      throw Element.Exceptions.elementAlreadyExists(edge);
    }
  }

  <E extends Edge> GraphTraversal find(E edge, Object fromId, Object toId) {
    return g.V().hasId(fromId).as("from").out(edge.label()).hasId(toId).as("to")
        .inE(edge.label()).as("edge");
  }

  @Override
  @SuppressWarnings("unchecked")
  protected org.apache.tinkerpop.gremlin.structure.Edge complete(GraphTraversal traversal) {
    log.info("Executing '{} edge' traversal {} ", should().label(), traversal);
    return (org.apache.tinkerpop.gremlin.structure.Edge) super.complete(traversal);
  }

  public void reset() {
    this.delegates.clear();
    this.edge = null;
  }
}
