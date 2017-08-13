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

import org.apache.tinkerpop.gremlin.object.provider.GraphSystem;
import org.apache.tinkerpop.gremlin.object.reflect.Primitives;
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.CREATE;

/**
 * The {@link ObjectGraph} implements the {@link Graph} interface using the provided {@link
 * GraphSystem} and {@link Query} instances.
 *
 * <p>
 * It delegates the {@link #addVertex} and {@link #removeVertex} methods to the {@link VertexGraph}
 * class. Similarly, it passes on the {@link #addEdge} and {@link #removeEdge} methods to the {@link
 * EdgeGraph}. Both of those delegates extend the {@link ElementGraph}, which provides ways to find
 * and update elements of any kind.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@SuppressWarnings({"unchecked", "rawtypes", "PMD.TooManyStaticImports"})
public abstract class ObjectGraph implements Graph {

  @SuppressWarnings({"PMD.AvoidFieldNameMatchingMethodName"})
  protected final GraphSystem system;
  @SuppressWarnings({"PMD.AvoidFieldNameMatchingMethodName"})
  protected final Query query;
  protected GraphTraversalSource g;

  @SuppressWarnings({"PMD.AvoidFieldNameMatchingMethodName"})
  /**
   * How should the object graph handle "merge conflicts"?
   */
  protected Should should;
  /**
   * An element graph that handles vertex operations.
   */
  protected VertexGraph vertexGraph;
  /*
   * An element graph that handles edge operations.
   */
  protected EdgeGraph edgeGraph;

  /**
   * The last element added to the object graph
   */
  protected Element object;
  /**
   * The objects that have been aliased so far in this graph.
   */
  protected Map<String, Element> aliases = new HashMap<>();
  /**
   * A feature verified based on this graph's {@link #g}.
   */
  protected HasFeature.Verifier verifier;

  public ObjectGraph(GraphSystem system, Query query) {
    this.system = system;
    this.query = query;
    should = CREATE;
    g = system.g();
    verifier = HasFeature.Verifier.of(g);
    vertexGraph = new VertexGraph(this, g);
    edgeGraph = new EdgeGraph(this, query, g);
  }

  @Override
  public <V extends Vertex> ObjectGraph addVertex(V vertex) {
    this.object = vertexGraph.addVertex(vertex);
    return this;
  }

  @Override
  public <V extends Vertex> ObjectGraph removeVertex(V vertex) {
    vertexGraph.removeVertex(vertex);
    return this;
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge, String toVertexAlias) {
    addEdge(edge, (V) aliases.get(toVertexAlias));
    return this;
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge,
      String fromVertexAlias,
      String toVertexAlias) {
    return addEdge(edge, (V) aliases.get(fromVertexAlias), (V) aliases.get(toVertexAlias));
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge, Vertex fromVertex,
      String toVertexAlias) {
    return addEdge(edge, fromVertex, (V) aliases.get(toVertexAlias));
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge, String fromVertexAlias,
      V toVertex) {
    return addEdge(edge, (V) aliases.get(fromVertexAlias), toVertex);
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge, V toVertex) {
    return addEdge(edge, vertexGraph.getVertex(), toVertex);
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge, V fromVertex, V toVertex) {
    if (!edge.connects(fromVertex, toVertex)) {
      throw Edge.Exceptions.invalidEdgeConnection(edge);
    }
    edge.setFrom(fromVertex);
    edge.setTo(toVertex);
    this.object = edgeGraph.addEdge(
        edge, vertexGraph.delegate(fromVertex), vertexGraph.delegate(toVertex));
    return this;
  }

  @Override
  public <E extends Edge, V extends Vertex> Graph addEdge(E edge, Vertex fromVertex,
      AnyTraversal toAnyTraversal) {
    edge.setFrom(fromVertex);
    this.object = edgeGraph.addEdge(edge, vertexGraph.delegate(fromVertex), toAnyTraversal);
    return this;
  }

  @Override
  public <E extends Edge> Graph addEdge(E edge, AnyTraversal toAnyTraversal) {
    return addEdge(edge, vertexGraph.getVertex(), toAnyTraversal);
  }

  @Override
  public <E extends Edge> Graph addEdge(E edge, SubTraversal... subTraversals) {
    Vertex fromVertex = vertexGraph.getVertex();
    edge.setFrom(fromVertex);
    this.object = edgeGraph.addEdge(edge, vertexGraph.delegate(fromVertex), subTraversals);
    return this;
  }

  @Override
  public <E extends Edge> Graph removeEdge(E edge) {
    edgeGraph.removeEdge(edge);
    return this;
  }

  @Override
  @SuppressWarnings({"PMD.ShortMethodName"})
  public ObjectGraph as(String alias) {
    this.aliases.put(alias, object);
    return this;
  }

  @Override
  @SuppressWarnings({"PMD.ShortMethodName"})
  public <E extends Element> Graph as(Consumer<E> consumer) {
    consumer.accept((E) object);
    return this;
  }

  @Override
  @SuppressWarnings({"PMD.ShortMethodName"})
  public <E extends Element> E as(Class<E> elementType) {
    return (E) object;
  }

  @Override
  public <E extends Element> E get(String alias, Class<E> elementType) {
    return (E) this.aliases.get(alias);
  }

  @Override
  public Graph should(Should should) {
    this.should = should;
    return this;
  }

  @Override
  public Should should() {
    return this.should;
  }


  @Override
  public Graph defaultKeys(boolean allow) {
    Primitives.allowDefaultKeys = allow;
    return this;
  }

  @Override
  public boolean verify(HasFeature hasFeature) {
    return verifier.verify(hasFeature);
  }

  @Override
  public Query query() {
    return query;
  }

  @Override
  public GraphSystem system() {
    return system;
  }

  @Override
  public void drop() {
    query.by(g -> g.V().drop()).none();
  }

  @Override
  public void reset() {
    aliases.clear();
    vertexGraph.reset();
    edgeGraph.reset();
    object = null;
  }

  @Override
  public void close() {
    if (system != null) {
      system.close();
    }
    if (query != null) {
      query.close();
    }
  }
}
