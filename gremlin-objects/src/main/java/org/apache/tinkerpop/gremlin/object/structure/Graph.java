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
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;

import java.util.function.Consumer;

/**
 * In order to write objects to the property graph, you'll need an instance of the {@link Graph}.
 *
 * <p>
 * Typically, you would create it using an implementation-specific graph factory. However, if the
 * implementation supports it, then it may be dependency inject'ed. The default qualifier is
 * provider by a reference {@code TinkerGraph} system.
 *
 * <p>
 * The {@link #addVertex(Vertex)} and {@link #removeVertex(Vertex)} take a user-defined vertex
 * object. Similarly, the {@link #addEdge} methods take a user-defined edge object, and it's
 * connecting vertex objects, specified as-is, or through aliases, or even traversals.
 *
 * <p>
 * Speaking of aliases, one may define it using the {@link #as} methods, which let you capture the
 * last vertex or edge object that was added under the parameter passed to that method. An aliased
 * object may be retrieved later using the {@link #get} method.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public interface Graph {

  /**
   * Add an given {@link Vertex} to the graph.
   */
  <V extends Vertex> Graph addVertex(V vertex);

  /**
   * Remove the given {@link Vertex} from the graph.
   */
  <V extends Vertex> Graph removeVertex(V vertex);

  /**
   * Add an {@link Edge} from the current vertex to the vertex referenced by the given alias.
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, String toVertexAlias);

  /**
   * Add an {@link Edge} from the current vertex to the given {@link Vertex}
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, V toVertex);

  /**
   * Add an {@link Edge} from the given from and to vertices specified by their aliases.
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, String fromVertexAlias, String toVertexAlias);

  /**
   * Add an {@link Edge} between the given vertices.
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, V fromVertex, V toVertex);

  /**
   * Add an {@link Edge} form the aliased vertex to the given vertex.
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, String fromVertexAlias, V toVertex);

  /**
   * Add an {@link Edge} from the given vertex to the aliased vertex.
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, Vertex fromVertex, String toVertexAlias);

  /**
   * Add an {@link Edge} from the given vertex to the vertices returned by the {@link
   * AnyTraversal}.
   */
  <E extends Edge,
      V extends Vertex> Graph addEdge(E edge, Vertex fromVertex, AnyTraversal toAnyTraversal);

  /**
   * Add an {@link Edge} to the vertices returned by the given {@link AnyTraversal}.
   */
  <E extends Edge> Graph addEdge(E edge, AnyTraversal toAnyTraversal);

  /**
   * Add an {@link Edge} to the vertices returned by the given {@link SubTraversal}s.
   */
  @SuppressWarnings("rawtypes")
  <E extends Edge> Graph addEdge(E edge, SubTraversal... toVertexQueries);

  /**
   * Remove the given edge.
   */
  <E extends Edge> Graph removeEdge(E edge);

  /**
   * Get the {@link Element} tied to the given alias.
   *
   * This is a materialization of the element provided to this graph, and may not reflect what's
   * actually in the graph. To see the actual element, use the {@link Query} interface.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  Graph as(String alias);

  /**
   * Supply the {@link Element} tied to the given consumer.
   *
   * This is a materialization of the element provided to this graph, and may not reflect what's
   * actually in the graph. To see the actual element, use the {@link Query} interface.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  <E extends Element> Graph as(Consumer<E> consumer);

  /**
   * Get the last added {@link Element} in the form of the given type.
   *
   * This is a materialization of the element provided to this graph, and may not reflect what's
   * actually in the graph. To see the actual element, use the {@link Query} interface.
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  <E extends Element> E as(Class<E> elementType);

  /**
   * How {@link Should} the {@link Graph} try to add vertices and edges?
   */
  Graph should(Should should);

  /**
   * What is the {@link Should} mode currently set to?
   */
  Should should();

  /**
   * Should primitive required keys be allowed to have their primitive default values?
   */
  Graph defaultKeys(boolean allow);

  /**
   * Verify if the graph has the given feature.
   */
  boolean verify(HasFeature hasFeature);

  /**
   * Get the element of the given type stored against the given alias.
   */
  <E extends Element> E get(String alias, Class<E> elementType);

  /**
   * Return the associated {@link Query} instance.
   */
  Query query();

  /**
   * Return the associated {@link GraphSystem} instance.
   */
  @SuppressWarnings("rawtypes")
  GraphSystem system();

  /**
   * Drop the elements in the graph.
   */
  void drop();

  /**
   * Release the resources in the graph.
   */
  void close();

  /**
   * Reset the state of the graph, including any aliases, and element mappings.
   */
  void reset();

  /**
   * How should the graph add elements, considering that an element with that id or key might
   * already exist?
   */
  enum Should {
    /**
     * The default, which is to merge properties of the new and existing element, if any.
     */
    MERGE,
    /**
     * If it exists, replace all its (mutable) properties. Otherwise {@link #INSERT}.
     */
    REPLACE,
    /**
     * If the graph supports user ids, then {@link #INSERT}. Otherwise, {@link #MERGE}.
     */
    CREATE,
    /**
     * Add the element if and only if there is no existing element.
     */
    IGNORE,
    /**
     * Add the element, raising an exception if it already exists, and a key violation occurs.
     */
    INSERT;

    public String label() {
      return name().toLowerCase();
    }
  }

  /**
   * An interface that defines graph update events.
   */
  interface Observer {

    <V extends Vertex> void vertexAdded(V given, V stored);

    <V extends Vertex> void vertexRemoved(V given);

    <E extends Edge> void edgeAdded(E given, E stored);

    <E extends Edge> void edgeRemoved(E given);
  }

  /**
   * {@link Observer}s can register interest in the graph through this class.
   */
  @SuppressWarnings("PMD.ShortClassName")
  class O extends Observable<Observer> {

    private static final O INSTANCE = new O();

    public static O get() {
      return INSTANCE;
    }
  }
}
