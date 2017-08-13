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

import org.apache.tinkerpop.gremlin.object.model.Hidden;
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Arrays;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * An {@link Edge} is an {@link Element} that is an object-oriented manifestation of an underlying
 * tinkerpop gremlin edge.
 *
 * <p>
 * Classes that extend the {@link Edge} will have it's fields seamlessly map to the underlying
 * edge's property. The label of the edge is its sub-classes uncapitalized simple name.
 *
 * <p>
 * The sub-class of an {@link Edge} must specify it's {@link #connections} upfront, at design time.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@ToString
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true,
    exclude = {"from", "to", "fromId", "toId", "cachedConnections"})
public abstract class Edge extends Element {

  /**
   * The object representation of the "from" vertex.
   */
  @Hidden
  private Vertex from;
  /**
   * The object representation of the "to" vertex.
   */
  @Hidden
  private Vertex to;

  /**
   * The id of the "from" vertex.
   */
  @Hidden
  private Object fromId;
  /**
   * The id of the "to" vertex.
   */
  @Hidden
  private Object toId;

  @Hidden
  private List<Connection> cachedConnections;

  protected abstract List<Connection> connections();

  /**
   * Does the edge have a connection between the given vertices?
   */
  @SuppressWarnings("PMD.CloseResource")
  public boolean connects(Vertex from, Vertex to) {
    return connects(from.getClass(), to.getClass());
  }

  /**
   * Does the edge have a connection between the given vertex types?
   */
  @SuppressWarnings("PMD.CloseResource")
  public boolean connects(Class<? extends Vertex> fromClass, Class<? extends Vertex> toClass) {
    Connection connection = Connection.of(fromClass, toClass);
    if (cachedConnections == null) {
      cachedConnections = connections();
    }
    return cachedConnections.contains(connection);
  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at edges, and ends at vertices.
   */
  public interface ToVertex extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Edge,
      org.apache.tinkerpop.gremlin.structure.Vertex> {

  }

  public static class Exceptions extends Element.Exceptions {

    private Exceptions() {}

    public static IllegalStateException missingEdgeVertex(
        Direction direction, Edge edge, org.apache.tinkerpop.gremlin.structure.Vertex vertex) {
      return missingEdgeVertex(direction, edge, vertex.toString());
    }

    public static IllegalStateException missingEdgeVertex(
        Direction direction, Edge edge, AnyTraversal anyTraversal) {
      return missingEdgeVertex(direction, edge, anyTraversal.toString());
    }

    @SuppressWarnings("rawtypes")
    public static IllegalStateException missingEdgeVertex(
        Direction direction, Edge edge, SubTraversal... subTraversals) {
      return missingEdgeVertex(direction, edge, Arrays.asList(subTraversals).toString());
    }

    public static IllegalStateException missingEdgeVertex(Direction direction, Edge edge,
        String vertexString) {
      return new IllegalStateException(
          String.format("%s vertex (%s) doesn't exist for edge %s",
              direction.equals(Direction.OUT) ? "Outgoing" : "Incoming", vertexString,
              edge));
    }

    public static IllegalStateException invalidEdgeConnection(Edge edge) {
      return new IllegalStateException(
          String.format("Edge connects an invalid pair of vertices", edge));
    }
  }
}
