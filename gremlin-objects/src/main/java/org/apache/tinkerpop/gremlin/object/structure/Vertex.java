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

import org.apache.tinkerpop.gremlin.object.model.PropertyValue;
import org.apache.tinkerpop.gremlin.object.traversal.SubTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * A {@link Vertex} is an {@link Element} that is the object-oriented manifestation of the
 * underlying graph vertex.
 *
 * <p>
 * Classes that extend the {@link Vertex} will have it's fields seamlessly map to the underlying
 * edge's property. The label of the edge is derived as the simple name of the sub-class.
 *
 * <p>
 * Typically, a vertex property is a primitive type, which manifests itself as a primitive field of
 * the user defined sub-class. Sometimes, a vertex property can have properties of its own, known as
 * meta-properties. Such vertex properties manifest themselves as fields that are {@link Element}s
 * in their own right. The value of the vertex property is mapped to the field of it's {@link
 * Element} that is annotated with {@link PropertyValue}. The rest of the fields in that element
 * correspond to the vertex property's meta-properties.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@ToString
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuppressWarnings("PMD.CommentSize")
public class Vertex extends Element {

  public Vertex(Element element) {
    super(element);
  }

  /**
   * Return the underlying gremlin vertex.
   */
  public org.apache.tinkerpop.gremlin.structure.Vertex delegate() {
    return (org.apache.tinkerpop.gremlin.structure.Vertex) delegate;
  }

  /**
   * Remove this vertex, if attached.
   */
  public void remove() {
    org.apache.tinkerpop.gremlin.structure.Vertex delegate = delegate();
    if (delegate == null) {
      throw Element.Exceptions.removingDetachedElement(this);
    }
    delegate.remove();
  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at vertices, and ends at vertices.
   */
  public interface ToVertex extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Vertex,
      org.apache.tinkerpop.gremlin.structure.Vertex> {

  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at vertices, and ends at edges.
   */
  interface ToEdge extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Vertex, Edge> {

  }
}
