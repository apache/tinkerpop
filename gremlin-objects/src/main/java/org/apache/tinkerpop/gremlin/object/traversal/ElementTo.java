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
package org.apache.tinkerpop.gremlin.object.traversal;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Given that most {@link SubTraversal}s start at a gremlin element, it behooves us to define
 * specializations around that. The name of this class is indicative of the start type of the
 * sub-traversals defined here. The name of the inner class defined here is indicative of the end
 * type of the sub-traversal.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@FunctionalInterface
public interface ElementTo<O> extends SubTraversal<Element, O> {

  /**
   * A function denoting a {@code GraphTraversal} that starts at elements, and ends at elements.
   */
  @FunctionalInterface
  interface Element extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Element,
      org.apache.tinkerpop.gremlin.structure.Element> {

  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at elements, and ends at vertices.
   */
  @FunctionalInterface
  interface Vertex extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Element,
      org.apache.tinkerpop.gremlin.structure.Vertex> {

  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at elements, and ends at strings.
   */
  @FunctionalInterface
  interface String extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Element,
      java.lang.String> {

  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at elements, and ends at longs.
   */
  @FunctionalInterface
  interface Long extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Element,
      java.lang.Long> {

  }

  /**
   * A function denoting a {@code GraphTraversal} that starts at elements, and ends anywhere.
   */
  @FunctionalInterface
  @SuppressWarnings("PMD.ShortClassName")
  interface Any extends SubTraversal<
      org.apache.tinkerpop.gremlin.structure.Element,
      java.lang.Object> {

  }
}
