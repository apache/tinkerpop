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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Formattable;
import java.util.Formatter;
import java.util.function.Function;

import lombok.SneakyThrows;


/**
 * {@link AnyTraversal} denotes an arbitrary yet complete walk through over the graph, which returns
 * a {@link GraphTraversal} whose results are ready to be consumed.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@FunctionalInterface
public interface AnyTraversal extends Function<GraphTraversalSource, GraphTraversal<?, ?>>,
    Formattable {
  /**
   * Reveal the steps in the {@link SubTraversal} by passing a {@link DefaultGraphTraversal} to it,
   * and outputting it's string representation.
   */
  @Override
  @SneakyThrows
  default void formatTo(Formatter formatter, int flags, int width, int precision) {
    GraphTraversalSource g = new GraphTraversalSource(EmptyGraph.instance());
    GraphTraversal<?, ?> graphTraversal = apply(g);
    formatter.out().append(graphTraversal.toString());
  }

}
