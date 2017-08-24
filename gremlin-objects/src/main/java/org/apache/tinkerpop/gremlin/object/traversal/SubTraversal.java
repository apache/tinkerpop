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
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Formattable;
import java.util.Formatter;
import java.util.function.Function;

import lombok.SneakyThrows;

/**
 * {@link SubTraversal} represents a partial walk through of the graph. Given a @{link
 * GraphTraversal}, it applies an arbitrary number of steps on it, and returns the traversal back.
 *
 * <p> The {@link I} type parameter denotes the type of the selected graph elements at the start of
 * the sub-traversal, which we will refer to as its start type. Similarly, the {@link O} parameter
 * specifies the type at the end of the sub-traversal, referred to as its end type.
 *
 * <p> This allows you think of traversals in terms of reusing building blocks, which can then be
 * combined in one of two ways:
 *
 * <p/> <ul> <li>By passing a chain of these to the {@link Query#by(SubTraversal[])} method</li>
 * <li>By passing a chain of these to the object {@code Element#compose(SubTraversal[])} method</li>
 * </ul>
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@FunctionalInterface
public interface SubTraversal<I, O>
    extends Function<GraphTraversal<Element, I>, GraphTraversal<Element, O>>, Formattable {

  /**
   * Reveal the steps in the {@link SubTraversal} by passing a {@link DefaultGraphTraversal} to it,
   * and outputting it's string representation.
   */
  @Override
  @SneakyThrows
  default void formatTo(Formatter formatter, int flags, int width, int precision) {
    DefaultGraphTraversal defaultGraphTraversal = new DefaultGraphTraversal();
    apply(defaultGraphTraversal);
    formatter.out().append(defaultGraphTraversal.toString());
  }
}
