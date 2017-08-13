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

import java.util.function.Function;

/**
 * The {@link SubGraph} is a function, which given a {@link Graph}, lets one chain the addition and
 * removal of object vertices and edges. This comes in handy, when one wants to update the {@link
 * Graph}, without necessarily knowing when or how it will be provided.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@FunctionalInterface
public interface SubGraph extends Function<Graph, Graph> {

  /**
   * Returns a composed function that first applies the {@code this} function to its input, and then
   * applies {@code that} function to the result. If evaluation of either function throws an
   * exception, it is relayed to the caller of the chained function.
   *
   * It works in the reverse order of the {@link #compose(Function)} method.
   */
  default SubGraph chain(SubGraph that) {
    return graph -> that.apply(apply(graph));
  }
}
