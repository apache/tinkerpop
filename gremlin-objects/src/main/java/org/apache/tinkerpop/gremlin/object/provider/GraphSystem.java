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
package org.apache.tinkerpop.gremlin.object.provider;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.script.Bindings;

/**
 * A {@link GraphSystem} represents an tinkerpop-based graph system. In essence, it's job is to
 * supply an implementation-defined {@link GraphTraversalSource}.
 *
 * <p>
 * Think of this as a service provider interface intended to be implemented by a graph system, such
 * as {@code TinkerGraph}, which at a minimum, knows how to define a {@link GraphTraversalSource}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings("rawtypes")
public interface GraphSystem<C> {

  @SuppressWarnings({"PMD.ShortMethodName"})
  /**
   * Provide an instance of the {@link GraphTraversalSource}.
   */
  GraphTraversalSource g();

  /**
   * How is this instance configured?
   */
  C configuration();

  /**
   * Complete the given gremlin traversal statement, and return a results iterable.
   */
  Iterable execute(String statement);

  /**
   * Complete the given gremlin traversal statement, and return a results iterable.
   */
  Iterable execute(String statement, Bindings bindings);

  /**
   * Release any resources associated with the system.
   */
  void close();
}
