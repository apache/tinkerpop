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
package org.apache.tinkerpop.gremlin.object.traversal.library;

import org.apache.tinkerpop.gremlin.object.traversal.ElementTo;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * {@link Id} extracts the id(s) of the currently selected elements in the traversal.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@RequiredArgsConstructor(staticName = "of")
@SuppressWarnings("PMD.ShortClassName")
public class Id implements ElementTo.Any {

  @Override
  public GraphTraversal<Element, Object> apply(GraphTraversal<Element, Element> traversal) {
    return traversal.id();
  }

}
