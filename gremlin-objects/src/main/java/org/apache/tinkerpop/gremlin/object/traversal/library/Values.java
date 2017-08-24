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

import org.apache.tinkerpop.gremlin.object.reflect.Properties;
import org.apache.tinkerpop.gremlin.object.traversal.ElementTo;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.List;

import lombok.Data;

/**
 * {@link Values} extracts the values of the given property keys, and removes duplicates thereof.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
public class Values implements ElementTo.Any {

  private final String[] propertyKeys;

  @SuppressWarnings("PMD.ShortMethodName")
  public static Values of(org.apache.tinkerpop.gremlin.object.structure.Element element) {
    return of(Properties.names(element));
  }

  @SuppressWarnings("PMD.ShortMethodName")
  public static Values of(List<String> propertyKeys) {
    return of(propertyKeys.toArray(new String[] {}));
  }

  @SuppressWarnings("PMD.ShortMethodName")
  public static Values of(String... propertyKeys) {
    return new Values(propertyKeys);
  }

  @Override
  public GraphTraversal<Element, Object> apply(GraphTraversal<Element, Element> traversal) {
    return traversal.values(propertyKeys).dedup();
  }

}
