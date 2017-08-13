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

import org.apache.tinkerpop.gremlin.object.structure.HasFeature;
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.lang.reflect.Field;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.keyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isMissing;
import static org.apache.tinkerpop.gremlin.object.structure.HasFeature.supportsUserSuppliedIds;

/**
 * {@link AddV} attempts to add a vertex with the keys of the given element.
 *
 * <p>
 * In the case of graphs that don't support user supplied ids, if a vertex with that key(s) already
 * exists, it will be returned as-is. Otherwise, a vertex will be created with the given key(s), and
 * returned.
 *
 * <p>
 * If user supplied ids is supported, then this will add the vertex if it's missing, and raise an
 * exception if it already exists.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@RequiredArgsConstructor(staticName = "of")
@SuppressWarnings({"unchecked", "rawtypes", "PMD.TooManyStaticImports"})
public class AddV implements AnyTraversal {

  private final org.apache.tinkerpop.gremlin.object.structure.Element element;

  @Override
  @SneakyThrows
  public GraphTraversal<Element, Element> apply(GraphTraversalSource g) {
    GraphTraversal traversal = g.addV(element.label());
    if (element.id() != null && HasFeature.Verifier.of(g)
        .verify(supportsUserSuppliedIds(element))) {
      traversal.property(T.id, element.id());
    }
    for (Field field : keyFields(element)) {
      String key = propertyKey(field);
      Object value = propertyValue(field, element);
      if (isMissing(value)) {
        throw org.apache.tinkerpop.gremlin.object.structure.Element.Exceptions.requiredKeysMissing(
            element.getClass(), key);
      }
      traversal.property(key, value);
    }
    return traversal;
  }
}
