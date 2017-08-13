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

import org.apache.tinkerpop.gremlin.object.model.OrderingKey;
import org.apache.tinkerpop.gremlin.object.model.PrimaryKey;
import org.apache.tinkerpop.gremlin.object.traversal.ElementTo;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.is;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyKey;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.propertyValue;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.keyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.orderingKeyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Keys.primaryKeyFields;
import static org.apache.tinkerpop.gremlin.object.reflect.Primitives.isMissing;

/**
 * {@link HasKeys} finds adjacent graph elements whose primary key properties correspond to that of
 * the given element.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Builder
@AllArgsConstructor
@RequiredArgsConstructor(staticName = "of")
@SuppressWarnings("PMD.TooManyStaticImports")
public class HasKeys implements ElementTo.Element {

  private final org.apache.tinkerpop.gremlin.object.structure.Element element;

  private Class<? extends Annotation> keyType;

  @SuppressWarnings("PMD.ShortMethodName")
  public static HasKeys of(org.apache.tinkerpop.gremlin.object.structure.Element element,
      Class<? extends Annotation> keyType) {
    return HasKeys.builder()
        .element(element)
        .keyType(keyType)
        .build();
  }

  @Override
  @SneakyThrows
  public GraphTraversal<Element, Element> apply(GraphTraversal<Element, Element> traversal) {
    traversal.hasLabel(element.label());
    List<Field> fields;
    if (keyType != null) {
      if (is(keyType, PrimaryKey.class)) {
        fields = primaryKeyFields(element);
      } else if (is(keyType, OrderingKey.class)) {
        fields = orderingKeyFields(element);
      } else {
        throw org.apache.tinkerpop.gremlin.object.structure.Element.Exceptions
            .invalidAnnotationType(keyType);
      }
    } else {
      fields = keyFields(element);
    }
    for (Field field : fields) {
      String key = propertyKey(field);
      Object value = propertyValue(field, element);
      if (isMissing(value)) {
        throw org.apache.tinkerpop.gremlin.object.structure.Element.Exceptions.requiredKeysMissing(
            element.getClass(), key);
      }
      traversal.has(key, value);
    }
    return traversal;
  }
}
