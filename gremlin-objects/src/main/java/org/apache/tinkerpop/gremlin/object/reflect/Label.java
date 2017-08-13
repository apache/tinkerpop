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
package org.apache.tinkerpop.gremlin.object.reflect;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.tinkerpop.gremlin.object.model.Alias;
import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.structure.Element;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.is;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.name;
import static org.apache.tinkerpop.gremlin.object.reflect.Fields.alias;

/**
 * The label of a vertex (or edge) object can be obtained through the {@link Label#of} methods.
 *
 * <p>
 * By default, the label of a vertex is the same as it's object's simple class name, and that of an
 * edge is the same, except it's uncapitalized. If you wish to override this default naming
 * convention, you may annotate the class with a {@link Alias#label()} value.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public final class Label {

  private Label() {}

  @SuppressWarnings("PMD.ShortMethodName")
  public static String of(Element element) {
    return of(element.getClass());
  }

  @SuppressWarnings("PMD.ShortMethodName")
  public static String of(Class<? extends Element> elementType) {
    String label = alias(elementType, Alias::label);
    if (label == null) {
      label = name(elementType);
      if (is(elementType, Edge.class)) {
        label = WordUtils.uncapitalize(label);
      }
    }
    return label;
  }
}
