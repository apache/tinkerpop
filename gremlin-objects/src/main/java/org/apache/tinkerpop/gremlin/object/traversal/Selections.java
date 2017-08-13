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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * The {@link Selection} captures the result of a {@link Query} that involves a select step. For
 * each alias selected, the corresponding {@link Query#as(String, Class)} must be called to specify
 * the type of the selected alias.
 *
 * <p>
 * The {@link Selection} returned by {@link Query#select()} is a list of {@link Selection}s. Each
 * {@link Selection} in turn is a map of the alias to it's corresponding object, whose type is as
 * was specified by the {@link Query#as(String, Class)} method.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@RequiredArgsConstructor(staticName = "of")
@EqualsAndHashCode(callSuper = true, exclude = {"classes"})
@SuppressWarnings({"PMD.ShortMethodName", "rawtypes"})
public class Selections extends ArrayList<Selections.Selection> {

  public static final long serialVersionUID = 1L;
  private Map<String, Class> classes = new HashMap<>();

  public static Selections of(Selection... selectionList) {
    Selections selections = Selections.of();
    selections.addAll(Arrays.asList(selectionList));
    return selections;
  }

  public void as(String alias, Class clazz) {
    classes.put(alias, clazz);
  }

  public Class as(String alias) {
    return classes.get(alias);
  }

  @RequiredArgsConstructor(staticName = "of")
  public static class Selection extends HashMap<String, Object> {

    public static final long serialVersionUID = 1L;


    public Selection add(String alias, Object object) {
      this.put(alias, object);
      return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T as(String alias, Class<T> elementType) {
      return (T) this.get(alias);
    }
  }
}
