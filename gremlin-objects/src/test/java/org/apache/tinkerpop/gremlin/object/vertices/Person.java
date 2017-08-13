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
package org.apache.tinkerpop.gremlin.object.vertices;

import org.apache.tinkerpop.gremlin.object.edges.Knows;
import org.apache.tinkerpop.gremlin.object.model.Alias;
import org.apache.tinkerpop.gremlin.object.model.OrderingKey;
import org.apache.tinkerpop.gremlin.object.model.PrimaryKey;
import org.apache.tinkerpop.gremlin.object.reflect.Label;
import org.apache.tinkerpop.gremlin.object.structure.Vertex;
import org.apache.tinkerpop.gremlin.object.traversal.AnyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * The {@link Person} class represents the "person" vertex. Its label has been overridden to be
 * "person", based on its class-level {@link Alias} annotation.
 *
 * <p>
 * It has a {@link #name} property which we'll assume uniquely identifies it in the "person" vertex,
 * and hence is annotated with {@link PrimaryKey}. It has a required {@link #age} property, by which
 * persons are assumed to be ordered, in the graph system, and hence a candidate for a {@link
 * GraphTraversal#order()} by clause.
 *
 * <p>
 * Whereas both of the above properties are primitive, the {@link #titles} property is a
 * multi-property, and furthermore a set as opposed to a list. Similarly, the {@link #locations}
 * property is a multi-property, only it's a list, *and* it has meta-properties of it's own. To
 * represent the value of the location property, as well as it's meta-properties, a {@link Location}
 * class is defined.
 *
 * <p>
 * Finally, the person vertex defines a sub-traversal, which tells us how to go from this person to
 * the other people that it knows. Other person-related traversal logic may be co-located here.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Alias(label = "person")
@EqualsAndHashCode(callSuper = true)
public class Person extends Vertex {

  public static ToVertex KnowsPeople = traversal -> traversal
      .out(Label.of(Knows.class))
      .hasLabel(Label.of(Person.class));

  @PrimaryKey
  private String name;

  @OrderingKey
  private int age;

  /**
   * Since the lambda object below is instance-specific, it needs to either have it's name start
   * with a {@code $} symbol, or be marked as {@code transient}. If you have a lot of such
   * instance-specific lambda fields, then you could ignore them all for the purposes of {@link
   * #equals(Object)} and {@link #hashCode()}, by including {@code of={}} in the {@link
   * EqualsAndHashCode} annotation above.
   */
  public final AnyTraversal $sameAge = g -> g.V()
      .hasLabel(Label.of(Knows.class))
      .has("age", age);

  /**
   * This is a multi-property of primitive types.
   */
  private Set<String> titles;

  /**
   * This is a multi-property of a vertex property, each of which has meta-properties of its own.
   */
  private List<Location> locations;

  public static Person of(String name, Location... locations) {
    return of(name, 0, locations);
  }

  public static Person of(String name, int age) {
    return of(name, age, (Location[]) null);
  }

  public static Person of(String name, int age, Location... locations) {
    return of(name, age, null, locations);
  }

  public static Person of(String name, String title, Location... locations) {
    return of(name, 0, title, locations);
  }

  public static Person of(String name, int age, String title, Location... locations) {
    Set<String> titleSet = null;
    if (title != null) {
      titleSet = new HashSet<>();
      Collections.addAll(titleSet, title);
    }
    List<Location> locationList = null;
    if (locations != null && locations.length > 0) {
      locationList = Arrays.asList(locations);
    }
    return of(name, age, titleSet, locationList);
  }

  public static Person of(String name, int age, Set<String> titles, List<Location> locations) {
    Person.PersonBuilder builder = Person.builder().name(name).age(age);
    if (locations != null) {
      builder.locations(locations);
    }
    if (titles != null) {
      builder.titles(titles);
    }
    return builder.build();
  }
}
