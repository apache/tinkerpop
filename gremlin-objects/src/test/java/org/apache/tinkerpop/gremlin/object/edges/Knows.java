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
package org.apache.tinkerpop.gremlin.object.edges;

import org.apache.tinkerpop.gremlin.object.reflect.Label;
import org.apache.tinkerpop.gremlin.object.structure.Connection;
import org.apache.tinkerpop.gremlin.object.structure.Edge;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.time.Instant;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * The {@link Knows} class represents the "knows" edge.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Knows extends Edge {

  /**
   * Go to the {@link Person}s you know, assuming you're currently selected.
   */
  public final static ToVertex KnowsWho = traversal -> traversal
      .outE(Label.of(Knows.class))
      .toV(Direction.IN);
  /**
   * Go to  to the {@link Person}s who know of you, assuming you're currently selected.
   */
  public final static ToVertex KnownBy = traversal -> traversal
      .inE(Label.of(Knows.class))
      .toV(Direction.OUT);

  private Double weight;

  private Instant since;

  public static Knows of(double weight) {
    return of(weight, null);
  }

  public static Knows of(Instant since) {
    return of(null, since);
  }

  public static Knows of(Double weight, Instant since) {
    return Knows.builder().weight(weight).since(since).build();
  }

  @Override
  protected List<Connection> connections() {
    return Connection.list(Person.class, Person.class);
  }

}
