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
import org.apache.tinkerpop.gremlin.object.vertices.Location;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.vertices.Software;

import java.time.Instant;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * The {@link Develops} class represents the "develops" edge.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Develops extends Edge {

  /**
   * Go to the {@link Person} who develop the currently selected {@link Software}s.
   */
  public final static ToVertex Developers = traversal -> traversal
      .in(Label.of(Develops.class))
      .hasLabel(Label.of(Person.class));
  public final static ToVertex Softwares = traversal -> traversal
      .out(Label.of(Develops.class))
      .hasLabel(Label.of(Software.class));
  private Instant since;

  public static Develops of(Instant since) {
    return Develops.builder().since(since).build();
  }

  public static Develops of(int year) {
    return of(Location.year(year));
  }

  @Override
  protected List<Connection> connections() {
    return Connection.list(Person.class, Software.class);
  }
}
