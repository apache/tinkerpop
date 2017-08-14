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
import org.apache.tinkerpop.gremlin.object.vertices.Software;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * The {@link Uses} class represents the "uses" edge.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(fluent = true, chain = true)
@EqualsAndHashCode(of = {}, callSuper = true)
public class Uses extends Edge {

  /**
   * Go to the {@link Software}s used by the currently selected {@link Person}s.
   */
  public final static ToVertex Softwares = traversal -> traversal
      .outE(Label.of(Uses.class))
      .toV(Direction.IN);
  /**
   * Go to the {@link Person}s using the currently selected {@link Software}s.
   */
  public final static ToVertex People = traversal -> traversal
      .inE(Label.of(Uses.class))
      .toV(Direction.OUT);

  /**
   * An enum field whose name is used as the value of the "skill" property.
   */
  private Skill skill;

  public static Uses of(Skill skill) {
    return Uses.builder().skill(skill).build();
  }

  @Override
  protected List<Connection> connections() {
    return Connection.list(Person.class, Software.class);
  }

  public enum Skill {
    Novice, Beginner, Competent, Proficient, Expert,
  }

}
