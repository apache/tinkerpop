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
package org.apache.tinkerpop.gremlin.object;

import org.apache.tinkerpop.gremlin.object.graphs.Modern;
import org.apache.tinkerpop.gremlin.object.graphs.TheCrew;
import org.apache.tinkerpop.gremlin.object.edges.Develops;
import org.apache.tinkerpop.gremlin.object.edges.Knows;
import org.apache.tinkerpop.gremlin.object.vertices.Location;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.edges.Uses;
import org.apache.tinkerpop.gremlin.object.structure.Graph;
import org.apache.tinkerpop.gremlin.object.structure.GraphTest;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.object.traversal.Selections;
import org.apache.tinkerpop.gremlin.object.traversal.library.AddV;
import org.apache.tinkerpop.gremlin.object.traversal.library.Count;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasId;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasLabel;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasKeys;
import org.apache.tinkerpop.gremlin.object.traversal.library.Order;
import org.apache.tinkerpop.gremlin.object.traversal.library.Out;
import org.apache.tinkerpop.gremlin.object.traversal.library.Range;
import org.apache.tinkerpop.gremlin.object.traversal.library.Values;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import static org.apache.tinkerpop.gremlin.object.vertices.Location.year;
import static org.apache.tinkerpop.gremlin.object.edges.Uses.Skill.Competent;
import static org.apache.tinkerpop.gremlin.object.edges.Uses.Skill.Expert;
import static org.apache.tinkerpop.gremlin.object.edges.Uses.Skill.Proficient;
import static org.apache.tinkerpop.gremlin.object.structure.HasFeature.supportsGraphAddEdge;
import static org.apache.tinkerpop.gremlin.object.structure.HasFeature.supportsUserSuppliedIds;
import static org.apache.tinkerpop.gremlin.object.traversal.Selections.Selection;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This acts as the base of all provider specific implementations of the {@link Graph} and {@link
 * Query} interfaces. It uses the {@link Modern} and {@link TheCrew} factories to create sample
 * graphs, and runs a gauntlet of queries against them.
 *
 * <p>
 * The providers of the object graph are expected to extend this class, and pass the
 * implementation-defined instances of the {@link Graph} and {@link Query} to this class.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Slf4j
@EqualsAndHashCode(callSuper = false)
public abstract class ObjectGraphTest extends GraphTest {

  protected Query query;

  protected ObjectGraphTest(Graph graph, Query query) {
    super(graph);
    this.query = query;
  }

  protected abstract boolean supportsScripts();

  @Test
  public void testGetFirstNames() {
    if (!supportsScripts()) {
      return;
    }
    Modern modern = Modern.of(graph);
    List<String> names = query
        .by("g.V().values(property)")
        .bind("property", "name")
        .list(String.class);
    assertNotNull(names);
    assertTrue(names.contains(modern.vadas.getName()));
    assertTrue(names.contains(modern.marko.getName()));
  }

  @Test
  public void testGetKnownPeopleNames() {
    Modern modern = Modern.of(graph);
    List<String> names = query
        .by(Person.KnowsPeople, Values.of("name"))
        .list(String.class);
    assertNotNull(names);
    Collections.sort(names);
    assertEquals(Arrays.asList(modern.josh.getName(), modern.vadas.getName()), names);
  }

  @Test
  public void testDoesNotKnowPeople() {
    Modern modern = Modern.of(graph);
    List<Person> friends = query
        .by(modern.vadas.s("name"), Person.KnowsPeople)
        .list(Person.class);
    assertNotNull(friends);
    assertTrue(friends.isEmpty());
  }

  @Test
  public void testKnowsSomePeople() {
    Modern modern = Modern.of(graph);
    List<Person> friends = query
        .by(HasKeys.of(modern.marko), Person.KnowsPeople)
        .list(Person.class);
    assertNotNull(friends);
    Collections.sort(friends);
    assertEquals(Arrays.asList(modern.josh, modern.vadas), friends);
  }

  @Test
  public void testGetRangeOfPeople() {
    Modern modern = Modern.of(graph);
    List<Person> friends = query
        .by(HasLabel.of(Person.class), Range.of(0, 5))
        .list(Person.class);
    assertNotNull(friends);
    Collections.sort(friends);
    assertEquals(Arrays.asList(modern.josh, modern.marko, modern.peter, modern.vadas), friends);
  }

  @Test
  public void testFindGremlinDevelopers() {
    TheCrew crew = TheCrew.of(graph);
    List<Person> friends = query
        .by(HasKeys.of(crew.gremlin), Develops.Developers, Order.by("name"))
        .list(Person.class);
    assertNotNull(friends);
    Collections.sort(friends);
    friends.forEach(friend -> Collections.sort(friend.getLocations()));
    assertEquals(Arrays.asList(crew.marko, crew.matthias, crew.stephen), friends);
  }

  @Test
  public void testDetermineCurrentLocation() {
    TheCrew crew = TheCrew.of(graph);
    Selections selections = query
        .by(g -> g.V().as("a").
            properties("locations").as("b").
            hasNot("endTime").as("c").
            order().by("startTime").
            select("a", "b", "c").by("name").by(T.value).by("startTime").dedup())
        .as("a", String.class)
        .as("b", String.class)
        .as("c", Instant.class)
        .select();
    assertNotNull(selections);
    assertEquals(Selections.of(
        Selection.of().add("a", crew.marko.getName()).add("b", "santa fe").add("c", year(2005)),
        Selection.of().add("a", crew.stephen.getName()).add("b", "purcellville")
            .add("c", year(2006)),
        Selection.of().add("a", crew.daniel.getName()).add("b", "aachen").add("c", year(2009)),
        Selection.of().add("a", crew.matthias.getName()).add("b", "seattle").add("c", year(2014))
        ), selections);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRankUsersBySkill() {
    TheCrew crew = TheCrew.of(graph);
    Selections selections = query
        .by(g -> g.V().has("name", "gremlin").inE("uses").
            order().by("skill", incr).as("a").
            outV().as("b").
            order().by("name").
            select("a", "b").by("skill").by("name").dedup())
        .as("a", Uses.Skill.class)
        .as("b", String.class)
        .select();
    assertNotNull(selections);
    assertEquals(Selections.of(
        Selection.of().add("a", Expert).add("b", crew.daniel.getName()),
        Selection.of().add("a", Proficient).add("b", crew.marko.getName()),
        Selection.of().add("a", Competent).add("b", crew.matthias.getName()),
        Selection.of().add("a", Expert).add("b", crew.stephen.getName())), selections);
  }

  @Test
  public void testRemoveVertex() {
    TheCrew crew = TheCrew.of(graph);

    graph.removeVertex(crew.marko);
    long count = query
        .by(HasKeys.of(crew.marko), Count.of())
        .one(Long.class);
    assertEquals(0l, count);
  }

  @Test
  public void testAddExistingVertex() {
    TheCrew crew = TheCrew.of(graph);

    try {
      graph.addVertex(Person.of("marko", "engineer",
          Location.of("los angeles", 1996, 1997)));
      Person marko = query.by(HasKeys.of(crew.marko)).one(Person.class);
      Collections.sort(marko.getLocations());
      long count = query
          .by(HasKeys.of(marko), Count.of())
          .one(Long.class);
      switch (should) {
        case MERGE:
        case CREATE:
          assertEquals(Person.of("marko", "engineer",
              Location.of("brussels", 2004, 2005),
              Location.of("los angeles", 1996, 1997),
              Location.of("san diego", 1997, 2001),
              Location.of("santa cruz", 2001, 2004),
              Location.of("santa fe", 2005)
              ), marko);
          assertEquals(1, count);
          break;
        case REPLACE:
          assertEquals(Person.of("marko", "engineer",
              Location.of("los angeles", 1996, 1997)), marko);
          assertEquals(1, count);
          break;
        case IGNORE:
          assertEquals(Person.of("marko",
              Location.of("brussels", 2004, 2005),
              Location.of("san diego", 1997, 2001),
              Location.of("santa cruz", 2001, 2004),
              Location.of("santa fe", 2005)
              ), marko);
          assertEquals(1, count);
          break;
        case INSERT:
          fail("Insert should have failed since vertex already exists");
          break;
        default:
          break;
      }
    } catch (IllegalStateException eae) {
      switch (should) {
        case INSERT:
          break;
        default:
          throw eae;
      }
    } catch (IllegalArgumentException eae) {
      switch (should) {
        case CREATE:
          if (!graph.verify(supportsUserSuppliedIds(crew.marko))) {
            throw eae;
          }
          break;
        default:
          throw eae;
      }
    }
  }

  @Test
  public void testAddExistingKey() {
    TheCrew crew = TheCrew.of(graph);

    AddV addV = AddV.of(crew.marko);

    if (graph.verify(supportsUserSuppliedIds(crew.marko))) {
      try {
        graph
            .addEdge(Knows.of(10D), crew.daniel, addV);
        fail("Should not have been able to add existing vertex");
      } catch (IllegalArgumentException iae) {}
    } else {
      graph
          .addEdge(Knows.of(10D), crew.daniel, addV);
      Person marko = query.by(
          HasKeys.of(crew.daniel),
          Out.of(Knows.class),
          HasKeys.of(crew.marko))
          .one(Person.class);
      assertEquals(crew.marko, marko);
    }
  }

  @Test
  public void testAddMissingKey() {
    TheCrew crew = TheCrew.of(graph);

    Person intern = Person.of("intern");
    AddV addV = AddV.of(intern);

    graph
        .addEdge(Knows.of(10D), crew.daniel, addV);
    Person actual = query.by(
        HasKeys.of(crew.daniel),
        Out.of(Knows.class),
        HasKeys.of(intern))
        .one(Person.class);
    assertEquals(intern, actual);
  }

  @Test
  public void testRemoveEdge() {
    TheCrew crew = TheCrew.of(graph);

    graph.removeEdge(crew.markoDevelopsGremlin);
    long count = query
        .source(Query.Source.E)
        .by(HasId.of(crew.markoDevelopsGremlin), Count.of())
        .one(Long.class);
    assertEquals(0l, count);
  }

  @Test
  public void testAddExistingEdge() {
    Modern modern = Modern.of(graph);

    try {
      Instant now = Instant.now();
      Knows knows = graph
          .addEdge(Knows.of(now), modern.marko, modern.vadas)
          .as(Knows.class);
      long count = query
          .by(modern.marko.s("name"), Knows.KnowsWho, modern.vadas.s("name"), Count.of())
          .one(Long.class);

      switch (should) {
        case MERGE:
          assertEquals(Knows.of(0.5d, now), knows);
          assertEquals(1, count);
          break;
        case REPLACE:
          assertEquals(Knows.of(now), knows);
          assertEquals(1, count);
          break;
        case INSERT:
          if (!graph.verify(supportsGraphAddEdge())) {
            fail("Insert should have failed since edge already exists");
          }
          break;
        case IGNORE:
          assertEquals(Knows.of(0.5d), knows);
          assertEquals(1, count);
          break;
        default:
          break;
      }
    } catch (IllegalStateException eae) {
      if (!should.equals(Graph.Should.INSERT)) {
        throw eae;
      }
    }
  }


}
