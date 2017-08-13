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
package org.apache.tinkerpop.gremlin.object.provider;

import org.apache.tinkerpop.gremlin.object.graphs.TheCrew;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.vertices.Software;
import org.apache.tinkerpop.gremlin.object.structure.Graph;
import org.apache.tinkerpop.gremlin.object.traversal.Query;
import org.apache.tinkerpop.gremlin.object.traversal.library.Count;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasLabel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import lombok.SneakyThrows;

import static org.apache.tinkerpop.gremlin.object.provider.CachedFactory.ShouldCache.EVERYTHING;
import static org.apache.tinkerpop.gremlin.object.provider.CachedFactory.ShouldCache.GRAPH_SYSTEM;
import static org.apache.tinkerpop.gremlin.object.provider.CachedFactory.ShouldCache.NOTHING;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * The {@link GraphFactoryTest} defines sanity tests to ensure that the {@link Graph} and {@link
 * Query} instances provided by the {@link #factory()} method work.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@RunWith(Parameterized.class)
@SuppressWarnings("rawtypes")
public abstract class GraphFactoryTest<C> {

  @Parameterized.Parameter
  public CachedFactory.ShouldCache shouldCache;
  private Graph graph;
  private Query query;
  private GraphSystem<C> system;
  private GraphFactory<C> factory;
  private ExecutorService executorService;

  @Parameterized.Parameters(name = "ShouldCache({0})")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {EVERYTHING}, {GRAPH_SYSTEM}, {NOTHING}
    });
  }

  protected abstract GraphFactory<C> factory(CachedFactory.ShouldCache shouldCache);

  protected GraphFactory<C> factory() {
    if (factory == null) {
      factory = factory(shouldCache);
      if (is(factory, CachedFactory.class)) {
        ((CachedFactory) factory).clear();
      }
    }
    return factory;
  }

  @Before
  public void setUp() {
    executorService = new ForkJoinPool();
    graph = factory().graph();
    query = factory().query();
    system = factory().system();
    graph.drop();
  }

  @After
  public void tearDown() {
    factory = null;
    graph.drop();
    graph.close();
    query.close();
    system.close();
  }

  @Test
  @SneakyThrows
  public void testCacheBehavior() {
    switch (shouldCache) {
      case EVERYTHING:
        assertEquals(factory().graph(), graph);
        assertEquals(factory().query(), query);
        assertEquals(factory().system(), system);
        break;
      case GRAPH_SYSTEM:
        executorService.submit(() -> {
          assertNotEquals(factory().graph(), graph);
          assertNotEquals(factory().query(), query);
        }).get();
        assertEquals(factory().graph(), graph);
        assertEquals(factory().query(), query);
        assertEquals(factory().system(), system);
        break;
      case NOTHING:
      default:
        assertNotEquals(factory().graph(), graph);
        assertNotEquals(factory().query(), query);
        assertNotEquals(factory().system(), system);
        break;
    }
  }

  @Test
  public void testSystemIsEmpty() {
    long count = system.g().V().count().next();
    assertEquals(0l, count);
  }

  @Test
  public void testGraphHasObjects() {
    TheCrew crew = TheCrew.of(graph);
    assertNotNull(crew.marko);
    assertNotNull(crew.gremlin);

    long persons = query
        .by(
            HasLabel.of(Person.class),
            Count.of())
        .one(Long.class);
    assertEquals(4l, persons);

    long softwares = query
        .by(
            HasLabel.of(Software.class),
            Count.of())
        .one(Long.class);
    assertEquals(2l, softwares);
  }

}
