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

import org.apache.tinkerpop.gremlin.object.structure.Graph;
import org.apache.tinkerpop.gremlin.object.traversal.Query;

import lombok.RequiredArgsConstructor;

/**
 * The {@link CachedFactory} implements the {@link GraphFactory} such that its provided instances
 * may be optionally cached, as specified by the {@link #shouldCache} value.
 *
 * <p>
 * The expectation is that the graph system provider's implementation of {@link GraphFactory} will
 * extend this class, and will turn caching on, so as to reduce the number of resources it holds.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@RequiredArgsConstructor
public abstract class CachedFactory<C> implements GraphFactory<C> {

  public enum ShouldCache {
    /**
     * Cache the instances returned by {@link #graph()}, {@link #query()}, and {@link #system()}.
     *
     * <p>
     * This mode may be used in single-threaded applications, if you want to avoid the overhead of
     * creating the state of the {@link #graph()} and {@link #query()} instances. The caller must
     * remember to finally invoke {@link Graph#reset()} after every use.
     */
    EVERYTHING,
    /**
     * Cache the instances returned by {@link #graph()}, {@link #query()}, and {@link #system()}.
     *
     * <p>
     * This mode should be used in multi-threaded applications, where the resources underlying the
     * {@link #system()} can handle concurrent requests. Since the {@link #graph()} } and {@link
     * #query()} instances need to be thread-safe, their states will be stored in a thread-local.
     * The caller thread must remember to finally invoke {@link Graph#reset()} after every use.
     */
    GRAPH_SYSTEM,
    /**
     * Do not cache instances returned by {@link #graph()}, {@link #query()}, and {@link
     * #system()}.
     *
     * <p>
     * This mode may be used in multi-threaded applications, when the resources underlying the
     * {@link #system()} cannot handle concurrent requests, or it is preferable to create new
     * "system" resources per caller, or if the caller needs to employ custom caching schemes. Since
     * the {@link #graph()}  and {@link #query()} instances are not cached, there's no need to
     * {@link Graph#reset} them after every use, unless of source, the caller keeps a copy of it.
     */
    NOTHING
  }

  protected final ShouldCache shouldCache;

  private static Graph cachedGraph;
  private static Query cachedQuery;

  private static ThreadLocal<Graph> threadGraph = new ThreadLocal<>();
  private static ThreadLocal<Query> threadQuery = new ThreadLocal<>();

  private static GraphSystem<?> cachedSystem;

  protected abstract Graph makeGraph();

  protected abstract Query makeQuery();

  protected abstract GraphSystem<C> makeSystem();

  @Override
  public final Graph graph() {
    switch (shouldCache) {
      case EVERYTHING:
        if (cachedGraph != null) {
          return cachedGraph;
        }
        break;
      case GRAPH_SYSTEM:
        if (threadGraph.get() != null) {
          return threadGraph.get();
        }
        break;
      case NOTHING:
        break;
      default:
        break;
    }
    synchronized (CachedFactory.class) {
      if (cachedGraph != null) {
        cachedGraph.reset();
        return cachedGraph;
      }
      Graph graph = makeGraph();
      switch (shouldCache) {
        case EVERYTHING:
          cachedGraph = graph;
          break;
        case GRAPH_SYSTEM:
          threadGraph.set(graph);
          break;
        case NOTHING:
        default:
      }
      graph.reset();
      return graph;
    }
  }

  @Override
  public final Query query() {
    switch (shouldCache) {
      case EVERYTHING:
        if (cachedQuery != null) {
          return cachedQuery;
        }
        break;
      case GRAPH_SYSTEM:
        if (threadQuery.get() != null) {
          return threadQuery.get();
        }
        break;
      case NOTHING:
        break;
      default:
        break;
    }
    synchronized (CachedFactory.class) {
      if (cachedQuery != null) {
        return cachedQuery;
      }
      Query query = makeQuery();
      switch (shouldCache) {
        case EVERYTHING:
          cachedQuery = query;
          break;
        case GRAPH_SYSTEM:
          threadQuery.set(query);
          break;
        case NOTHING:
        default:
          break;
      }
      return query;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public final GraphSystem<C> system() {
    switch (shouldCache) {
      case EVERYTHING:
      case GRAPH_SYSTEM:
        if (cachedSystem != null) {
          return (GraphSystem<C>) cachedSystem;
        }
        break;
      case NOTHING:
      default:
        break;
    }
    synchronized (CachedFactory.class) {
      if (cachedSystem != null) {
        return (GraphSystem<C>) cachedSystem;
      }
      GraphSystem<C> system = makeSystem();
      switch (shouldCache) {
        case EVERYTHING:
        case GRAPH_SYSTEM:
          cachedSystem = system;
          break;
        case NOTHING:
        default:
          break;
      }
      return system;
    }
  }

  public void clear() {
    cachedGraph = null;
    cachedQuery = null;
    cachedSystem = null;
    threadGraph.remove();
    threadQuery.remove();
  }
}
