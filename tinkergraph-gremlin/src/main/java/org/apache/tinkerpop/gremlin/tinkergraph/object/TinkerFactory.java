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
package org.apache.tinkerpop.gremlin.tinkergraph.object;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.object.provider.CachedFactory;
import org.apache.tinkerpop.gremlin.object.provider.GraphFactory;
import org.apache.tinkerpop.gremlin.object.provider.GraphSystem;
import org.apache.tinkerpop.gremlin.object.structure.Graph;
import org.apache.tinkerpop.gremlin.object.traversal.Query;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * * The {@link TinkerFactory} class is a cache-able implementation of the {@link GraphFactory}
 * interface. It is to be used as a fallback when dependency injection is not available.
 *
 * Note that tinkergraph is the default implementation of the object {@link Graph} and {@link Query}
 * interfaces, hence no explicit qualifier is needed to {@link javax.inject.Inject} it.
 *
 * For writes to the object {@link Graph}, use the {@link #graph()} method. For reads from the
 * object graph, use the {@link #query()} method.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TinkerFactory extends CachedFactory<Configuration> {

  private Configuration configuration;

  public TinkerFactory(ShouldCache shouldCache) {
    super(shouldCache);
  }

  @SuppressWarnings("PMD.ShortMethodName")
  public static TinkerFactory of() {
    return of(ShouldCache.GRAPH_SYSTEM);
  }

  @SuppressWarnings("PMD.ShortMethodName")
  public static TinkerFactory of(ShouldCache shouldCache) {
    return new TinkerFactory(shouldCache);
  }

  @Override
  protected Graph makeGraph() {
    return new TinkerGraph(system(), query());
  }

  @Override
  protected Query makeQuery() {
    return makeQuery(system());
  }

  @Override
  public GraphSystem<Configuration> makeSystem() {
    return new TinkerSystem(configuration);
  }

  private Query makeQuery(GraphSystem<Configuration> system) {
    return new TinkerQuery(system);
  }
}
