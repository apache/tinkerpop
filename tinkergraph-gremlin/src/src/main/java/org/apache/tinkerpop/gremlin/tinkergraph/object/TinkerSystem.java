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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.object.provider.GraphSystem;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.script.Bindings;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link TinkerSystem} is an {@link GraphSystem} based on the  tinkergraph reference
 * implementation. It obtains a {@link GraphTraversalSource} by opening a {@link TinkerGraph}, using
 * the provided {@link Configuration}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
@Singleton
public class TinkerSystem implements GraphSystem<Configuration> {

  /**
   * A cache of unique configurations and their corresponding tinker graphs.
   */
  static Map<Configuration, TinkerGraph> tinkerGraphs = new LinkedHashMap<>();

  /**
   * The default configuration to use, if none is provided.
   */
  static final Configuration EMPTY_CONFIGURATION;

  static {
    EMPTY_CONFIGURATION = new BaseConfiguration();
    EMPTY_CONFIGURATION.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
  }

  /**
   * The configuration activated in this instance.
   */
  private Configuration configurationActivated;

  /**
   * Create an instance using the default configuration.
   */
  public TinkerSystem() {
    this(EMPTY_CONFIGURATION);
  }

  /**
   * Make an instance for the given configuration, and save it in the {@link #tinkerGraphs} cache.
   */
  public TinkerSystem(Configuration configuration) {
    configurationActivated = configuration != null ? configuration : EMPTY_CONFIGURATION;
    tinkerGraphs.computeIfAbsent(configurationActivated, TinkerGraph::open);
  }

  @Override
  public Configuration configuration() {
    return configurationActivated;
  }

  /**
   * Supply a {@link GraphTraversalSource} by applying {@link TinkerGraph#traversal()} on this
   * instance's configuration.
   */
  @Override
  @SuppressWarnings("PMD.ShortMethodName")
  public GraphTraversalSource g() {
    return tinkerGraphs.get(configurationActivated).traversal();
  }

  @Override
  public Iterable<?> execute(String statement) {
    throw new UnsupportedOperationException("Tinker graph script execution not supported");
  }

  @Override
  public Iterable<?> execute(String statement, Bindings bindings) {
    throw new UnsupportedOperationException("Tinker graph script execution not supported");
  }

  @Override
  @SneakyThrows
  public void close() {
    tinkerGraphs.values().forEach(TinkerGraph::close);
    tinkerGraphs.clear();
  }
}
