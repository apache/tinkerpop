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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertEquals;

/**
 * Ensure that the {@link TinkerSystem} is configurable and traversable.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
public class TinkerSystemTest {

  private TinkerSystem tinkerSystem;

  @After
  public void tearDown() {
    if (tinkerSystem != null) {
      tinkerSystem.close();
    }
  }

  @Test
  public void testDefaultConfiguration() {
    tinkerSystem = new TinkerSystem();
    Set<Configuration> configurations = TinkerSystem.tinkerGraphs.keySet();
    assertEquals(1, configurations.size());
    assertEquals(TinkerSystem.EMPTY_CONFIGURATION, configurations.iterator().next());
    assertEquals(TinkerSystem.EMPTY_CONFIGURATION, tinkerSystem.configuration());
  }

  @Test
  public void testCustomConfiguration() {
    Configuration configuration = new BaseConfiguration();
    tinkerSystem = new TinkerSystem(configuration);
    Set<Configuration> configurations = TinkerSystem.tinkerGraphs.keySet();
    assertEquals(1, configurations.size());
    assertEquals(configuration, configurations.iterator().next());
    assertEquals(configuration, tinkerSystem.configuration());
  }

  @Test
  public void testMultipleConfigurations() {
    Configuration someConfiguration = new BaseConfiguration();
    tinkerSystem = new TinkerSystem(someConfiguration);

    Configuration otherConfiguration = new BaseConfiguration();
    TinkerSystem otherSystem = new TinkerSystem(otherConfiguration);

    Set<Configuration> configurations = TinkerSystem.tinkerGraphs.keySet();
    assertEquals(2, configurations.size());
    Iterator<Configuration> iterator = configurations.iterator();
    assertEquals(someConfiguration, iterator.next());
    assertEquals(someConfiguration, tinkerSystem.configuration());
    assertEquals(otherConfiguration, iterator.next());
    assertEquals(otherConfiguration, otherSystem.configuration());
  }

  @Test
  public void testTraversalObtainable() {
    tinkerSystem = new TinkerSystem();
    GraphTraversalSource g = tinkerSystem.g();
    System.out.println("" + g.V().toBulkSet());
    assertEquals(Long.valueOf(0L), g.V().count().next());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExecuteStatementFails() {
    tinkerSystem = new TinkerSystem();
    tinkerSystem.execute("g.V().count().next()");
  }
}
