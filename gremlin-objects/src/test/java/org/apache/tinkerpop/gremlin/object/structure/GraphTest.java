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
package org.apache.tinkerpop.gremlin.object.structure;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.CREATE;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.IGNORE;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.INSERT;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.MERGE;
import static org.apache.tinkerpop.gremlin.object.structure.Graph.Should.REPLACE;

/**
 * This acts as a base class for all {@link Graph} related tests. It parameterizes the {@link
 * #should} variable, and runs all tests for each of the possible values of {@link Graph.Should}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Slf4j
@RunWith(Parameterized.class)
public abstract class GraphTest {

  protected final Graph graph;
  @Parameterized.Parameter
  public Graph.Should should;

  protected GraphTest(Graph graph) {
    this.graph = graph;
  }

  @Parameterized.Parameters(name = "should({0})")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {CREATE}, {MERGE}, {REPLACE}, {IGNORE}, {INSERT}
    });
  }

  @Before
  public void setUp() {
    this.graph.should(should);
    graph.drop();
  }

  @After
  public void tearDown() {
    graph.reset();
    graph.drop();
  }
}
