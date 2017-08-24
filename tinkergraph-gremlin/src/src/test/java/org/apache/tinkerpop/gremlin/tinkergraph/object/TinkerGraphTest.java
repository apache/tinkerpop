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
import org.apache.tinkerpop.gremlin.object.ObjectGraphTest;
import org.apache.tinkerpop.gremlin.object.provider.GraphFactory;
import org.apache.tinkerpop.gremlin.object.structure.Graph;
import org.apache.tinkerpop.gremlin.object.traversal.Query;

import lombok.extern.slf4j.Slf4j;

/**
 * This is a test suite for the {@link TinkerGraph} and {@link TinkerQuery} alike. While all of the
 * testing is defined in {@link ObjectGraphTest}, this class is responsible for providing a
 * tinkergraph-specific version of the {@link Graph} and {@link Query} to it.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Slf4j
public class TinkerGraphTest extends ObjectGraphTest {

  private static final GraphFactory<Configuration> FACTORY = TinkerFactory.of();

  public TinkerGraphTest() {
    super(FACTORY.graph(), FACTORY.query());
  }

  @Override
  protected boolean supportsScripts() {
    return false;
  }
}
