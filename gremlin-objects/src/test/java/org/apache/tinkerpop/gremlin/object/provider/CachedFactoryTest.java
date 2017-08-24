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

import static org.mockito.Mockito.mock;

/**
 * Run the {@link GraphFactoryTest} using mocked {@link Graph} and {@link Query} instances.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class CachedFactoryTest extends GraphFactoryTest<Void> {

  @Override
  protected GraphFactory<Void> factory(CachedFactory.ShouldCache shouldCache) {
    return new MockedFactory(shouldCache);
  }

  @Override
  public void testSystemIsEmpty() {}

  @Override
  public void testGraphHasObjects() {}

  public static class MockedFactory extends CachedFactory<Void> {

    public MockedFactory(ShouldCache shouldCache) {
      super(shouldCache);
    }

    @Override
    protected Graph makeGraph() {
      return mock(Graph.class);
    }

    @Override
    protected Query makeQuery() {
      return mock(Query.class);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected GraphSystem<Void> makeSystem() {
      return mock(GraphSystem.class);
    }

  }
}
