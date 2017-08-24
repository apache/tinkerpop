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
import org.apache.tinkerpop.gremlin.object.provider.GraphFactoryTest;

/**
 * This is a sanity test for the {@link TinkerFactory}. While all of the testing is defined in
 * {@link GraphFactoryTest}, this class is responsible for providing a {@link TinkerFactory} based
 * on the parameterized {@code ShouldCache} value.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class TinkerFactoryTest extends GraphFactoryTest<Configuration> {

  @Override
  protected GraphFactory<Configuration> factory(CachedFactory.ShouldCache shouldCache) {
    return TinkerFactory.of(shouldCache);
  }
}
