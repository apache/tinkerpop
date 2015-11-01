/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategyTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TinkerGraphStrategySuite extends AbstractGremlinSuite {

    public TinkerGraphStrategySuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {

        super(klass, builder,
                new Class<?>[]{
                        TinkerGraphStepStrategyTest.class
                }, new Class<?>[]{
                        TinkerGraphStepStrategyTest.class
                },
                false,
                TraversalEngine.Type.STANDARD);
    }

}