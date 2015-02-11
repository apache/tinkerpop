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
package com.apache.tinkerpop.gremlin.structure.strategy;

import com.apache.tinkerpop.gremlin.structure.Graph;

/**
 * The {@link StrategyContext} object is provided to the methods of {@link GraphStrategy} so that the strategy functions
 * it constructs have some knowledge of the environment.
 *
 * @param <T> represents the object that is triggering the strategy (i.e. the vertex on which addEdge was called).
 */
public final class StrategyContext<T extends StrategyWrapped> {
    private final StrategyGraph g;
    private final T current;

    public StrategyContext(final StrategyGraph g, final T current) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");

        this.g = g;
        this.current = current;
    }

    /**
     * Gets the {@link StrategyWrapped} instance that is triggering the {@link GraphStrategy} method.
     */
    public T getCurrent() {
        return current;
    }

    /**
     * Gets the current {@link com.apache.tinkerpop.gremlin.structure.strategy.StrategyGraph} instance.
     */
    public StrategyGraph getStrategyGraph() {
        return g;
    }
}
