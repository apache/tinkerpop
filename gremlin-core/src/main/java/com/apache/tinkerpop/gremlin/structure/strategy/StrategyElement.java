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

import com.apache.tinkerpop.gremlin.structure.Element;
import com.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StrategyElement implements Element, StrategyWrapped {
    protected final StrategyGraph strategyGraph;
    protected final GraphStrategy strategy;
    protected final Element baseElement;
    protected final StrategyContext<StrategyElement> elementStrategyContext;

    protected StrategyElement(final Element baseElement, final StrategyGraph strategyGraph) {
        if (baseElement instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The element %s is already StrategyWrapped and must be a base Element", baseElement));
        this.strategyGraph = strategyGraph;
        this.strategy = strategyGraph.getStrategy();
        this.baseElement = baseElement;
        this.elementStrategyContext = new StrategyContext<>(strategyGraph, this);
    }

    public Element getBaseElement() {
        return this.baseElement;
    }

    @Override
    public int hashCode() {
        return this.baseElement.hashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
