/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.language.grammar.TraversalStrategyVisitor;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Collections;

/**
 * This class is for use with {@link GremlinLang} and for serialization purposes. It is not meant for direct use with
 * {@link TraversalSource#withStrategies(TraversalStrategy[])}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalStrategyProxy<T extends TraversalStrategy> implements Serializable, TraversalStrategy {

    private final Configuration configuration;
    private final String strategyName;

    public TraversalStrategyProxy(final String strategyName) {
        this(strategyName, new MapConfiguration(Collections.EMPTY_MAP));
    }

    public TraversalStrategyProxy(final String strategyName, final Configuration configuration) {
        this.configuration = configuration;
        this.strategyName = strategyName;
    }

    public TraversalStrategyProxy(final T traversalStrategy) {
        this(traversalStrategy.getClass().getSimpleName(), traversalStrategy.getConfiguration());
    }

    /**
     * @deprecated
     * This constructor has been deprecated since 4.0.0 as TraversalStrategyProxy is now based around strategy names,
     * instead of strategy classes. Use {@link TraversalStrategyProxy#TraversalStrategyProxy(String, Configuration)}
     * instead.
     */
    @Deprecated
    public TraversalStrategyProxy(final Class<T> strategyClass, final Configuration configuration) {
        this(strategyClass.getSimpleName(), configuration);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public String getStrategyName() {
        return this.strategyName;
    }

    /**
     * @deprecated
     * As of 4.0.0, TraversalStrategyProxy is now based around strategy names, instead of strategy classes. For
     * compatibility, this method will attempt to lookup the strategy name in {@link TraversalStrategies.GlobalCache}.
     * Use of {@link TraversalStrategyProxy#getStrategyName()} is preferred. If a class object is needed, users should
     * utilize a mapping of strategy name to strategy class which is appropriate for their environment.
     */
    @Deprecated
    public Class<T> getStrategyClass() {
        return (Class<T>) TraversalStrategies.GlobalCache.getRegisteredStrategyClass(strategyName).get();
    }

    @Override
    public void apply(final Traversal.Admin traversal) {
        throw new UnsupportedOperationException("TraversalStrategyProxy is not meant to be used directly as a TraversalStrategy and is for serialization purposes only");
    }

    @Override
    public int compareTo(final Object o) {
        throw new UnsupportedOperationException("TraversalStrategyProxy is not meant to be used directly as a TraversalStrategy and is for serialization purposes only");
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategyProxyString(this);
    }
}
