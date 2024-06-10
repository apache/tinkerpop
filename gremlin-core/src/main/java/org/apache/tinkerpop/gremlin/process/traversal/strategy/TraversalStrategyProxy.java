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
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

/**
 * This class is for use with {@link GremlinLang} and for serialization purposes. It is not meant for direct use with
 * {@link TraversalSource#withStrategies(TraversalStrategy[])}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalStrategyProxy<T extends TraversalStrategy> implements Serializable, TraversalStrategy {

    private final Configuration configuration;
    private final Class<T> strategyClass;

    public TraversalStrategyProxy(final T traversalStrategy) {
        this((Class<T>) traversalStrategy.getClass(), traversalStrategy.getConfiguration());
    }

    public TraversalStrategyProxy(final Class<T> strategyClass, final Configuration configuration) {
        this.configuration = configuration;
        this.strategyClass = strategyClass;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public Class<T> getStrategyClass() {
        return this.strategyClass;
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
