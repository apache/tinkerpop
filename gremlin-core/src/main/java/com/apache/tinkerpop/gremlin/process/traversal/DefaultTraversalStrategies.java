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
package com.apache.tinkerpop.gremlin.process.traversal;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.TraversalEngine;
import com.apache.tinkerpop.gremlin.process.TraversalStrategies;
import com.apache.tinkerpop.gremlin.process.TraversalStrategy;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.apache.tinkerpop.gremlin.process.traverser.util.DefaultTraverserGeneratorFactory;
import com.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalStrategies implements TraversalStrategies {

    protected List<TraversalStrategy> traversalStrategies = new ArrayList<>();
    protected TraverserGeneratorFactory traverserGeneratorFactory = DefaultTraverserGeneratorFactory.instance();

    @Override
    public TraversalStrategies addStrategies(final TraversalStrategy... strategies) {
        boolean added = false;
        for (final TraversalStrategy strategy : strategies) {
            if (!this.traversalStrategies.contains(strategy)) {
                this.traversalStrategies.add(strategy);
                added = true;
            }
        }
        if (added) TraversalStrategies.sortStrategies(this.traversalStrategies);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses) {
        boolean removed = false;
        for (final Class<? extends TraversalStrategy> strategyClass : strategyClasses) {
            final Optional<TraversalStrategy> strategy = this.traversalStrategies.stream().filter(s -> s.getClass().equals(strategyClass)).findAny();
            if (strategy.isPresent()) {
                this.traversalStrategies.remove(strategy.get());
                removed = true;
            }
        }
        if (removed) TraversalStrategies.sortStrategies(this.traversalStrategies);
        return this;
    }

    @Override
    public List<TraversalStrategy> toList() {
        return Collections.unmodifiableList(this.traversalStrategies);
    }

    @Override
    public void applyStrategies(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        this.traversalStrategies.forEach(ts -> ts.apply(traversal, engine));
    }

    @Override
    public TraverserGeneratorFactory getTraverserGeneratorFactory() {
        return this.traverserGeneratorFactory;
    }

    @Override
    public void setTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory) {
        this.traverserGeneratorFactory = traverserGeneratorFactory;
    }

    @Override
    public DefaultTraversalStrategies clone() throws CloneNotSupportedException {
        final DefaultTraversalStrategies clone = (DefaultTraversalStrategies) super.clone();
        clone.traversalStrategies = new ArrayList<>();
        clone.traversalStrategies.addAll(this.traversalStrategies);
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategiesString(this);
    }
}
