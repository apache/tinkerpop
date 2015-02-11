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
package com.apache.tinkerpop.gremlin.process.traversal.util;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.TraversalEngine;
import com.apache.tinkerpop.gremlin.process.TraversalStrategies;
import com.apache.tinkerpop.gremlin.process.TraversalStrategy;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.apache.tinkerpop.gremlin.process.traverser.util.DefaultTraverserGeneratorFactory;

import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraversalStrategies implements TraversalStrategies {

    private static final EmptyTraversalStrategies INSTANCE = new EmptyTraversalStrategies();

    private EmptyTraversalStrategies() {
    }

    @Override
    public List<TraversalStrategy> toList() {
        return Collections.emptyList();
    }

    @Override
    public void applyStrategies(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {

    }

    @Override
    public TraversalStrategies addStrategies(final TraversalStrategy... strategies) {
        return this;
    }

    @Override
    public TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses) {
        return this;
    }

    @Override
    public TraversalStrategies clone() throws CloneNotSupportedException {
        return this;
    }

    @Override
    public TraverserGeneratorFactory getTraverserGeneratorFactory() {
        return DefaultTraverserGeneratorFactory.instance();
    }

    @Override
    public void setTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory) {

    }

    public static EmptyTraversalStrategies instance() {
        return INSTANCE;
    }
}
