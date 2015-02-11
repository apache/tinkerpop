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
package com.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.TraversalEngine;
import com.apache.tinkerpop.gremlin.process.TraversalStrategy;
import com.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.SideEffectCapable;
import com.apache.tinkerpop.gremlin.process.traversal.step.EmptyStep;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

public final class SideEffectCapStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final SideEffectCapStrategy INSTANCE = new SideEffectCapStrategy();
    private static final Set<Class<? extends TraversalStrategy>> POSTS = Collections.singleton(LabeledEndStepStrategy.class);

    private SideEffectCapStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (traversal.getEndStep() instanceof SideEffectCapable && traversal.getParent() instanceof EmptyStep) {
            ((GraphTraversal) traversal).cap();
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

    public static SideEffectCapStrategy instance() {
        return INSTANCE;
    }
}