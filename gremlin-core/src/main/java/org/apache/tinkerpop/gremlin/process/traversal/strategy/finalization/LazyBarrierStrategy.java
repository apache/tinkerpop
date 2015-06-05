/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LazyBarrierStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final LazyBarrierStrategy INSTANCE = new LazyBarrierStrategy();
    private static final Set<Class<? extends FinalizationStrategy>> PRIORS = new HashSet<>();
    private static final Set<Class<? extends FinalizationStrategy>> POSTS = new HashSet<>();

    static {
        PRIORS.add(ScopingStrategy.class);
        //
        POSTS.add(ProfileStrategy.class);
    }


    private LazyBarrierStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer())
            return;
        // TODO:
    }


    @Override
    public Set<Class<? extends FinalizationStrategy>> applyPrior() {
        return PRIORS;
    }

    @Override
    public Set<Class<? extends FinalizationStrategy>> applyPost() {
        return POSTS;
    }

    public static LazyBarrierStrategy instance() {
        return INSTANCE;
    }
}
