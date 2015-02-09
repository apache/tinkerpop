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
package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectOneStep<S, E> extends SelectStep {

    private final Function<Traverser<S>, Map<String, E>> tempFunction;
    private final String selectLabel;

    public SelectOneStep(final Traversal.Admin traversal, final String selectLabel) {
        super(traversal, selectLabel);
        this.selectLabel = selectLabel;
        this.tempFunction = this.selectFunction;
        this.setFunction(traverser -> this.tempFunction.apply(((Traverser<S>) traverser)).get(this.selectLabel));
    }

    @Override
    public SelectOneStep<S, E> clone() throws CloneNotSupportedException {
        final SelectOneStep<S, E> clone = (SelectOneStep<S, E>) super.clone();
        clone.setFunction(traverser -> this.tempFunction.apply(((Traverser<S>) traverser)).get(this.selectLabel));
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabel, this.traversalRing);
    }
}


