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
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BackStep<S, E> extends MapStep<S, E> implements EngineDependent {

    private final String stepLabel;
    private boolean requiresPaths = false;

    public BackStep(final Traversal.Admin traversal, final String stepLabel) {
        super(traversal);
        this.stepLabel = stepLabel;
        this.setFunction(traverser -> traverser.path(this.stepLabel));
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.requiresPaths = traversalEngine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        //return this.requiresPaths ? Collections.singleton(TraverserRequirement.PATH) : Collections.singleton(TraverserRequirement.PATH_ACCESS);
        return Collections.singleton(TraverserRequirement.PATH); // TODO: if the traversal isn't nested, path access works
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.stepLabel);
    }
}
