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

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.path.MutablePath;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathStep<S> extends MapStep<S, Path> implements TraversalParent {

    private TraversalRing<Object, Object> traversalRing;

    public PathStep(final Traversal.Admin traversal) {
        super(traversal);
        this.traversalRing = new TraversalRing<>();
        PathStep.generateFunction(this);
    }

    @Override
    public PathStep<S> clone() throws CloneNotSupportedException {
        final PathStep<S> clone = (PathStep<S>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            clone.traversalRing.addTraversal(clone.integrateChild(traversal.clone(), TYPICAL_LOCAL_OPERATIONS));
        }
        PathStep.generateFunction(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> pathTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(pathTraversal, TYPICAL_LOCAL_OPERATIONS));
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalRing);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    /////////////////////////

    private static final <S> void generateFunction(final PathStep<S> pathStep) {
        pathStep.setFunction(traverser -> {
            final Path path;
            if (pathStep.traversalRing.isEmpty())
                path = traverser.path();
            else {
                path = MutablePath.make();
                traverser.path().forEach((object, labels) -> path.extend(TraversalUtil.apply(object, pathStep.traversalRing.next()), labels.toArray(new String[labels.size()])));
            }
            pathStep.traversalRing.reset();
            return path;
        });
    }
}
