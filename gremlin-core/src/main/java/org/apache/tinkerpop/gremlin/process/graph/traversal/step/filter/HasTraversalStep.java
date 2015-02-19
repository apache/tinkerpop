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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class HasTraversalStep<S> extends AbstractStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, ?> hasTraversal;
    private final boolean negate;

    public HasTraversalStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?> hasTraversal, final boolean negate) {
        super(traversal);
        this.negate = negate;
        this.hasTraversal = this.integrateChild(hasTraversal);
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            if (TraversalUtil.test(start, this.hasTraversal) != this.negate)
                return start;
        }
    }

    @Override
    public String toString() {
        final String stepString = TraversalHelper.makeStepString(this, this.hasTraversal);
        return this.negate ? stepString.replaceFirst("\\(", "(!") : stepString;
    }

    @Override
    public List<Traversal<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.hasTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public HasTraversalStep<S> clone() throws CloneNotSupportedException {
        final HasTraversalStep<S> clone = (HasTraversalStep<S>) super.clone();
        clone.hasTraversal = clone.integrateChild(this.hasTraversal.clone());
        return clone;
    }
}
