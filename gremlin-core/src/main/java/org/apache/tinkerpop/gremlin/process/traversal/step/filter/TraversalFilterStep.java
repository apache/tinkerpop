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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalFilterStep<S> extends FilterStep<S> implements TraversalParent, Configuring {
    private final Parameters parameters = new Parameters();

    private Traversal.Admin<S, ?> filterTraversal;

    public TraversalFilterStep(final Traversal.Admin traversal, final Traversal<S, ?> filterTraversal) {
        super(traversal);
        this.filterTraversal = this.integrateChild(filterTraversal.asAdmin());
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return TraversalUtil.test(traverser, this.filterTraversal);
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.filterTraversal);
    }

    @Override
    public TraversalFilterStep<S> clone() {
        final TraversalFilterStep<S> clone = (TraversalFilterStep<S>) super.clone();
        clone.filterTraversal = this.filterTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.filterTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.filterTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.filterTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }
}
