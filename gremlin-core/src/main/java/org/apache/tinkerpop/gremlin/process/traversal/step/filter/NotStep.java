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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class NotStep<S> extends FilterStep<S> implements TraversalParent{

    private Traversal.Admin<S, ?> notTraversal;

    public NotStep(final Traversal.Admin traversal, final Traversal<S, ?> notTraversal) {
        super(traversal);
        this.notTraversal = this.integrateChild(notTraversal.asAdmin());
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return !TraversalUtil.test(traverser, this.notTraversal);
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.notTraversal);
    }

    @Override
    public NotStep<S> clone() {
        final NotStep<S> clone = (NotStep<S>) super.clone();
        clone.notTraversal = this.notTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.notTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.notTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.notTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }
}
