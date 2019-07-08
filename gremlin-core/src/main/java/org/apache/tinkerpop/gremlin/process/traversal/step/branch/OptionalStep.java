/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OptionalStep<S> extends AbstractStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, S> optionalTraversal;

    public OptionalStep(final Traversal.Admin traversal, final Traversal.Admin<S, S> optionalTraversal) {
        super(traversal);
        this.optionalTraversal = optionalTraversal;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.optionalTraversal.hasNext())
            return this.optionalTraversal.nextTraverser();
        else {
            final Traverser.Admin<S> traverser = this.starts.next();
            this.optionalTraversal.reset();
            this.optionalTraversal.addStart(traverser.split());
            if (this.optionalTraversal.hasNext())
                return this.optionalTraversal.nextTraverser();
            else
                return traverser;
        }
    }

    @Override
    public List<Traversal.Admin<S, S>> getLocalChildren() {
        return Collections.singletonList(this.optionalTraversal);
    }

    @Override
    public OptionalStep<S> clone() {
        final OptionalStep<S> clone = (OptionalStep<S>) super.clone();
        clone.optionalTraversal = this.optionalTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.optionalTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.optionalTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.optionalTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.optionalTraversal.getTraverserRequirements();
    }

    @Override
    public void reset() {
        super.reset();
        this.optionalTraversal.reset();
    }
}
