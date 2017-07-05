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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalStep<S, E> extends AbstractStep<S, E> implements TraversalParent {

    private Traversal.Admin<S, E> localTraversal;
    private boolean first = true;

    public LocalStep(final Traversal.Admin traversal, final Traversal.Admin<S, E> localTraversal) {
        super(traversal);
        this.localTraversal = this.integrateChild(localTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.singletonList(this.localTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.localTraversal.getTraverserRequirements();
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            this.localTraversal.addStart(this.starts.next());
        }
        while (true) {
            if (this.localTraversal.hasNext())
                return this.localTraversal.nextTraverser();
            else if (this.starts.hasNext()) {
                this.localTraversal.reset();
                this.localTraversal.addStart(this.starts.next());
            } else {
                throw FastNoSuchElementException.instance();
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.localTraversal.reset();
    }

    @Override
    public LocalStep<S, E> clone() {
        final LocalStep<S, E> clone = (LocalStep<S, E>) super.clone();
        clone.localTraversal = this.localTraversal.clone();
        clone.first = true;
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.localTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.localTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.localTraversal.hashCode();
    }
}
