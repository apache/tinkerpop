/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public final class TailLocalStep<S> extends MapStep<S, S> {

    private final long limit;

    public TailLocalStep(final Traversal.Admin traversal, final long limit) {
        super(traversal);
        this.limit = limit;
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        // We may consider optimizing the iteration of these containers using subtype-specific interfaces.  For
        // example, we could use descendingIterator if we have a Deque.  But in general, we cannot reliably iterate a
        // collection in reverse, so we use the range algorithm with dynamically computed boundaries.
        final S start = traverser.get();
        final long high =
            start instanceof Map ? ((Map)start).size() :
            start instanceof Collection ? ((Collection)start).size() :
            this.limit;
        final long low = high - this.limit;
        final S result = RangeLocalStep.applyRange(start, low, high);

        // If we are limiting a collection to a single item, then emit only that item.
        if (1 == this.limit && result instanceof Collection) {
            final Collection c = (Collection) result;
            if (c.isEmpty()) {
                // We have nothing to emit, so stop traversal.
                throw FastNoSuchElementException.instance();
            } else {
                return (S) c.iterator().next();
            }
        } else {
            return result;
        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.limit);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
