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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MapStep<S, E> extends AbstractStep<S, E> {

    public MapStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        final Traverser.Admin<S> traverser = this.starts.next();
        final E obj = this.map(traverser);

        // maybe looks tricky, but it's just a play on Java generics without having to tear apart every MapStep
        // instance. basically just want to respect the fact that a subclass can return an EmptyTraverser from
        // map() and if so, just return that empty.
        return isEmptyTraverser(obj) ? EmptyTraverser.instance() : traverser.split(obj, this);
    }

    protected abstract E map(final Traverser.Admin<S> traverser);

    /**
     * Determines if the value returned from {@link #map(Traverser.Admin)} should be representative of an
     * {@link EmptyTraverser}. Such traversers will effectively be filtered out by the traversal.
     */
    protected boolean isEmptyTraverser(E obj) {
        return false;
    }

}

