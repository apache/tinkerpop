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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Host;

import java.util.Collection;
import java.util.function.Function;

/**
 * A {@link TraverserSet} that has an index back to the object in the {@link Traverser}. Using this extension of
 * {@link TraverserSet} can make it easier to find traversers within the set if the internal value is known. Without
 * the index the entire {@link TraverserSet} needs to be iterated to find a particular value.
 */
public class IndexedTraverserSet<S,I> extends TraverserSet<S> {

    private final MultiValueMap index = new MultiValueMap();
    private final Function<S,I> indexingFunction;

    public IndexedTraverserSet(final Function<S, I> indexingFunction) {
        this(indexingFunction, null);
    }

    public IndexedTraverserSet(final Function<S, I> indexingFunction, final Traverser.Admin<S> traverser) {
        super(traverser);
        this.indexingFunction = indexingFunction;
    }

    @Override
    public void clear() {
        index.clear();
        super.clear();
    }

    @Override
    public boolean add(final Traverser.Admin<S> traverser) {
        final boolean newOne = super.add(traverser);

        // if newly added then the traverser will be the same as the one passed in here to add().
        // if it is not, then it was merged to an existing traverser and the bulk would have
        // updated on that reference, thus only new stuff really needs to be added to the index
        if (newOne) index.put(indexingFunction.apply(traverser.get()), traverser);

        return newOne;
    }

    /**
     * Gets a collection of {@link Traverser} objects that contain the specified value.
     *
     * @param k the key produced by the indexing function
     * @return
     */
    public Collection<Traverser.Admin<S>> get(final I k) {
        final Collection<Traverser.Admin<S>> c = index.getCollection(k);

        // if remove() is called on this class, then the MultiValueMap *may* (javadoc wasn't clear
        // what the expectation was - used the word "typically") return an empty list if the last
        // item removed leaves the list empty. i think we want to enforce null for TraverserSet
        // semantics
        return c != null && c.isEmpty() ? null : c;
    }

    @Override
    public boolean offer(final Traverser.Admin<S> traverser) {
        return this.add(traverser);
    }

    @Override
    public Traverser.Admin<S> remove() {
        final Traverser.Admin<S> removed = super.remove();
        index.remove(indexingFunction.apply(removed.get()), removed);
        return removed;
    }

    @Override
    public boolean remove(final Object traverser) {
        if (!(traverser instanceof Traverser.Admin))
            throw new IllegalArgumentException("The object to remove must be traverser");

        final boolean removed = super.remove(traverser);
        if (removed) index.remove(indexingFunction.apply(((Traverser.Admin<S>) traverser).get()), traverser);
        return removed;
    }

    /**
     * An {@link IndexedTraverserSet} that indexes based on a {@link Vertex} traverser.
     */
    public static class VertexIndexedTraverserSet extends IndexedTraverserSet<Object, Vertex> {
        public VertexIndexedTraverserSet() {
            super(Host::getHostingVertex);
        }
    }
}
