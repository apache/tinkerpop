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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinTypeErrorException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIndexHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 */
public final class TinkerGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder, AutoCloseable {

    private final List<HasContainer> hasContainers = new ArrayList<>();
    /**
     * List of iterators opened by this step.
     */
    private final List<Iterator> iterators = new ArrayList<>();

    public TinkerGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);

        // we used to only setIteratorSupplier() if there were no ids OR the first id was instanceof Element,
        // but that allowed the filter in g.V(v).has('k','v') to be ignored.  this created problems for
        // PartitionStrategy which wants to prevent someone from passing "v" from one TraversalSource to
        // another TraversalSource using a different partition
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        Iterator<Edge> iterator;
        final Object[] resolvedIds = this.getIdsAsValues();
        // ids are present, filter on them first
        if (null == resolvedIds)
            iterator = Collections.emptyIterator();
        else if (resolvedIds.length > 0)
            iterator = this.iteratorList(graph.edges(resolvedIds));
        else
            iterator = null == indexedContainer ?
                    this.iteratorList(graph.edges()) :
                    TinkerIndexHelper.queryEdgeIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()).stream()
                                .filter(edge -> HasContainer.testAll(edge, this.hasContainers))
                                .collect(Collectors.<Edge>toList()).iterator();


        iterators.add(iterator);

        return iterator;
    }

    private Iterator<? extends Vertex> vertices() {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        Iterator<? extends Vertex> iterator;
        final Object[] resolvedIds = this.getIdsAsValues();
        // ids are present, filter on them first
        if (null == resolvedIds)
            iterator = Collections.emptyIterator();
        else if (resolvedIds.length > 0)
            iterator = this.iteratorList(graph.vertices(resolvedIds));
        else
            iterator = (null == indexedContainer ?
                    this.iteratorList(graph.vertices()) :
                    IteratorUtils.filter(TinkerIndexHelper.queryVertexIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()).iterator(),
                                         vertex -> HasContainer.testAll(vertex, this.hasContainers)));

        iterators.add(iterator);

        return iterator;
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = ((AbstractTinkerGraph) this.getTraversal().getGraph().get()).getIndexedKeys(indexedClass);

        final Iterator<HasContainer> itty = IteratorUtils.filter(hasContainers.iterator(),
                c -> c.getPredicate().getBiPredicate() == Compare.eq && indexedKeys.contains(c.getKey()));
        return itty.hasNext() ? itty.next() : null;

    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return (null == this.ids || 0 == this.ids.length) ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<>();

        try {
            while (iterator.hasNext()) {
                final E e = iterator.next();
                try {
                    if (HasContainer.testAll(e, this.hasContainers))
                        list.add(e);
                } catch (GremlinTypeErrorException ex) {
                    if (getTraversal().isRoot() || !(getTraversal().getParent() instanceof FilterStep)) {
                        /*
                         * Either we are at the top level of the query, or our parent query is not a FilterStep and thus
                         * cannot handle a GremlinTypeErrorException. In any of these cases we do a binary reduction
                         * from ERROR -> FALSE and filter the solution quietly.
                         */
                    } else {
                        // not a ternary -> binary reducer, pass the ERROR on
                        throw ex;
                    }
                }
            }
        } finally {
            // close the old iterator to release resources since we are returning a new iterator (over list)
            // out of this function.
            CloseableIterator.closeIterator(iterator);
        }

        return new TinkerGraphIterator<>(list.iterator());
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

    @Override
    public void close() {
        iterators.forEach(CloseableIterator::closeIterator);
    }
}
