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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;             
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 */
public class GraphStep<S, E extends Element> extends AbstractStep<S, E> implements GraphComputing, AutoCloseable, Configuring {

    protected Parameters parameters = new Parameters();
    protected final Class<E> returnClass;
    protected GValue<?>[] ids;
    protected boolean legacyLogicForPassingNoIds = false;
    protected transient Supplier<Iterator<E>> iteratorSupplier;
    protected boolean isStart;
    protected boolean done = false;
    private Traverser.Admin<S> head = null;
    private Iterator<E> iterator = EmptyIterator.instance();


    public GraphStep(final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal);
        this.returnClass = returnClass;

        // if ids is a single collection like g.V(['a','b','c']), then unroll it into an array of ids
        this.ids = GValue.ensureGValues(tryUnrollSingleCollectionArgument(ids));

        this.isStart = isStart;

        Object[] idValues = GValue.resolveToValues(this.ids);
        this.iteratorSupplier = () -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ?
                this.getTraversal().getGraph().get().vertices(idValues) :
                this.getTraversal().getGraph().get().edges(idValues));
    }

    /**
     * Unrolls a single collection argument into an array of ids. This is useful for steps like
     * {@code g.V(['a','b','c'])}.
     */
    protected static Object[] tryUnrollSingleCollectionArgument(final Object[] ids) {
        final Object[] tempIds;
        if (ids != null && ids.length == 1) {
            final Optional<Collection> opt;
            if (ids[0] instanceof GValue && ((GValue<?>) ids[0]).getType().isCollection())
                opt = Optional.of((Collection) ((GValue) ids[0]).get());
            else if (ids[0] instanceof Collection)
                opt = Optional.of((Collection) ids[0]);
            else
                opt = Optional.empty();

            if (opt.isPresent()) {
                tempIds = opt.get().toArray(new Object[opt.get().size()]);
            } else
                tempIds = ids;
        } else {
            tempIds = ids;
        }
        return tempIds;
    }

    public String toString() {
        return StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids));
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    public boolean isStartStep() {
        return this.isStart;
    }

    public static boolean isStartStep(final Step<?, ?> step) {
        return step instanceof GraphStep && ((GraphStep) step).isStart;
    }

    public boolean returnsVertex() {
        return this.returnClass.equals(Vertex.class);
    }

    public boolean returnsEdge() {
        return this.returnClass.equals(Edge.class);
    }

    public void setIteratorSupplier(final Supplier<Iterator<E>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    /**
     * Get the ids associated with this step. If there are {@link GValue} objects present they will be returned
     * alongside literal ids. Prefer {@link #getIdsAsValues()} if you prefer to work with literal ids only.
     */
    public GValue[] getIds() {
        return this.ids;
    }

    /**
     * Gets the ids associated with this step as literal values rather than {@link GValue} objects.
     */
    public Object[] getIdsAsValues() {
        if (legacyLogicForPassingNoIds) return null;
        return GValue.resolveToValues(this.ids);
    }

    public void addIds(final Object... newIds) {
        // there is some logic that has been around for a long time that used to set ids to null. it only happened here
        // in this method and only occurred when the ids were already null or empty and the newIds were length 1 and
        // an instance of List and that list was empty. so basically it would trigger for something like g.V().hasId([])
        // which in turn would trigger an empty iterator in TinkerGraphStep and zero results. trying to maintain that
        // logic now with GValue in the mix is tough because the context of what the meaning is gets lost by the time
        // you get to calling getResolvedIds(). using a flag to try to maintain that legacy logic, but ultimately, all
        // this needs to be rethought.
        this.legacyLogicForPassingNoIds = newIds.length == 1 && ((newIds[0] instanceof List && ((List) newIds[0]).isEmpty()) ||
                (newIds[0] instanceof GValue && ((GValue) newIds[0]).getType().isCollection() && ((List) ((GValue) newIds[0]).get()).isEmpty()));

        final GValue[] gvalues = GValue.ensureGValues(tryUnrollSingleCollectionArgument(newIds));
        this.ids = ArrayUtils.addAll(this.ids, gvalues);
    }

    public void clearIds() {
        this.ids = new GValue[0];
    }

    @Override
    public void onGraphComputer() {
        this.iteratorSupplier = Collections::emptyIterator;
        convertElementsToIds();
    }

    public void convertElementsToIds() {
        if (null != this.ids) {
            // if this is going to OLAP, convert to ids so you don't serialize elements
            for (int i = 0; i < this.ids.length; i++) {
                final GValue<?> current = this.ids[i];
                if (current.get() instanceof Element) {
                    this.ids[i] = GValue.of(current.getName(), ((Element) current.get()).id());
                }
            }
        }
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                return this.isStart ? this.getTraversal().getTraverserGenerator().generate(this.iterator.next(), (Step) this, 1l) : this.head.split(this.iterator.next(), this);
            } else {
                if (this.isStart) {
                    if (this.done)
                        throw FastNoSuchElementException.instance();
                    else {
                        this.done = true;
                        this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
                    }
                } else {
                    this.head = this.starts.next();
                    this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
                }
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.head = null;
        this.done = false;
        this.iterator = EmptyIterator.instance();
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), returnClass);
        if (ids != null) {
            for (Object id : ids) {
                result = 31 * result + Objects.hashCode(id);
            }
        }
        return result;
    }

    /**
     * Attempts to close an underlying iterator if it is of type {@link CloseableIterator}. Graph providers may choose
     * to return this interface containing their vertices and edges if there are expensive resources that might need to
     * be released at some point.
     */
    @Override
    public void close() {
        CloseableIterator.closeIterator(iterator);
    }

    /**
     * Helper method for providers that want to "fold in" {@link HasContainer}'s based on id checking into the ids of the {@link GraphStep}.
     *
     * @param graphStep    the GraphStep to potentially {@link GraphStep#addIds(Object...)}.
     * @param hasContainer The {@link HasContainer} to check for id validation.
     * @return true if the {@link HasContainer} updated ids and thus, was processed.
     */
    public static boolean processHasContainerIds(final GraphStep<?, ?> graphStep, final HasContainer hasContainer) {
        final String key = hasContainer.getKey();
        if (key != null && key.equals(T.id.getAccessor()) && graphStep.ids.length == 0 &&
                (hasContainer.getBiPredicate() == Compare.eq || hasContainer.getBiPredicate() == Contains.within)) {
            graphStep.addIds(hasContainer.getValue());
            return true;
        }
        return false;
    }
}
