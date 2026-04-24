/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 */
public class GraphStep<S, E extends Element> extends AbstractStep<S, E> implements GraphStepContract<S, E>, TraversalParent {

    protected Parameters parameters = new Parameters();
    protected final Class<E> returnClass;
    protected Object[] ids;
    protected transient Supplier<Iterator<E>> iteratorSupplier;
    protected boolean isStart;
    protected boolean done = false;
    private Traverser.Admin<S> head = null;
    private Iterator<E> iterator = EmptyIterator.instance();
    private Traversal.Admin<?, ?> idTraversal;


    public GraphStep(final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal);
        this.returnClass = returnClass;
        this.ids = (ids != null && ids.length == 1 && ids[0] instanceof Collection) ? ((Collection) ids[0]).toArray(new Object[((Collection) ids[0]).size()]) : ids;

        for (Object id : this.ids) {
            if (id instanceof GValue) {
                throw new IllegalArgumentException("GValue Not Allowed as id in GraphStep");
            }
        }
        this.isStart = isStart;
        this.iteratorSupplier = () -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ?
                this.getTraversal().getGraph().get().vertices(this.ids) :
                this.getTraversal().getGraph().get().edges(this.ids));
    }

    public GraphStep(final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Traversal.Admin<?, ?> idTraversal) {
        this(traversal, returnClass, isStart);
        this.idTraversal = idTraversal;
        if (this.idTraversal != null) {
            this.integrateChild(this.idTraversal);
        }
    }

    /**
     * Returns the child traversal used to resolve element IDs, or {@code null} if literal IDs are used.
     */
    public Traversal.Admin<?, ?> getIdTraversal() {
        return this.idTraversal;
    }

    /**
     * Sets the child traversal used to resolve element IDs. Calls {@link #integrateChild(Traversal.Admin)}
     * on the provided traversal.
     */
    public void setIdTraversal(final Traversal.Admin<?, ?> idTraversal) {
        this.idTraversal = idTraversal;
        if (this.idTraversal != null) {
            this.integrateChild(this.idTraversal);
        }
    }

    public String toString() {
        if (this.idTraversal != null) {
            return StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.idTraversal);
        }
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

    @Override
    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    @Override
    public boolean isStartStep() {
        return this.isStart;
    }

    public static boolean isStartStep(final Step<?, ?> step) {
        return step instanceof GraphStep && ((GraphStep) step).isStart;
    }

    @Override
    public boolean returnsVertex() {
        return this.returnClass.equals(Vertex.class);
    }

    @Override
    public boolean returnsEdge() {
        return this.returnClass.equals(Edge.class);
    }

    public void setIteratorSupplier(final Supplier<Iterator<E>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    @Override
    public Object[] getIds() {
        return this.ids;
    }

    public void addIds(final Object... newIds) {
        if ((this.ids == null || this.ids.length == 0) &&
                newIds.length == 1 &&
                newIds[0] instanceof Collection && ((Collection) newIds[0]).isEmpty())
            this.ids = null;
        else
            this.ids = ArrayUtils.addAll(this.ids,
                    (newIds.length == 1 && newIds[0] instanceof Collection) ?
                            ((Collection) newIds[0]).toArray(new Object[((Collection) newIds[0]).size()]) :
                            newIds);
    }

    @Override
    public void clearIds() {
        this.ids = new Object[0];
    }

    @Override
    public void onGraphComputer() {
        this.iteratorSupplier = Collections::emptyIterator;
        convertElementsToIds();
    }

    public void convertElementsToIds() {
        if (null != this.ids) {
            for (int i = 0; i < this.ids.length; i++) {    // if this is going to OLAP, convert to ids so you don't serialize elements
                if (this.ids[i] instanceof Element)
                    this.ids[i] = ((Element) this.ids[i]).id();
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
                    if (this.idTraversal != null) {
                        throw new IllegalStateException(
                                "V(traversal)/E(traversal) cannot be used as a start step: " +
                                "no Traverser context available to evaluate the child traversal");
                    }
                    this.done = true;
                    this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
                } else {
                    this.head = this.starts.next();
                    if (this.idTraversal != null) {
                        final Object[] resolvedIds = resolveTraversalIds(this.head);
                        this.iterator = lookupElements(resolvedIds);
                    } else {
                        this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
                    }
                }
            }
        }
    }

    /**
     * Resolves element IDs by evaluating the child traversal against the current traverser.
     * Handles Element (extract ID), Collection (unpack), and raw values.
     */
    @SuppressWarnings("unchecked")
    private Object[] resolveTraversalIds(final Traverser.Admin<S> traverser) {
        final List<Object> ids = new ArrayList<>();
        TraversalUtil.prepare(traverser, (Traversal.Admin) this.idTraversal);
        while (this.idTraversal.hasNext()) {
            final Object result = this.idTraversal.next();
            if (result instanceof Element) {
                ids.add(((Element) result).id());
            } else if (result instanceof Collection) {
                for (final Object item : (Collection<?>) result) {
                    ids.add(item instanceof Element ? ((Element) item).id() : item);
                }
            } else {
                ids.add(result);
            }
        }
        return ids.toArray();
    }

    /**
     * Looks up elements (vertices or edges) by the given IDs using the graph.
     */
    @SuppressWarnings("unchecked")
    private Iterator<E> lookupElements(final Object[] elementIds) {
        if (elementIds.length == 0) {
            return EmptyIterator.instance();
        }
        return (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ?
                this.getTraversal().getGraph().get().vertices(elementIds) :
                this.getTraversal().getGraph().get().edges(elementIds));
    }

    @Override
    public void reset() {
        super.reset();
        this.head = null;
        this.done = false;
        this.iterator = EmptyIterator.instance();
        if (this.idTraversal != null) {
            this.idTraversal.reset();
        }
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

    @SuppressWarnings("unchecked")
    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.idTraversal != null
                ? Collections.singletonList((Traversal.Admin<S, E>) this.idTraversal)
                : Collections.emptyList();
    }

    @Override
    public GraphStep<S, E> clone() {
        final GraphStep<S, E> clone = (GraphStep<S, E>) super.clone();
        if (this.idTraversal != null) {
            clone.idTraversal = this.idTraversal.clone();
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (this.idTraversal != null) {
            this.integrateChild(this.idTraversal);
        }
    }

    /**
     * Attempts to close an underlying iterator if it is of type {@link CloseableIterator}. Graph providers may choose
     * to return this interface containing their vertices and edges if there are expensive resources that might need to
     * be released at some point.
     */
    @Override
    public void close() {
        CloseableIterator.closeIterator(iterator);
        try {
            TraversalParent.super.close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper method for providers that want to "fold in" {@link HasContainer}'s based on id checking into the ids of the {@link GraphStep}.
     *
     * @param graphStep    the GraphStep to potentially {@link GraphStep#addIds(Object...)}.
     * @param hasContainer The {@link HasContainer} to check for id validation.
     * @return true if the {@link HasContainer} updated ids and thus, was processed.
     */
    public static boolean processHasContainerIds(final GraphStep<?, ?> graphStep, final HasContainer hasContainer) {
        if (hasContainer.hasTraversal()) return false;
        final String key = hasContainer.getKey();
        if (key != null && key.equals(T.id.getAccessor()) && graphStep.ids.length == 0 &&
                (hasContainer.getBiPredicate() == Compare.eq || hasContainer.getBiPredicate() == Contains.within)) {
            graphStep.addIds(hasContainer.getValue());
            return true;
        }
        return false;
    }
}
